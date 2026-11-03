package store

import (
	"context"
	"strings"
	"time"

	"vigil/internal/sketch"
)

// Rollup resolutions.
const (
	Hour = "hour"
	Day  = "day"
)

// Rollup aggregates one monitor's results in one region over one bucket.
type Rollup struct {
	Resolution string
	MonitorID  string
	Region     string
	Bucket     time.Time // bucket start, UTC
	Total      int64
	OK         int64
	Latency    *sketch.Sketch // successful checks only
}

// UpsertRollup writes or replaces a rollup row.
func (tx *Tx) UpsertRollup(ctx context.Context, r Rollup) error {
	blob, err := r.Latency.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO rollups (resolution, monitor_id, region, bucket, total, ok, sketch) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (resolution, monitor_id, region, bucket) DO UPDATE SET total = excluded.total, ok = excluded.ok, sketch = excluded.sketch`,
		r.Resolution, r.MonitorID, r.Region, ms(r.Bucket), r.Total, r.OK, blob)
	return err
}

type rollupKey struct {
	monitor, region string
	bucket          int64
}

// RollupHours recomputes hourly rollups for every hour touching [from, to)
// from raw results. Recomputing from scratch keeps the job idempotent: it
// can rerun over the same hours after a crash and converge on the same rows.
func (s *Store) RollupHours(ctx context.Context, from, to time.Time) (int, error) {
	from = from.UTC().Truncate(time.Hour)
	rows, err := s.r.QueryContext(ctx, `SELECT monitor_id, region, slot, ok, latency_ms FROM results WHERE slot >= ? AND slot < ?`, ms(from), ms(to))
	if err != nil {
		return 0, err
	}
	agg := map[rollupKey]*Rollup{}
	for rows.Next() {
		var monitor, region string
		var slot int64
		var ok bool
		var latency float64
		if err := rows.Scan(&monitor, &region, &slot, &ok, &latency); err != nil {
			rows.Close()
			return 0, err
		}
		bucket := fromMS(slot).Truncate(time.Hour)
		k := rollupKey{monitor, region, ms(bucket)}
		r, exists := agg[k]
		if !exists {
			r = &Rollup{Resolution: Hour, MonitorID: monitor, Region: region, Bucket: bucket, Latency: sketch.New()}
			agg[k] = r
		}
		r.Total++
		if ok {
			r.OK++
			r.Latency.Add(latency)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return len(agg), s.writeRollups(ctx, agg)
}

// RollupDays recomputes daily rollups for every UTC day touching [from, to)
// by merging that day's hourly sketches.
func (s *Store) RollupDays(ctx context.Context, from, to time.Time) (int, error) {
	from = startOfDay(from)
	hours, err := s.Rollups(ctx, Hour, nil, from, to)
	if err != nil {
		return 0, err
	}
	agg := map[rollupKey]*Rollup{}
	for _, h := range hours {
		day := startOfDay(h.Bucket)
		k := rollupKey{h.MonitorID, h.Region, ms(day)}
		r, exists := agg[k]
		if !exists {
			r = &Rollup{Resolution: Day, MonitorID: h.MonitorID, Region: h.Region, Bucket: day, Latency: sketch.New()}
			agg[k] = r
		}
		r.Total += h.Total
		r.OK += h.OK
		r.Latency.Merge(h.Latency)
	}
	return len(agg), s.writeRollups(ctx, agg)
}

func (s *Store) writeRollups(ctx context.Context, agg map[rollupKey]*Rollup) error {
	if len(agg) == 0 {
		return nil
	}
	return s.Tx(ctx, func(tx *Tx) error {
		for _, r := range agg {
			if err := tx.UpsertRollup(ctx, *r); err != nil {
				return err
			}
		}
		return nil
	})
}

// Rollups returns rollups of one resolution with buckets in [from, to),
// optionally limited to some monitors, ordered by bucket.
func (s *Store) Rollups(ctx context.Context, resolution string, monitorIDs []string, from, to time.Time) ([]Rollup, error) {
	query := `SELECT monitor_id, region, bucket, total, ok, sketch FROM rollups WHERE resolution = ? AND bucket >= ? AND bucket < ?`
	args := []any{resolution, ms(from), ms(to)}
	if len(monitorIDs) > 0 {
		query += ` AND monitor_id IN (?` + strings.Repeat(",?", len(monitorIDs)-1) + `)`
		for _, id := range monitorIDs {
			args = append(args, id)
		}
	}
	rows, err := s.r.QueryContext(ctx, query+` ORDER BY bucket`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Rollup
	for rows.Next() {
		r := Rollup{Resolution: resolution, Latency: sketch.New()}
		var bucket int64
		var blob []byte
		if err := rows.Scan(&r.MonitorID, &r.Region, &bucket, &r.Total, &r.OK, &blob); err != nil {
			return nil, err
		}
		r.Bucket = fromMS(bucket)
		if err := r.Latency.UnmarshalBinary(blob); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Retention is how long each resolution is kept.
type Retention struct {
	Raw, Hourly, Daily time.Duration
}

// Prune deletes data older than its retention. Incidents are kept: they are
// small and are the history people actually come back for.
func (s *Store) Prune(ctx context.Context, now time.Time, r Retention) (deleted int64, err error) {
	err = s.Tx(ctx, func(tx *Tx) error {
		for _, q := range []struct {
			sql    string
			cutoff time.Time
		}{
			{`DELETE FROM results WHERE slot < ?`, now.Add(-r.Raw)},
			{`DELETE FROM rollups WHERE resolution = 'hour' AND bucket < ?`, now.Add(-r.Hourly)},
			{`DELETE FROM rollups WHERE resolution = 'day' AND bucket < ?`, now.Add(-r.Daily)},
			{`DELETE FROM notifications WHERE status != 'pending' AND created_at < ?`, now.Add(-r.Daily)},
		} {
			res, err := tx.ExecContext(ctx, q.sql, ms(q.cutoff))
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			deleted += n
		}
		return nil
	})
	return deleted, err
}

func startOfDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
