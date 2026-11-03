package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Result is one stored check result.
type Result struct {
	ID         string
	MonitorID  string
	Region     string
	ProbeID    string
	Slot       time.Time
	ReceivedAt time.Time
	OK         bool
	LatencyMS  float64
	StatusCode int
	Error      string
	Warning    string
}

// InsertResult stores r unless a result with the same ID already exists.
// It reports whether the row was new; callers only advance the state
// machine for new rows.
func (tx *Tx) InsertResult(ctx context.Context, r Result) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO results (id, monitor_id, region, probe_id, slot, received_at, ok, latency_ms, status_code, error, warning)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO NOTHING`,
		r.ID, r.MonitorID, r.Region, r.ProbeID, ms(r.Slot), ms(r.ReceivedAt), r.OK, r.LatencyMS, r.StatusCode, r.Error, r.Warning)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// ResultExists reports whether a result with this ID was stored.
func (s *Store) ResultExists(ctx context.Context, id string) (bool, error) {
	var one int
	err := s.r.QueryRowContext(ctx, `SELECT 1 FROM results WHERE id = ?`, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Results returns a monitor's results with slots in [from, to), oldest first.
func (s *Store) Results(ctx context.Context, monitorID string, from, to time.Time) ([]Result, error) {
	return s.queryResults(ctx, `
		SELECT id, monitor_id, region, probe_id, slot, received_at, ok, latency_ms, status_code, error, warning
		FROM results WHERE monitor_id = ? AND slot >= ? AND slot < ? ORDER BY slot, region`,
		monitorID, ms(from), ms(to))
}

// LatestResults returns the newest results for a monitor, newest first.
func (s *Store) LatestResults(ctx context.Context, monitorID string, limit int) ([]Result, error) {
	return s.queryResults(ctx, `
		SELECT id, monitor_id, region, probe_id, slot, received_at, ok, latency_ms, status_code, error, warning
		FROM results WHERE monitor_id = ? ORDER BY slot DESC, region LIMIT ?`,
		monitorID, limit)
}

// LatestPerRegion returns the newest result per monitor and region since a
// cutoff, for the dashboard's region lamps.
func (s *Store) LatestPerRegion(ctx context.Context, since time.Time) (map[string]map[string]Result, error) {
	rows, err := s.queryResults(ctx, `
		SELECT r.id, r.monitor_id, r.region, r.probe_id, r.slot, r.received_at, r.ok, r.latency_ms, r.status_code, r.error, r.warning
		FROM results r
		JOIN (SELECT monitor_id, region, max(slot) AS slot FROM results WHERE slot >= ? GROUP BY monitor_id, region) latest
		  ON latest.monitor_id = r.monitor_id AND latest.region = r.region AND latest.slot = r.slot`,
		ms(since))
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]Result{}
	for _, r := range rows {
		if out[r.MonitorID] == nil {
			out[r.MonitorID] = map[string]Result{}
		}
		out[r.MonitorID][r.Region] = r
	}
	return out, nil
}

func (s *Store) queryResults(ctx context.Context, query string, args ...any) ([]Result, error) {
	rows, err := s.r.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Result
	for rows.Next() {
		var r Result
		var slot, received int64
		if err := rows.Scan(&r.ID, &r.MonitorID, &r.Region, &r.ProbeID, &slot, &received, &r.OK, &r.LatencyMS, &r.StatusCode, &r.Error, &r.Warning); err != nil {
			return nil, err
		}
		r.Slot, r.ReceivedAt = fromMS(slot), fromMS(received)
		out = append(out, r)
	}
	return out, rows.Err()
}
