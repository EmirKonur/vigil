package store

import (
	"context"
	"time"
)

// Probe is a registered probe and its health.
type Probe struct {
	ID              string
	Region          string
	Version         string
	Embedded        bool
	RemoteAddr      string
	FirstSeen       time.Time
	LastSeen        time.Time
	ClockSkew       time.Duration
	ChecksCompleted int64
}

// UpsertProbe records a probe heartbeat. checks is added to the running
// total of completed checks.
func (s *Store) UpsertProbe(ctx context.Context, p Probe, checks int64) error {
	_, err := s.w.ExecContext(ctx, `
		INSERT INTO probes (id, region, version, embedded, remote_addr, first_seen_at, last_seen_at, clock_skew_ms, checks_completed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			region = excluded.region, version = excluded.version, embedded = excluded.embedded,
			remote_addr = excluded.remote_addr, last_seen_at = excluded.last_seen_at,
			clock_skew_ms = excluded.clock_skew_ms,
			checks_completed = probes.checks_completed + excluded.checks_completed`,
		p.ID, p.Region, p.Version, p.Embedded, p.RemoteAddr, ms(p.LastSeen), ms(p.LastSeen), p.ClockSkew.Milliseconds(), checks)
	return err
}

// Probes returns every probe seen since a cutoff, newest first.
func (s *Store) Probes(ctx context.Context, since time.Time) ([]Probe, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT id, region, version, embedded, remote_addr, first_seen_at, last_seen_at, clock_skew_ms, checks_completed
		FROM probes WHERE last_seen_at >= ? ORDER BY region, id`, ms(since))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Probe
	for rows.Next() {
		var p Probe
		var first, last, skew int64
		if err := rows.Scan(&p.ID, &p.Region, &p.Version, &p.Embedded, &p.RemoteAddr, &first, &last, &skew, &p.ChecksCompleted); err != nil {
			return nil, err
		}
		p.FirstSeen, p.LastSeen, p.ClockSkew = fromMS(first), fromMS(last), time.Duration(skew)*time.Millisecond
		out = append(out, p)
	}
	return out, rows.Err()
}
