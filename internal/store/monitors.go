package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"vigil/internal/consensus"
)

// Monitor mirrors a configured monitor plus its runtime flags.
type Monitor struct {
	ID        string
	Name      string
	Type      string
	Target    string
	Interval  time.Duration
	Regions   []string
	Paused    bool
	Archived  bool
	CreatedAt time.Time
}

// SyncMonitors upserts the configured monitors and archives rows that are no
// longer in the config. Paused flags survive, since pausing is an operator
// action rather than configuration.
func (s *Store) SyncMonitors(ctx context.Context, monitors []Monitor, now time.Time) error {
	return s.Tx(ctx, func(tx *Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE monitors SET archived = 1`); err != nil {
			return err
		}
		for _, m := range monitors {
			regions, _ := json.Marshal(m.Regions)
			_, err := tx.ExecContext(ctx, `
				INSERT INTO monitors (id, name, type, target, interval_ms, regions, archived, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
				ON CONFLICT (id) DO UPDATE SET
					name = excluded.name, type = excluded.type, target = excluded.target,
					interval_ms = excluded.interval_ms, regions = excluded.regions,
					archived = 0, updated_at = excluded.updated_at`,
				m.ID, m.Name, m.Type, m.Target, m.Interval.Milliseconds(), string(regions), ms(now), ms(now))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Monitors returns active (non-archived) monitors keyed by ID.
func (s *Store) Monitors(ctx context.Context) (map[string]Monitor, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT id, name, type, target, interval_ms, regions, paused, archived, created_at FROM monitors WHERE archived = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Monitor{}
	for rows.Next() {
		var m Monitor
		var interval, created int64
		var regions string
		if err := rows.Scan(&m.ID, &m.Name, &m.Type, &m.Target, &interval, &regions, &m.Paused, &m.Archived, &created); err != nil {
			return nil, err
		}
		m.Interval = time.Duration(interval) * time.Millisecond
		m.CreatedAt = fromMS(created)
		_ = json.Unmarshal([]byte(regions), &m.Regions)
		out[m.ID] = m
	}
	return out, rows.Err()
}

// SetPaused pauses or resumes a monitor.
func (s *Store) SetPaused(ctx context.Context, id string, paused bool, now time.Time) error {
	res, err := s.w.ExecContext(ctx, `UPDATE monitors SET paused = ?, updated_at = ? WHERE id = ? AND archived = 0`, paused, ms(now), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// SetMonitorCreated backdates when a monitor started being tracked. Uptime
// math excludes time before it; `vigil demo` uses this for seeded history.
func (s *Store) SetMonitorCreated(ctx context.Context, id string, at time.Time) error {
	_, err := s.w.ExecContext(ctx, `UPDATE monitors SET created_at = ? WHERE id = ?`, ms(at), id)
	return err
}

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("not found")

// States loads every persisted consensus state.
func (s *Store) States(ctx context.Context) (map[string]*consensus.State, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT monitor_id, state FROM monitor_state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*consensus.State{}
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		st := consensus.New()
		if err := json.Unmarshal([]byte(raw), st); err != nil {
			return nil, err
		}
		out[id] = st
	}
	return out, rows.Err()
}

// SaveState persists a monitor's consensus state.
func (tx *Tx) SaveState(ctx context.Context, monitorID string, st *consensus.State, now time.Time) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO monitor_state (monitor_id, state, updated_at) VALUES (?, ?, ?)
		ON CONFLICT (monitor_id) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at`,
		monitorID, string(raw), ms(now))
	return err
}
