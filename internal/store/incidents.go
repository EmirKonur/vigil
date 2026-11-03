package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

// Incident is a stored incident with its timeline.
type Incident struct {
	ID         int64
	MonitorID  string
	OpenedAt   time.Time
	ResolvedAt time.Time // zero while open
	Impact     string
	Regions    []string
	Voting     int
	Cause      string
	Flapping   bool
	Updates    []Update // oldest first; filled by Incident and IncidentsBetween
}

// Open reports whether the incident is unresolved.
func (i Incident) Open() bool { return i.ResolvedAt.IsZero() }

// Duration is how long the incident lasted, or has lasted so far.
func (i Incident) Duration(now time.Time) time.Duration {
	if i.Open() {
		return now.Sub(i.OpenedAt)
	}
	return i.ResolvedAt.Sub(i.OpenedAt)
}

// Update is one entry in an incident's timeline.
type Update struct {
	ID         int64
	IncidentID int64
	At         time.Time
	Status     string
	Body       string
	Author     string
}

// Update authors.
const (
	AuthorVigil    = "vigil"
	AuthorOperator = "operator"
)

// OpenIncident inserts a new incident and returns its ID.
func (tx *Tx) OpenIncident(ctx context.Context, inc Incident) (int64, error) {
	regions, _ := json.Marshal(inc.Regions)
	res, err := tx.ExecContext(ctx, `
		INSERT INTO incidents (monitor_id, opened_at, impact, regions, voting, cause, flapping)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		inc.MonitorID, ms(inc.OpenedAt), inc.Impact, string(regions), inc.Voting, inc.Cause, inc.Flapping)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// IncidentID finds an incident by its natural key.
func (tx *Tx) IncidentID(ctx context.Context, monitorID string, openedAt time.Time) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM incidents WHERE monitor_id = ? AND opened_at = ?`, monitorID, ms(openedAt)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return id, err
}

// ResolveIncident marks an incident resolved.
func (tx *Tx) ResolveIncident(ctx context.Context, id int64, at time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE incidents SET resolved_at = ? WHERE id = ? AND resolved_at IS NULL`, ms(at), id)
	return err
}

// WidenImpact records that an incident got worse. The stored row keeps the
// peak: impact only escalates, and regions accumulate, so a major outage is
// still recorded as major after regions start recovering. It reports whether
// anything changed.
func (tx *Tx) WidenImpact(ctx context.Context, id int64, impact string, regions []string, voting int) (bool, error) {
	var curImpact, curRegions string
	var curVoting int
	if err := tx.QueryRowContext(ctx, `SELECT impact, regions, voting FROM incidents WHERE id = ?`, id).Scan(&curImpact, &curRegions, &curVoting); err != nil {
		return false, err
	}
	var merged []string
	_ = json.Unmarshal([]byte(curRegions), &merged)
	before := len(merged)
	for _, r := range regions {
		if !slices.Contains(merged, r) {
			merged = append(merged, r)
		}
	}
	slices.Sort(merged)
	newImpact := curImpact
	if impact == "major" {
		newImpact = "major"
	}
	if newImpact == curImpact && len(merged) == before {
		return false, nil
	}
	raw, _ := json.Marshal(merged)
	_, err := tx.ExecContext(ctx, `UPDATE incidents SET impact = ?, regions = ?, voting = ? WHERE id = ?`, newImpact, string(raw), max(voting, curVoting), id)
	return err == nil, err
}

// SetFlapping marks an incident as flapping.
func (tx *Tx) SetFlapping(ctx context.Context, id int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE incidents SET flapping = 1 WHERE id = ?`, id)
	return err
}

// AddUpdate appends a timeline entry.
func (tx *Tx) AddUpdate(ctx context.Context, u Update) (int64, error) {
	res, err := tx.ExecContext(ctx, `INSERT INTO incident_updates (incident_id, at, status, body, author) VALUES (?, ?, ?, ?, ?)`,
		u.IncidentID, ms(u.At), u.Status, u.Body, u.Author)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

const incidentColumns = `id, monitor_id, opened_at, resolved_at, impact, regions, voting, cause, flapping`

func scanIncident(sc interface{ Scan(...any) error }) (Incident, error) {
	var inc Incident
	var opened int64
	var resolved sql.NullInt64
	var regions string
	if err := sc.Scan(&inc.ID, &inc.MonitorID, &opened, &resolved, &inc.Impact, &regions, &inc.Voting, &inc.Cause, &inc.Flapping); err != nil {
		return inc, err
	}
	inc.OpenedAt, inc.ResolvedAt = fromMS(opened), nullMS(resolved)
	_ = json.Unmarshal([]byte(regions), &inc.Regions)
	return inc, nil
}

// Incident returns one incident with its updates.
func (s *Store) Incident(ctx context.Context, id int64) (Incident, error) {
	inc, err := scanIncident(s.r.QueryRowContext(ctx, `SELECT `+incidentColumns+` FROM incidents WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return inc, ErrNotFound
	}
	if err != nil {
		return inc, err
	}
	withUpdates := []Incident{inc}
	if err := s.attachUpdates(ctx, withUpdates); err != nil {
		return inc, err
	}
	return withUpdates[0], nil
}

// OpenIncidents returns unresolved incidents, newest first.
func (s *Store) OpenIncidents(ctx context.Context) ([]Incident, error) {
	return s.queryIncidents(ctx, `SELECT `+incidentColumns+` FROM incidents WHERE resolved_at IS NULL ORDER BY opened_at DESC`)
}

// IncidentsBetween returns incidents overlapping [from, to) for the given
// monitors (all monitors when empty), newest first, with their updates.
func (s *Store) IncidentsBetween(ctx context.Context, monitorIDs []string, from, to time.Time) ([]Incident, error) {
	query := `SELECT ` + incidentColumns + ` FROM incidents WHERE opened_at < ? AND (resolved_at IS NULL OR resolved_at >= ?)`
	args := []any{ms(to), ms(from)}
	if len(monitorIDs) > 0 {
		query += ` AND monitor_id IN (?` + strings.Repeat(",?", len(monitorIDs)-1) + `)`
		for _, id := range monitorIDs {
			args = append(args, id)
		}
	}
	query += ` ORDER BY opened_at DESC`
	incs, err := s.queryIncidents(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return incs, s.attachUpdates(ctx, incs)
}

func (s *Store) queryIncidents(ctx context.Context, query string, args ...any) ([]Incident, error) {
	rows, err := s.r.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Incident
	for rows.Next() {
		inc, err := scanIncident(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inc)
	}
	return out, rows.Err()
}

func (s *Store) attachUpdates(ctx context.Context, incs []Incident) error {
	if len(incs) == 0 {
		return nil
	}
	index := map[int64]int{}
	args := make([]any, len(incs))
	for i, inc := range incs {
		index[inc.ID] = i
		args[i] = inc.ID
	}
	rows, err := s.r.QueryContext(ctx, `SELECT id, incident_id, at, status, body, author FROM incident_updates
		WHERE incident_id IN (?`+strings.Repeat(",?", len(incs)-1)+`) ORDER BY at, id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var u Update
		var at int64
		if err := rows.Scan(&u.ID, &u.IncidentID, &at, &u.Status, &u.Body, &u.Author); err != nil {
			return err
		}
		u.At = fromMS(at)
		i := index[u.IncidentID]
		incs[i].Updates = append(incs[i].Updates, u)
	}
	return rows.Err()
}

// AddOperatorUpdate posts an update from the dashboard.
func (s *Store) AddOperatorUpdate(ctx context.Context, incidentID int64, status, body string, at time.Time) error {
	return s.Tx(ctx, func(tx *Tx) error {
		var one int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM incidents WHERE id = ?`, incidentID).Scan(&one); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		_, err := tx.AddUpdate(ctx, Update{IncidentID: incidentID, At: at, Status: status, Body: body, Author: AuthorOperator})
		return err
	})
}
