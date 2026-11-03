package store

import (
	"context"
	"database/sql"
	"time"
)

// Notification is an outbox row.
type Notification struct {
	ID            int64
	Notifier      string
	DedupKey      string
	IncidentID    int64
	MonitorID     string
	Kind          string
	Message       []byte
	Status        string
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	SentAt        time.Time
}

// Notification statuses.
const (
	NotificationPending = "pending"
	NotificationSent    = "sent"
	NotificationFailed  = "failed"
)

// EnqueueNotification adds an outbox row unless one with the same notifier
// and dedup key exists. It reports whether a row was added. A zero
// NextAttemptAt means "send now".
func (tx *Tx) EnqueueNotification(ctx context.Context, n Notification) (bool, error) {
	var incident any
	if n.IncidentID != 0 {
		incident = n.IncidentID
	}
	if n.NextAttemptAt.IsZero() {
		n.NextAttemptAt = n.CreatedAt
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO notifications (notifier, dedup_key, incident_id, monitor_id, kind, message, next_attempt_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (notifier, dedup_key) DO NOTHING`,
		n.Notifier, n.DedupKey, incident, n.MonitorID, n.Kind, string(n.Message), ms(n.NextAttemptAt), ms(n.CreatedAt))
	if err != nil {
		return false, err
	}
	added, err := res.RowsAffected()
	return added == 1, err
}

const notificationColumns = `id, notifier, dedup_key, incident_id, monitor_id, kind, message, status, attempts, next_attempt_at, last_error, created_at, sent_at`

func scanNotification(sc interface{ Scan(...any) error }) (Notification, error) {
	var n Notification
	var incident, sent sql.NullInt64
	var next, created int64
	var msg string
	err := sc.Scan(&n.ID, &n.Notifier, &n.DedupKey, &incident, &n.MonitorID, &n.Kind, &msg, &n.Status, &n.Attempts, &next, &n.LastError, &created, &sent)
	n.IncidentID = incident.Int64
	n.Message = []byte(msg)
	n.NextAttemptAt, n.CreatedAt, n.SentAt = fromMS(next), fromMS(created), nullMS(sent)
	return n, err
}

// DueNotifications returns pending rows whose next attempt is due.
func (s *Store) DueNotifications(ctx context.Context, now time.Time, limit int) ([]Notification, error) {
	return s.queryNotifications(ctx, `SELECT `+notificationColumns+` FROM notifications
		WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY next_attempt_at, id LIMIT ?`, ms(now), limit)
}

// NextNotificationDue returns when the earliest pending row is due, or the
// zero time when nothing is pending.
func (s *Store) NextNotificationDue(ctx context.Context) (time.Time, error) {
	var next sql.NullInt64
	err := s.r.QueryRowContext(ctx, `SELECT min(next_attempt_at) FROM notifications WHERE status = 'pending'`).Scan(&next)
	return nullMS(next), err
}

// RecentNotifications returns the newest rows for the dashboard, optionally
// for a single monitor.
func (s *Store) RecentNotifications(ctx context.Context, monitorID string, limit int) ([]Notification, error) {
	if monitorID == "" {
		return s.queryNotifications(ctx, `SELECT `+notificationColumns+` FROM notifications ORDER BY created_at DESC, id DESC LIMIT ?`, limit)
	}
	return s.queryNotifications(ctx, `SELECT `+notificationColumns+` FROM notifications WHERE monitor_id = ? ORDER BY created_at DESC, id DESC LIMIT ?`, monitorID, limit)
}

// NotificationsForIncident returns every outbox row for an incident.
func (s *Store) NotificationsForIncident(ctx context.Context, incidentID int64) ([]Notification, error) {
	return s.queryNotifications(ctx, `SELECT `+notificationColumns+` FROM notifications WHERE incident_id = ? ORDER BY created_at, id`, incidentID)
}

func (s *Store) queryNotifications(ctx context.Context, query string, args ...any) ([]Notification, error) {
	rows, err := s.r.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// MarkSent records a successful delivery.
func (s *Store) MarkSent(ctx context.Context, id int64, at time.Time) error {
	_, err := s.w.ExecContext(ctx, `UPDATE notifications SET status = 'sent', attempts = attempts + 1, sent_at = ?, last_error = '' WHERE id = ?`, ms(at), id)
	return err
}

// MarkRetry records a failed attempt and schedules the next one. When final
// is set the row is given up on.
func (s *Store) MarkRetry(ctx context.Context, id int64, errMsg string, next time.Time, final bool) error {
	status := NotificationPending
	if final {
		status = NotificationFailed
	}
	_, err := s.w.ExecContext(ctx, `UPDATE notifications SET status = ?, attempts = attempts + 1, last_error = ?, next_attempt_at = ? WHERE id = ?`,
		status, errMsg, ms(next), id)
	return err
}
