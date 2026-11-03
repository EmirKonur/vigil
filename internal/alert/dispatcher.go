package alert

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"vigil/internal/clock"
	"vigil/internal/store"
)

// Dispatcher drains the notification outbox. Delivery is at least once: a
// crash between a successful send and marking the row sent causes one
// resend on restart, which is why every payload carries a stable delivery
// ID that receivers can deduplicate on.
type Dispatcher struct {
	store *store.Store
	clock clock.Clock
	log   *slog.Logger
	// MaxAttempts bounds retries before a row is marked failed.
	MaxAttempts int
	// Observe, when set, is called after every attempt with an outcome of
	// "sent", "retry", or "failed".
	Observe func(notifier string, kind Kind, outcome string)

	mu        sync.RWMutex
	notifiers map[string]Notifier
	wake      chan struct{}
}

// NewDispatcher returns a dispatcher with no notifiers configured.
func NewDispatcher(st *store.Store, clk clock.Clock, log *slog.Logger) *Dispatcher {
	return &Dispatcher{store: st, clock: clk, log: log, MaxAttempts: 8, notifiers: map[string]Notifier{}, wake: make(chan struct{}, 1)}
}

// SetNotifiers swaps the notifier set after a config reload.
func (d *Dispatcher) SetNotifiers(n map[string]Notifier) {
	d.mu.Lock()
	d.notifiers = n
	d.mu.Unlock()
	d.Wake()
}

// Wake makes Run check the outbox now instead of at its next timer.
func (d *Dispatcher) Wake() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// Backoff is the delay before retry number attempt (1-based).
func Backoff(attempt int) time.Duration {
	return min(15*time.Second<<min(attempt-1, 10), 30*time.Minute)
}

// Run delivers notifications until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		if _, err := d.DeliverDue(ctx); err != nil && ctx.Err() == nil {
			d.log.Error("delivering notifications", "err", err)
		}
		wait := 30 * time.Second
		if next, err := d.store.NextNotificationDue(ctx); err == nil && !next.IsZero() {
			wait = min(wait, max(next.Sub(d.clock.Now()), 0))
		}
		timer := d.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-d.wake:
			timer.Stop()
		case <-timer.C():
		}
	}
}

// DeliverDue sends every due notification once and returns how many were
// attempted.
func (d *Dispatcher) DeliverDue(ctx context.Context) (int, error) {
	due, err := d.store.DueNotifications(ctx, d.clock.Now(), 50)
	if err != nil {
		return 0, err
	}
	for _, n := range due {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		d.deliver(ctx, n)
	}
	return len(due), nil
}

func (d *Dispatcher) deliver(ctx context.Context, n store.Notification) {
	log := d.log.With("notifier", n.Notifier, "kind", n.Kind, "monitor", n.MonitorID, "delivery", n.DedupKey)
	d.mu.RLock()
	notifier, ok := d.notifiers[n.Notifier]
	d.mu.RUnlock()

	var msg Message
	err := json.Unmarshal(n.Message, &msg)
	if err == nil {
		d.refresh(ctx, &msg)
	}
	switch {
	case err != nil:
		err = &PermanentError{err}
	case !ok:
		err = &PermanentError{errors.New("notifier is no longer configured")}
	default:
		sendCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err = notifier.Send(sendCtx, msg)
		cancel()
	}

	now := d.clock.Now()
	// Use a fresh context so a shutdown mid-send still records the outcome.
	bookkeeping, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err == nil {
		if err := d.store.MarkSent(bookkeeping, n.ID, now); err != nil {
			log.Error("marking notification sent", "err", err)
		}
		log.Info("notification sent", "attempt", n.Attempts+1)
		d.observe(n, "sent")
		return
	}
	if ctx.Err() != nil {
		// Interrupted by shutdown: leave the row pending without spending
		// an attempt; it goes out on the next start.
		return
	}
	var perm *PermanentError
	final := errors.As(err, &perm) || n.Attempts+1 >= d.MaxAttempts
	next := now.Add(Backoff(n.Attempts + 1))
	if err := d.store.MarkRetry(bookkeeping, n.ID, err.Error(), next, final); err != nil {
		log.Error("recording notification failure", "err", err)
	}
	if final {
		log.Error("notification failed permanently", "attempt", n.Attempts+1, "err", err)
		d.observe(n, "failed")
		return
	}
	log.Warn("notification failed, will retry", "attempt", n.Attempts+1, "retry_in", next.Sub(now), "err", err)
	d.observe(n, "retry")
}

// refresh updates an incident's extent to what is known at send time. An
// incident can widen in the seconds between being raised and the alert
// going out, and the alert should say "major" if that is what it became.
// Resolved alerts keep their snapshot: they describe a finished incident.
func (d *Dispatcher) refresh(ctx context.Context, msg *Message) {
	if msg.Incident == nil || msg.Incident.ID == 0 || msg.Kind == KindResolved {
		return
	}
	inc, err := d.store.Incident(ctx, msg.Incident.ID)
	if err != nil {
		return
	}
	msg.Incident.Impact, msg.Incident.Regions, msg.Incident.Voting = inc.Impact, inc.Regions, inc.Voting
}

func (d *Dispatcher) observe(n store.Notification, outcome string) {
	if d.Observe != nil {
		d.Observe(n.Notifier, Kind(n.Kind), outcome)
	}
}
