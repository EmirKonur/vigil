// Package engine is the server's core loop: it turns scheduler slots into
// leased jobs, applies probe results to the consensus state machine, and
// persists every transition together with the notifications it causes.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"vigil/internal/alert"
	"vigil/internal/clock"
	"vigil/internal/config"
	"vigil/internal/consensus"
	"vigil/internal/lease"
	"vigil/internal/probeapi"
	"vigil/internal/scheduler"
	"vigil/internal/store"
)

// Topics published to OnChange.
const (
	TopicMonitors  = "monitors"
	TopicIncidents = "incidents"
	TopicProbes    = "probes"
)

// Options configures an Engine.
type Options struct {
	Store   *store.Store
	Clock   clock.Clock
	Log     *slog.Logger
	Metrics *Metrics
	// BaseURL is the externally reachable server URL, used for links in
	// alerts. Empty omits links.
	BaseURL string
	// UserAgent is sent by webhook notifiers.
	UserAgent string
	// Demo marks alerts as coming from a demo installation.
	Demo bool
	// OnChange is called (from engine goroutines) when a topic's data
	// changes. It must not block.
	OnChange func(topic string)
	// TickEvery is how often timers (reminders, staleness) are evaluated.
	TickEvery time.Duration
	// RollupEvery is how often raw results are rolled up.
	RollupEvery time.Duration
}

// Engine owns the scheduler, the job queue, and the notification
// dispatcher, and is the only writer of monitor state.
type Engine struct {
	opts       Options
	store      *store.Store
	clock      clock.Clock
	log        *slog.Logger
	metrics    *Metrics
	queue      *lease.Queue
	sched      *scheduler.Scheduler
	dispatcher *alert.Dispatcher

	// mu serializes state transitions. Throughput is bounded by SQLite's
	// single writer anyway, and one lock makes it obvious that a monitor's
	// state and its persisted copy never diverge.
	mu       sync.Mutex
	cfg      *config.Config
	monitors map[string]config.Monitor
	states   map[string]*consensus.State
	paused   map[string]bool

	probes probeRegistry
}

// New loads persisted state and returns an engine. Call Configure before Run.
func New(ctx context.Context, opts Options) (*Engine, error) {
	if opts.TickEvery == 0 {
		opts.TickEvery = 5 * time.Second
	}
	if opts.RollupEvery == 0 {
		opts.RollupEvery = time.Minute
	}
	if opts.OnChange == nil {
		opts.OnChange = func(string) {}
	}
	if opts.Metrics == nil {
		opts.Metrics = NewMetrics(nil)
	}
	states, err := opts.Store.States(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading monitor state: %w", err)
	}
	e := &Engine{
		opts:     opts,
		store:    opts.Store,
		clock:    opts.Clock,
		log:      opts.Log,
		metrics:  opts.Metrics,
		queue:    lease.NewQueue(opts.Clock),
		states:   states,
		monitors: map[string]config.Monitor{},
		paused:   map[string]bool{},
		probes:   probeRegistry{lastWrite: map[string]time.Time{}},
	}
	e.sched = scheduler.New(opts.Clock, e.dispatch)
	e.dispatcher = alert.NewDispatcher(opts.Store, opts.Clock, opts.Log.With("component", "dispatcher"))
	e.dispatcher.Observe = func(notifier string, kind alert.Kind, outcome string) {
		e.metrics.Notifications.WithLabelValues(notifier, string(kind), outcome).Inc()
	}
	e.metrics.observeQueue(e.queue, e.sched)
	return e, nil
}

// Configure applies a loaded config. It is safe to call while running; a
// reload keeps every monitor's state and schedule phase.
func (e *Engine) Configure(ctx context.Context, cfg *config.Config) error {
	now := e.clock.Now()
	rows := make([]store.Monitor, len(cfg.Monitors))
	for i, m := range cfg.Monitors {
		rows[i] = store.Monitor{ID: m.ID, Name: m.Name, Type: string(m.Spec.Type), Target: m.Spec.Target(), Interval: m.Interval, Regions: m.Policy.Regions}
	}
	if err := e.store.SyncMonitors(ctx, rows, now); err != nil {
		return fmt.Errorf("syncing monitors: %w", err)
	}
	stored, err := e.store.Monitors(ctx)
	if err != nil {
		return err
	}

	notifiers := map[string]alert.Notifier{}
	for name, n := range cfg.Notifiers {
		notifiers[name] = alert.New(n, e.opts.UserAgent)
	}
	e.dispatcher.SetNotifiers(notifiers)

	e.mu.Lock()
	e.cfg = cfg
	e.monitors = map[string]config.Monitor{}
	e.paused = map[string]bool{}
	for _, m := range cfg.Monitors {
		e.monitors[m.ID] = m
		e.paused[m.ID] = stored[m.ID].Paused
		if e.states[m.ID] == nil {
			e.states[m.ID] = consensus.New()
		}
	}
	targets := e.targetsLocked()
	e.mu.Unlock()

	e.sched.Set(targets)
	e.opts.OnChange(TopicMonitors)
	return nil
}

func (e *Engine) targetsLocked() []scheduler.Target {
	var out []scheduler.Target
	for _, m := range e.cfg.Monitors {
		if !e.paused[m.ID] {
			out = append(out, scheduler.Target{ID: m.ID, Interval: m.Interval})
		}
	}
	return out
}

// Config returns the active configuration.
func (e *Engine) Config() *config.Config {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cfg
}

// Run starts the scheduler, dispatcher, and maintenance loops, and blocks
// until ctx is cancelled and they have stopped.
func (e *Engine) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Go(func() { _ = e.sched.Run(ctx) })
	wg.Go(func() { _ = e.dispatcher.Run(ctx) })
	wg.Go(func() { e.every(ctx, e.opts.TickEvery, e.tick) })
	wg.Go(func() { e.every(ctx, time.Second, func(context.Context) { e.queue.Reap() }) })
	wg.Go(func() { e.every(ctx, e.opts.RollupEvery, e.rollup) })
	<-ctx.Done()
	// Wake long-polling probes so HTTP shutdown is not held up by them.
	e.queue.Close()
	wg.Wait()
	return nil
}

func (e *Engine) every(ctx context.Context, d time.Duration, fn func(context.Context)) {
	fn(ctx)
	for {
		timer := e.clock.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
			fn(ctx)
		}
	}
}

// dispatch turns a scheduler slot into one job per region.
func (e *Engine) dispatch(monitorID string, slot time.Time) {
	e.mu.Lock()
	m, ok := e.monitors[monitorID]
	paused := e.paused[monitorID]
	e.mu.Unlock()
	if !ok || paused {
		return
	}
	for _, region := range m.Policy.Regions {
		e.queue.Enqueue(lease.Job{ID: lease.NewID(), MonitorID: m.ID, Region: region, Slot: slot, Spec: m.Spec})
	}
}

// Lease hands jobs to a probe and records its heartbeat.
func (e *Engine) Lease(ctx context.Context, req probeapi.LeaseRequest, remoteAddr string, embedded bool) (probeapi.LeaseResponse, error) {
	now := e.clock.Now()
	var skew time.Duration
	if !req.ProbeTime.IsZero() && !embedded {
		skew = req.ProbeTime.Sub(now)
	}
	e.heartbeat(ctx, store.Probe{
		ID: req.ProbeID, Region: req.Region, Version: req.Version, Embedded: embedded,
		RemoteAddr: remoteAddr, LastSeen: now, ClockSkew: skew,
	})
	jobs, err := e.queue.Lease(ctx, req.ProbeID, req.Region, max(req.Max, 1), min(req.Wait, 30*time.Second))
	if err != nil && !errors.Is(err, context.Canceled) {
		return probeapi.LeaseResponse{}, err
	}
	if jobs == nil {
		jobs = []lease.Leased{}
	}
	return probeapi.LeaseResponse{Jobs: jobs, ServerTime: e.clock.Now()}, nil
}

// Report ingests a batch of results.
func (e *Engine) Report(ctx context.Context, req probeapi.ReportRequest) (probeapi.ReportResponse, error) {
	acks := make([]probeapi.Ack, 0, len(req.Results))
	accepted := 0
	for _, r := range req.Results {
		status, err := e.ingest(ctx, req.ProbeID, req.Region, r)
		if err != nil {
			// Unacknowledged results are retried by the probe.
			e.log.Error("ingesting result", "job", r.JobID, "probe", req.ProbeID, "err", err)
			continue
		}
		if status == probeapi.AckAccepted {
			accepted++
		}
		acks = append(acks, probeapi.Ack{JobID: r.JobID, Status: status})
	}
	if accepted > 0 {
		e.probes.addCompleted(req.ProbeID, accepted)
		e.opts.OnChange(TopicMonitors)
	}
	return probeapi.ReportResponse{Acks: acks}, nil
}

func (e *Engine) ingest(ctx context.Context, probeID, region string, r probeapi.Result) (string, error) {
	job, ok := e.queue.Complete(r.JobID, region)
	if !ok {
		exists, err := e.store.ResultExists(ctx, r.JobID)
		if err != nil {
			return "", err
		}
		if exists {
			e.metrics.Results.WithLabelValues("duplicate").Inc()
			return probeapi.AckDuplicate, nil
		}
		e.metrics.Results.WithLabelValues("unknown").Inc()
		return probeapi.AckUnknown, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	m, known := e.monitors[job.MonitorID]
	if !known {
		// Removed from the config while the check was in flight.
		return probeapi.AckAccepted, nil
	}
	now := e.clock.Now()
	st := e.states[m.ID].Clone()
	var changed []string
	var notified bool
	err := e.store.Tx(ctx, func(tx *store.Tx) error {
		inserted, err := tx.InsertResult(ctx, store.Result{
			ID: job.ID, MonitorID: m.ID, Region: region, ProbeID: probeID,
			Slot: job.Slot, ReceivedAt: now, OK: r.OK, LatencyMS: float64(r.Latency.Microseconds()) / 1000,
			StatusCode: r.StatusCode, Error: r.Error, Warning: r.Warning,
		})
		if err != nil || !inserted {
			return err
		}
		if e.paused[m.ID] {
			return nil
		}
		effects := st.ApplyResult(m.Policy, consensus.Result{Region: region, OK: r.OK, Slot: job.Slot, Error: r.Error}, now)
		changed, notified, err = e.applyEffects(ctx, tx, m, st, effects)
		if err != nil {
			return err
		}
		if r.Warning != "" && !r.CertNotAfter.IsZero() {
			added, err := e.enqueueCertWarning(ctx, tx, m, r, now)
			if err != nil {
				return err
			}
			notified = notified || added
		}
		return tx.SaveState(ctx, m.ID, st, now)
	})
	if err != nil {
		return "", err
	}
	e.states[m.ID] = st
	e.afterCommit(m, st, changed, notified)

	outcome := "up"
	if !r.OK {
		outcome = "down"
	}
	e.metrics.Checks.WithLabelValues(m.ID, region, outcome).Inc()
	if r.OK {
		e.metrics.Latency.WithLabelValues(region).Observe(r.Latency.Seconds())
	}
	e.metrics.Results.WithLabelValues("accepted").Inc()
	return probeapi.AckAccepted, nil
}

// tick advances every monitor's timers.
func (e *Engine) tick(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cfg == nil {
		return
	}
	now := e.clock.Now()
	for _, m := range e.cfg.Monitors {
		if e.paused[m.ID] {
			continue
		}
		before := e.states[m.ID]
		st := before.Clone()
		effects := st.Tick(m.Policy, now)
		if len(effects) == 0 && st.Phase == before.Phase && st.Flapping == before.Flapping {
			// Timers that did not fire leave nothing worth persisting.
			continue
		}
		var changed []string
		var notified bool
		err := e.store.Tx(ctx, func(tx *store.Tx) error {
			var err error
			changed, notified, err = e.applyEffects(ctx, tx, m, st, effects)
			if err != nil {
				return err
			}
			return tx.SaveState(ctx, m.ID, st, now)
		})
		if err != nil {
			if ctx.Err() == nil {
				e.log.Error("applying timers", "monitor", m.ID, "err", err)
			}
			continue
		}
		e.states[m.ID] = st
		e.afterCommit(m, st, changed, notified)
	}
}

func (e *Engine) afterCommit(m config.Monitor, st *consensus.State, changed []string, notified bool) {
	down := 0.0
	if st.Phase == consensus.PhaseDown {
		down = 1
	}
	e.metrics.MonitorDown.WithLabelValues(m.ID).Set(down)
	for _, topic := range changed {
		e.opts.OnChange(topic)
	}
	if notified {
		e.dispatcher.Wake()
	}
}

// SetPaused pauses or resumes a monitor. Pausing stops scheduling and drops
// queued jobs; resuming forgets old streaks so stale results cannot count.
func (e *Engine) SetPaused(ctx context.Context, id string, paused bool) error {
	now := e.clock.Now()
	e.mu.Lock()
	if _, ok := e.monitors[id]; !ok {
		e.mu.Unlock()
		return store.ErrNotFound
	}
	if err := e.store.SetPaused(ctx, id, paused, now); err != nil {
		e.mu.Unlock()
		return err
	}
	e.paused[id] = paused
	if !paused {
		st := e.states[id].Clone()
		st.Reset()
		if err := e.store.Tx(ctx, func(tx *store.Tx) error { return tx.SaveState(ctx, id, st, now) }); err != nil {
			e.mu.Unlock()
			return err
		}
		e.states[id] = st
	}
	targets := e.targetsLocked()
	e.mu.Unlock()

	if paused {
		e.queue.Remove(id)
	}
	e.sched.Set(targets)
	e.log.Info("monitor paused state changed", "monitor", id, "paused", paused)
	e.opts.OnChange(TopicMonitors)
	return nil
}

// MonitorStatus is a read-only view of a monitor for the UI.
type MonitorStatus struct {
	Monitor config.Monitor
	State   *consensus.State
	Paused  bool
}

// Statuses returns every configured monitor's current state, in config order.
func (e *Engine) Statuses() []MonitorStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cfg == nil {
		return nil
	}
	out := make([]MonitorStatus, 0, len(e.cfg.Monitors))
	for _, m := range e.cfg.Monitors {
		out = append(out, MonitorStatus{Monitor: m, State: e.states[m.ID].Clone(), Paused: e.paused[m.ID]})
	}
	return out
}

// Status returns one monitor's state.
func (e *Engine) Status(id string) (MonitorStatus, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.monitors[id]
	if !ok {
		return MonitorStatus{}, false
	}
	return MonitorStatus{Monitor: m, State: e.states[id].Clone(), Paused: e.paused[id]}, true
}

// QueueStats exposes the job queue counters.
func (e *Engine) QueueStats() lease.Stats { return e.queue.Stats() }

// DeliverPending makes one delivery pass over the outbox, used to flush
// notifications during shutdown.
func (e *Engine) DeliverPending(ctx context.Context) {
	if _, err := e.dispatcher.DeliverDue(ctx); err != nil && ctx.Err() == nil {
		e.log.Error("flushing notifications", "err", err)
	}
}
