// Package scheduler decides when each monitor runs.
//
// Every monitor gets a fixed phase inside its interval, derived from a hash
// of its ID, so 500 monitors with a 60s interval start spread across the
// minute instead of all at :00. The phase is anchored to the Unix epoch, so
// restarts and config reloads do not move a monitor's slots, and all regions
// of one monitor share the same slot, which is what consensus compares.
//
// A small random jitter is added to each dispatch so that two monitors that
// hash into the same instant do not collide on every run.
//
// A monitor seen for the first time whose next slot is more than Warmup away
// gets one extra slot within Warmup (spread by the same hash), so a new
// hourly check does not show "no data" for an hour.
package scheduler

import (
	"container/heap"
	"context"
	"hash/fnv"
	"math/rand/v2"
	"sync"
	"time"

	"vigil/internal/clock"
)

// Target is a monitor to schedule.
type Target struct {
	ID       string
	Interval time.Duration
}

// DispatchFunc receives each due slot. It is called from the scheduler
// goroutine and must not block for long.
type DispatchFunc func(id string, slot time.Time)

// Scheduler fires DispatchFunc for every target at its slots.
type Scheduler struct {
	clock    clock.Clock
	dispatch DispatchFunc
	// jitter returns a value in [0, 1); replaceable for deterministic tests.
	jitter func() float64

	mu      sync.Mutex
	queue   entryHeap
	byID    map[string]*entry
	wake    chan struct{}
	skipped uint64
}

type entry struct {
	id         string
	interval   time.Duration
	slot       time.Time // next slot to dispatch
	dispatchAt time.Time // slot plus jitter
	index      int
}

// Warmup bounds how long a newly added monitor waits for its first check.
const Warmup = 30 * time.Second

// MaxJitter bounds the random delay added to a dispatch.
func MaxJitter(interval time.Duration) time.Duration {
	return min(interval/10, 2*time.Second)
}

// New returns a scheduler. Call Set to load targets and Run to start it.
func New(c clock.Clock, dispatch DispatchFunc) *Scheduler {
	return &Scheduler{
		clock:    c,
		dispatch: dispatch,
		jitter:   rand.Float64,
		byID:     map[string]*entry{},
		wake:     make(chan struct{}, 1),
	}
}

// Phase is the monitor's fixed offset within its interval.
func Phase(id string, interval time.Duration) time.Duration {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return time.Duration(h.Sum64() % uint64(interval))
}

// NextSlot returns the first slot for id strictly after t.
func NextSlot(id string, interval time.Duration, t time.Time) time.Time {
	phase := int64(Phase(id, interval))
	iv := int64(interval)
	n := t.UnixNano() - phase
	k := n / iv
	if n < 0 && n%iv != 0 {
		k-- // floor division for times before the epoch phase
	}
	return time.Unix(0, phase+(k+1)*iv)
}

// Set replaces the target list. Targets whose interval is unchanged keep
// their next slot; new or changed targets start at their next slot after now.
func (s *Scheduler) Set(targets []Target) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	keep := map[string]bool{}
	for _, t := range targets {
		if t.Interval <= 0 {
			continue
		}
		keep[t.ID] = true
		if e, ok := s.byID[t.ID]; ok {
			if e.interval == t.Interval {
				continue
			}
			heap.Remove(&s.queue, e.index)
			delete(s.byID, t.ID)
		}
		e := &entry{id: t.ID, interval: t.Interval}
		first := NextSlot(t.ID, t.Interval, now)
		if first.Sub(now) > Warmup {
			first = now.Add(Phase(t.ID, Warmup))
		}
		s.arm(e, first)
		heap.Push(&s.queue, e)
		s.byID[t.ID] = e
	}
	for id, e := range s.byID {
		if !keep[id] {
			heap.Remove(&s.queue, e.index)
			delete(s.byID, id)
		}
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Skipped reports how many slots were dropped because the scheduler fell
// more than an interval behind (for example after the host was suspended).
func (s *Scheduler) Skipped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.skipped
}

func (s *Scheduler) arm(e *entry, slot time.Time) {
	e.slot = slot
	e.dispatchAt = slot.Add(time.Duration(s.jitter() * float64(MaxJitter(e.interval))))
}

// Run dispatches slots until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	for {
		s.mu.Lock()
		wait := time.Hour
		if len(s.queue) > 0 {
			wait = s.queue[0].dispatchAt.Sub(s.clock.Now())
		}
		s.mu.Unlock()

		timer := s.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-s.wake:
			timer.Stop()
			continue
		case <-timer.C():
		}
		for _, d := range s.due(s.clock.Now()) {
			s.dispatch(d.id, d.slot)
		}
	}
}

type dueSlot struct {
	id   string
	slot time.Time
}

func (s *Scheduler) due(now time.Time) []dueSlot {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []dueSlot
	for len(s.queue) > 0 && !s.queue[0].dispatchAt.After(now) {
		e := s.queue[0]
		slot := e.slot
		// If we are more than an interval late, only the newest slot is
		// worth running; replaying a backlog would burst every monitor at
		// once, which is the herd this package exists to prevent.
		if latest := NextSlot(e.id, e.interval, now).Add(-e.interval); latest.After(slot) {
			s.skipped += uint64(latest.Sub(slot) / e.interval)
			slot = latest
		}
		out = append(out, dueSlot{e.id, slot})
		// NextSlot rather than slot+interval, so a warm-up slot rejoins the
		// monitor's fixed phase.
		s.arm(e, NextSlot(e.id, e.interval, slot))
		heap.Fix(&s.queue, e.index)
	}
	return out
}

type entryHeap []*entry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].dispatchAt.Before(h[j].dispatchAt) }
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *entryHeap) Push(x any) {
	e := x.(*entry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *entryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}
