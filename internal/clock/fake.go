package clock

import (
	"sort"
	"sync"
	"time"
)

// Fake is a manually advanced clock for tests. Timers fire synchronously
// inside Advance, in deadline order.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	changed chan struct{}
}

// NewFake returns a fake clock set to start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start, changed: make(chan struct{})}
}

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer registers a timer that fires when the fake clock reaches now+d.
func (f *Fake) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{f: f, deadline: f.now.Add(d), ch: make(chan time.Time, 1)}
	if d <= 0 {
		t.ch <- f.now
		return t
	}
	f.timers = append(f.timers, t)
	f.notifyLocked()
	return t
}

// Advance moves the clock forward by d and fires every timer whose deadline
// has passed.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	sort.Slice(f.timers, func(i, j int) bool { return f.timers[i].deadline.Before(f.timers[j].deadline) })
	kept := f.timers[:0]
	for _, t := range f.timers {
		if !t.deadline.After(f.now) {
			t.ch <- t.deadline
			continue
		}
		kept = append(kept, t)
	}
	f.timers = kept
	f.notifyLocked()
}

// Waiters reports how many timers are pending.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

// BlockUntil waits until at least n timers are pending. Tests use it to make
// sure a goroutine under test has armed its timer before advancing time.
func (f *Fake) BlockUntil(n int) {
	for {
		f.mu.Lock()
		if len(f.timers) >= n {
			f.mu.Unlock()
			return
		}
		ch := f.changed
		f.mu.Unlock()
		<-ch
	}
}

func (f *Fake) notifyLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *Fake) remove(t *fakeTimer) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, x := range f.timers {
		if x == t {
			f.timers = append(f.timers[:i], f.timers[i+1:]...)
			f.notifyLocked()
			return true
		}
	}
	return false
}

type fakeTimer struct {
	f        *Fake
	deadline time.Time
	ch       chan time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool          { return t.f.remove(t) }
