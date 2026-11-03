// Package clock abstracts time so that schedulers, leases, and incident timers
// can be driven deterministically in tests.
package clock

import "time"

// Clock is the subset of the time package that Vigil depends on.
type Clock interface {
	Now() time.Time
	// NewTimer returns a timer that fires once after d.
	NewTimer(d time.Duration) Timer
}

// Timer mirrors *time.Timer behind an interface so fakes can implement it.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Real is the wall clock.
type Real struct{}

// Now returns time.Now.
func (Real) Now() time.Time { return time.Now() }

// NewTimer wraps time.NewTimer.
func (Real) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }
