package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"vigil/internal/clock"
)

var start = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

func TestNextSlotIsAlignedAndStable(t *testing.T) {
	for _, iv := range []time.Duration{time.Second, 10 * time.Second, 30 * time.Second, time.Minute, time.Hour} {
		for i := range 50 {
			id := fmt.Sprintf("monitor-%d", i)
			phase := Phase(id, iv)
			at := start.Add(time.Duration(i) * 7919 * time.Millisecond)
			slot := NextSlot(id, iv, at)
			if !slot.After(at) || slot.Sub(at) > iv {
				t.Fatalf("%s/%s: slot %s not within (at, at+iv]", id, iv, slot)
			}
			if (time.Duration(slot.UnixNano())-phase)%iv != 0 {
				t.Fatalf("%s/%s: slot %s off phase %s", id, iv, slot, phase)
			}
			if again := NextSlot(id, iv, slot.Add(-time.Nanosecond)); !again.Equal(slot) {
				t.Fatalf("%s: slot not stable: %s vs %s", id, slot, again)
			}
			if next := NextSlot(id, iv, slot); next.Sub(slot) != iv {
				t.Fatalf("%s: consecutive slots %s apart", id, next.Sub(slot))
			}
		}
	}
}

// TestPhasesSpreadLoad checks that many monitors with the same interval do
// not start in the same second. With 600 monitors over 60 seconds the mean
// is 10 per second; a herd would put all 600 in one.
func TestPhasesSpreadLoad(t *testing.T) {
	const monitors, iv = 600, time.Minute
	perSecond := map[int64]int{}
	for i := range monitors {
		slot := NextSlot(fmt.Sprintf("svc-%03d", i), iv, start)
		perSecond[slot.Unix()]++
	}
	worst := 0
	for _, n := range perSecond {
		worst = max(worst, n)
	}
	if worst > 25 {
		t.Fatalf("busiest second has %d of %d monitors", worst, monitors)
	}
	if len(perSecond) < 50 {
		t.Fatalf("only %d distinct seconds used out of 60", len(perSecond))
	}
}

type recorder struct {
	mu    sync.Mutex
	slots map[string][]time.Time
	at    map[string][]time.Time
	clk   *clock.Fake
}

func (r *recorder) dispatch(id string, slot time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slots[id] = append(r.slots[id], slot)
	r.at[id] = append(r.at[id], r.clk.Now())
}

func (r *recorder) snapshot() (map[string][]time.Time, map[string][]time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, a := map[string][]time.Time{}, map[string][]time.Time{}
	for k, v := range r.slots {
		s[k] = append([]time.Time(nil), v...)
		a[k] = append([]time.Time(nil), r.at[k]...)
	}
	return s, a
}

// runScheduler starts a scheduler on a fake clock with a fixed jitter fraction.
func runScheduler(t *testing.T, jitter float64, targets ...Target) (*Scheduler, *clock.Fake, *recorder) {
	t.Helper()
	clk := clock.NewFake(start)
	rec := &recorder{slots: map[string][]time.Time{}, at: map[string][]time.Time{}, clk: clk}
	s := New(clk, rec.dispatch)
	s.jitter = func() float64 { return jitter }
	s.Set(targets)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return s, clk, rec
}

// step advances the fake clock in small increments, letting the scheduler
// goroutine re-arm its timer between steps.
func step(clk *clock.Fake, total, by time.Duration) {
	for elapsed := time.Duration(0); elapsed < total; elapsed += by {
		clk.BlockUntil(1)
		clk.Advance(by)
	}
	clk.BlockUntil(1)
}

func TestRunDispatchesEverySlotWithBoundedJitter(t *testing.T) {
	targets := []Target{{"api", 10 * time.Second}, {"web", 10 * time.Second}, {"db", 30 * time.Second}}
	_, clk, rec := runScheduler(t, 0.5, targets...)
	step(clk, 2*time.Minute, 250*time.Millisecond)

	slots, at := rec.snapshot()
	for _, tg := range targets {
		got := slots[tg.ID]
		want := int(2 * time.Minute / tg.Interval)
		if len(got) < want-1 || len(got) > want {
			t.Fatalf("%s: %d dispatches in 2m, want about %d", tg.ID, len(got), want)
		}
		for i := range got {
			if i > 0 && got[i].Sub(got[i-1]) != tg.Interval {
				t.Fatalf("%s: slots %s apart, want %s", tg.ID, got[i].Sub(got[i-1]), tg.Interval)
			}
			delay := at[tg.ID][i].Sub(got[i])
			if delay < 0 || delay > MaxJitter(tg.Interval)+250*time.Millisecond {
				t.Fatalf("%s: dispatched %s after its slot (max jitter %s)", tg.ID, delay, MaxJitter(tg.Interval))
			}
		}
	}
}

func TestRunSkipsBacklogAfterSuspend(t *testing.T) {
	s, clk, rec := runScheduler(t, 0, Target{"api", 10 * time.Second})
	clk.BlockUntil(1)
	// The host sleeps for five minutes: one timer fires, very late.
	clk.Advance(5 * time.Minute)
	clk.BlockUntil(1)

	slots, _ := rec.snapshot()
	if len(slots["api"]) != 1 {
		t.Fatalf("dispatched %d slots after suspend, want only the latest", len(slots["api"]))
	}
	if behind := clk.Now().Sub(slots["api"][0]); behind < 0 || behind >= 10*time.Second {
		t.Fatalf("dispatched slot is %s old", behind)
	}
	if s.Skipped() < 25 {
		t.Fatalf("skipped = %d, want the ~29 missed slots counted", s.Skipped())
	}
}

func TestSetKeepsPhaseAcrossReloads(t *testing.T) {
	s, clk, rec := runScheduler(t, 0, Target{"api", 10 * time.Second}, Target{"old", 10 * time.Second})
	step(clk, 30*time.Second, time.Second)

	// Reload: api unchanged, old removed, new added.
	s.Set([]Target{{"api", 10 * time.Second}, {"new", 5 * time.Second}})
	step(clk, 30*time.Second, time.Second)

	slots, _ := rec.snapshot()
	api := slots["api"]
	for i := 1; i < len(api); i++ {
		if api[i].Sub(api[i-1]) != 10*time.Second {
			t.Fatalf("reload shifted api's slots: %v", api)
		}
	}
	for _, sl := range slots["old"] {
		if sl.After(start.Add(30 * time.Second)) {
			t.Fatalf("removed monitor still dispatched at %s", sl)
		}
	}
	if n := len(slots["new"]); n < 5 || n > 6 {
		t.Fatalf("new monitor dispatched %d times in 30s at 5s interval", n)
	}
}

func TestNewLongIntervalMonitorGetsAWarmupCheck(t *testing.T) {
	_, clk, rec := runScheduler(t, 0, Target{"cert", time.Hour})
	step(clk, Warmup, time.Second)
	slots, _ := rec.snapshot()
	if len(slots["cert"]) != 1 {
		t.Fatalf("hourly monitor: %d dispatches in the first %s, want 1 warm-up", len(slots["cert"]), Warmup)
	}
	// After the warm-up it runs on its fixed phase.
	step(clk, time.Hour, time.Minute)
	slots, _ = rec.snapshot()
	if len(slots["cert"]) != 2 || !slots["cert"][1].Equal(NextSlot("cert", time.Hour, start)) {
		t.Fatalf("slots after warm-up = %v, want the regular slot %v", slots["cert"], NextSlot("cert", time.Hour, start))
	}
}
