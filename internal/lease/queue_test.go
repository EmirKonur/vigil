package lease

import (
	"context"
	"sync"
	"testing"
	"time"

	"vigil/internal/check"
	"vigil/internal/clock"
)

var t0 = time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)

func job(monitor, region string, slot time.Duration) Job {
	return Job{
		ID: NewID(), MonitorID: monitor, Region: region, Slot: t0.Add(slot),
		Spec: check.Spec{Type: check.TypeTCP, Timeout: 5 * time.Second},
	}
}

func lease(t *testing.T, q *Queue, probe, region string, n int) []Leased {
	t.Helper()
	out, err := q.Lease(context.Background(), probe, region, n, 0)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestLeaseIsPartitionedByRegion(t *testing.T) {
	q := NewQueue(clock.NewFake(t0))
	q.Enqueue(job("api", "eu", 0))
	q.Enqueue(job("api", "us", 0))
	q.Enqueue(job("web", "eu", 0))

	eu := lease(t, q, "eu-1", "eu", 10)
	if len(eu) != 2 || eu[0].Region != "eu" || eu[1].Region != "eu" {
		t.Fatalf("eu probe leased %+v", eu)
	}
	if eu[0].TTL != 10*time.Second {
		t.Fatalf("ttl = %s, want timeout+grace", eu[0].TTL)
	}
	if got := lease(t, q, "eu-1", "eu", 10); len(got) != 0 {
		t.Fatalf("leased twice: %+v", got)
	}
	if us := lease(t, q, "us-1", "us", 1); len(us) != 1 {
		t.Fatalf("us probe got %d jobs", len(us))
	}
}

func TestExpiredLeaseIsRequeuedOnceThenDropped(t *testing.T) {
	clk := clock.NewFake(t0)
	q := NewQueue(clk)
	j := job("api", "eu", 0)
	q.Enqueue(j)

	first := lease(t, q, "eu-1", "eu", 1)
	if len(first) != 1 {
		t.Fatal("no job")
	}
	// eu-1 dies mid-check. Before the TTL, nobody else gets the job.
	clk.Advance(9 * time.Second)
	if got := lease(t, q, "eu-2", "eu", 1); len(got) != 0 {
		t.Fatal("job re-leased before its lease expired")
	}
	clk.Advance(2 * time.Second)
	second := lease(t, q, "eu-2", "eu", 1)
	if len(second) != 1 || second[0].ID != j.ID || second[0].Attempt != 1 {
		t.Fatalf("expected the same job as attempt 1, got %+v", second)
	}
	// eu-2 dies too: after MaxAttempts the job is dropped, and the next
	// slot can be enqueued.
	clk.Advance(11 * time.Second)
	if got := lease(t, q, "eu-3", "eu", 1); len(got) != 0 {
		t.Fatalf("job leased a third time: %+v", got)
	}
	if !q.Enqueue(job("api", "eu", 30*time.Second)) {
		t.Fatal("next slot rejected after the old job expired")
	}
	s := q.Stats()
	if s.Requeued != 1 || s.Expired != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestLateReportFromExpiredLeaseIsAcceptedOnce(t *testing.T) {
	clk := clock.NewFake(t0)
	q := NewQueue(clk)
	j := job("api", "eu", 0)
	q.Enqueue(j)
	lease(t, q, "eu-1", "eu", 1)
	clk.Advance(11 * time.Second)
	lease(t, q, "eu-2", "eu", 1) // re-leased after eu-1 went quiet

	// eu-1 was slow, not dead. Its result is the first to arrive and wins.
	if _, ok := q.Complete(j.ID, "eu"); !ok {
		t.Fatal("first report rejected")
	}
	// eu-2's report for the same job is now a duplicate.
	if _, ok := q.Complete(j.ID, "eu"); ok {
		t.Fatal("second report for the same job accepted")
	}
}

func TestCompleteRejectsWrongRegion(t *testing.T) {
	q := NewQueue(clock.NewFake(t0))
	j := job("api", "eu", 0)
	q.Enqueue(j)
	lease(t, q, "eu-1", "eu", 1)
	if _, ok := q.Complete(j.ID, "us"); ok {
		t.Fatal("a probe in another region completed the job")
	}
}

func TestEnqueueCoalescesAndSupersedes(t *testing.T) {
	q := NewQueue(clock.NewFake(t0))
	q.Enqueue(job("api", "eu", 0))
	// No probe has taken slot 0 yet; slot 30s replaces it.
	q.Enqueue(job("api", "eu", 30*time.Second))
	got := lease(t, q, "eu-1", "eu", 10)
	if len(got) != 1 || !got[0].Slot.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("expected only the newest slot, got %+v", got)
	}
	// While slot 30s is in flight, slot 60s is dropped rather than queued.
	if q.Enqueue(job("api", "eu", 60*time.Second)) {
		t.Fatal("enqueued a second job behind an in-flight one")
	}
	q.Complete(got[0].ID, "eu")
	if !q.Enqueue(job("api", "eu", 90*time.Second)) {
		t.Fatal("rejected a job after the previous one completed")
	}
	s := q.Stats()
	if s.Superseded != 1 || s.Coalesced != 1 || s.Pending != 1 || s.InFlight != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestLongPollWakesOnEnqueue(t *testing.T) {
	clk := clock.NewFake(t0)
	q := NewQueue(clk)
	var wg sync.WaitGroup
	var got []Leased
	wg.Go(func() {
		got, _ = q.Lease(context.Background(), "eu-1", "eu", 5, 20*time.Second)
	})
	clk.BlockUntil(1) // the poller is waiting on its timer
	q.Enqueue(job("api", "eu", 0))
	wg.Wait()
	if len(got) != 1 {
		t.Fatalf("long poll returned %d jobs", len(got))
	}
}

func TestLongPollTimesOutAndClosesEarly(t *testing.T) {
	clk := clock.NewFake(t0)
	q := NewQueue(clk)
	done := make(chan []Leased)
	go func() {
		out, _ := q.Lease(context.Background(), "eu-1", "eu", 5, 20*time.Second)
		done <- out
	}()
	clk.BlockUntil(1)
	clk.Advance(20 * time.Second)
	if out := <-done; len(out) != 0 {
		t.Fatalf("timed-out poll returned %d jobs", len(out))
	}

	go func() {
		out, _ := q.Lease(context.Background(), "eu-1", "eu", 5, time.Hour)
		done <- out
	}()
	clk.BlockUntil(1)
	q.Close()
	if out := <-done; len(out) != 0 {
		t.Fatal("closed queue returned jobs")
	}
	if q.Enqueue(job("api", "eu", 0)) {
		t.Fatal("closed queue accepted a job")
	}
}

func TestRemoveDropsPendingJobs(t *testing.T) {
	q := NewQueue(clock.NewFake(t0))
	q.Enqueue(job("api", "eu", 0))
	q.Enqueue(job("web", "eu", 0))
	q.Remove("api")
	got := lease(t, q, "eu-1", "eu", 10)
	if len(got) != 1 || got[0].MonitorID != "web" {
		t.Fatalf("got %+v", got)
	}
}
