package probe

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"vigil/internal/check"
	"vigil/internal/lease"
	"vigil/internal/probeapi"
)

// fakeServer hands out a fixed set of jobs and fails the first few report
// calls, like a server that is briefly unreachable.
type fakeServer struct {
	mu          sync.Mutex
	jobs        []lease.Leased
	failReports int
	reports     int
	received    map[string]int // job ID -> deliveries the server saw
}

func (f *fakeServer) Lease(ctx context.Context, req probeapi.LeaseRequest) (probeapi.LeaseResponse, error) {
	f.mu.Lock()
	n := min(req.Max, len(f.jobs))
	out := f.jobs[:n]
	f.jobs = f.jobs[n:]
	f.mu.Unlock()
	if n == 0 {
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	return probeapi.LeaseResponse{Jobs: out}, nil
}

func (f *fakeServer) Report(_ context.Context, req probeapi.ReportRequest) (probeapi.ReportResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports++
	if f.failReports > 0 {
		f.failReports--
		return probeapi.ReportResponse{}, errors.New("connection refused")
	}
	var resp probeapi.ReportResponse
	for _, r := range req.Results {
		f.received[r.JobID]++
		resp.Acks = append(resp.Acks, probeapi.Ack{JobID: r.JobID, Status: probeapi.AckAccepted})
	}
	return resp, nil
}

func (f *fakeServer) delivered() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for k, v := range f.received {
		out[k] = v
	}
	return out
}

func TestResultsSurviveReportFailures(t *testing.T) {
	jobs := make([]lease.Leased, 20)
	for i := range jobs {
		jobs[i] = lease.Leased{Job: lease.Job{ID: lease.NewID(), Spec: check.Spec{Type: "noop", Timeout: time.Second}}, TTL: 3 * time.Second}
	}
	srv := &fakeServer{jobs: append([]lease.Leased(nil), jobs...), failReports: 2, received: map[string]int{}}
	p := New(Options{API: srv, ID: "t1", Region: "lab", Concurrency: 4, Log: slog.New(slog.DiscardHandler)})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()

	deadline := time.Now().Add(10 * time.Second)
	for len(srv.delivered()) < len(jobs) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	got := srv.delivered()
	for _, j := range jobs {
		if got[j.ID] != 1 {
			t.Fatalf("job %s delivered %d times after retries, want once", j.ID, got[j.ID])
		}
	}
}

func TestShutdownFlushesInFlightResults(t *testing.T) {
	srv := &fakeServer{received: map[string]int{}}
	job := lease.Leased{Job: lease.Job{ID: "slow", Spec: check.Spec{Type: check.TypeTCP, Timeout: 300 * time.Millisecond, TCP: &check.TCP{Address: "192.0.2.1:9"}}}, TTL: 2 * time.Second}
	srv.jobs = []lease.Leased{job}
	p := New(Options{API: srv, ID: "t1", Region: "lab", Concurrency: 1, Log: slog.New(slog.DiscardHandler)})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = p.Run(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond) // the job is leased and its check is running
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not stop")
	}
	if srv.delivered()["slow"] != 1 {
		t.Fatal("in-flight result was not reported during shutdown")
	}
}
