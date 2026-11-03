// Package probe runs checks leased from a Vigil server.
//
// A probe is stateless apart from its outbox of unacknowledged results. It
// long-polls for jobs, runs up to Concurrency of them at once, and reports
// results until the server acknowledges each one. On shutdown it stops
// leasing, lets in-flight checks finish (each is bounded by its timeout),
// and makes a final attempt to deliver what is left.
package probe

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"vigil/internal/check"
	"vigil/internal/lease"
	"vigil/internal/probeapi"
)

// Options configures a probe.
type Options struct {
	API         probeapi.API
	ID          string
	Region      string
	Version     string
	Concurrency int
	Runner      *check.Runner
	Log         *slog.Logger
	// PollWait is how long a lease request may wait on the server.
	PollWait time.Duration
	// FlushTimeout bounds the final report attempt on shutdown.
	FlushTimeout time.Duration
}

// Probe is a running checker.
type Probe struct {
	opts   Options
	outbox chan probeapi.Result
}

// maxPending bounds results kept while the server is unreachable. At a few
// hundred bytes each this caps memory at well under a megabyte; past it the
// oldest results are the least useful and are dropped first.
const maxPending = 2000

// New returns a probe.
func New(opts Options) *Probe {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 16
	}
	if opts.PollWait == 0 {
		opts.PollWait = 20 * time.Second
	}
	if opts.FlushTimeout == 0 {
		opts.FlushTimeout = 10 * time.Second
	}
	if opts.Runner == nil {
		opts.Runner = &check.Runner{}
	}
	return &Probe{opts: opts, outbox: make(chan probeapi.Result, opts.Concurrency*4)}
}

// Run leases and runs checks until ctx is cancelled, then drains.
func (p *Probe) Run(ctx context.Context) error {
	log := p.opts.Log.With("probe", p.opts.ID, "region", p.opts.Region)
	reporterDone := make(chan struct{})
	go func() {
		p.report(ctx, log)
		close(reporterDone)
	}()

	slots := make(chan struct{}, p.opts.Concurrency)
	var checks sync.WaitGroup
	backoff := time.Second
	for ctx.Err() == nil {
		free := p.opts.Concurrency - len(slots)
		if free == 0 {
			// Wait for a check to finish before asking for more work.
			select {
			case slots <- struct{}{}:
				<-slots
			case <-ctx.Done():
			}
			continue
		}
		resp, err := p.opts.API.Lease(ctx, probeapi.LeaseRequest{
			ProbeID: p.opts.ID, Region: p.opts.Region, Version: p.opts.Version,
			Max: free, Wait: p.opts.PollWait, ProbeTime: time.Now(),
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Warn("lease failed", "err", err, "retry_in", backoff)
			sleep(ctx, backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		for _, job := range resp.Jobs {
			slots <- struct{}{}
			checks.Go(func() {
				defer func() { <-slots }()
				p.outbox <- p.run(ctx, job)
			})
		}
	}

	checks.Wait()
	close(p.outbox)
	<-reporterDone
	log.Info("probe stopped")
	return nil
}

// run executes one job. The deadline comes from the lease TTL measured on
// this machine's monotonic clock from the moment the lease arrived, so it
// holds regardless of how far this probe's wall clock is from the server's.
func (p *Probe) run(ctx context.Context, job lease.Leased) probeapi.Result {
	deadline := time.Now().Add(job.TTL - time.Second)
	// Detached from ctx: a shutdown lets in-flight checks finish.
	jobCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	return probeapi.Result{JobID: job.ID, Result: p.opts.Runner.Run(jobCtx, job.Spec)}
}

// report delivers results at least once. It returns when the outbox is
// closed and drained, or when the flush deadline passes after shutdown.
func (p *Probe) report(ctx context.Context, log *slog.Logger) {
	var pending []probeapi.Result
	backoff := time.Second
	open := true
	var flushDeadline <-chan time.Time

	for open || len(pending) > 0 {
		if len(pending) == 0 {
			r, ok := <-p.outbox
			if !ok {
				return
			}
			pending = append(pending, r)
		}
		// Batch whatever else is ready without waiting for it.
	collect:
		for open && len(pending) < 100 {
			select {
			case r, ok := <-p.outbox:
				if !ok {
					open = false
					break collect
				}
				pending = append(pending, r)
			default:
				break collect
			}
		}
		if dropped := len(pending) - maxPending; dropped > 0 {
			log.Warn("server unreachable for too long, dropping oldest results", "dropped", dropped)
			pending = pending[dropped:]
		}

		if ctx.Err() != nil && flushDeadline == nil {
			flushDeadline = time.After(p.opts.FlushTimeout)
		}
		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		resp, err := p.opts.API.Report(sendCtx, probeapi.ReportRequest{ProbeID: p.opts.ID, Region: p.opts.Region, Results: pending})
		cancel()
		if err != nil {
			log.Warn("report failed", "results", len(pending), "err", err, "retry_in", backoff)
			select {
			case <-time.After(backoff):
			case <-flushDeadline:
				log.Error("giving up on unreported results at shutdown", "results", len(pending))
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		acked := make(map[string]bool, len(resp.Acks))
		for _, a := range resp.Acks {
			acked[a.JobID] = true
			if a.Status == probeapi.AckUnknown {
				log.Debug("server did not recognize job; dropping result", "job", a.JobID)
			}
		}
		kept := pending[:0]
		for _, r := range pending {
			if !acked[r.JobID] {
				kept = append(kept, r)
			}
		}
		pending = kept
		if len(pending) > 0 {
			// The server answered but could not store some results; give
			// it a moment rather than resending in a tight loop.
			select {
			case <-time.After(backoff):
			case <-flushDeadline:
				return
			}
			backoff = min(backoff*2, 30*time.Second)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
