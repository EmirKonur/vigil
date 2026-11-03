// Package lease hands check jobs to probes under time-limited leases.
//
// The protocol is pull-based: a probe long-polls for jobs in its region and
// gets each one with a lease TTL, a duration rather than a deadline. The
// server tracks expiry on its own clock, so a probe whose wall clock is off
// by minutes still behaves correctly; probes never compare timestamps with
// the server.
//
// If a probe dies mid-check its lease expires and the job goes back to the
// front of the region's queue for another probe, once. Results are keyed by
// job ID, so when the "dead" probe turns out to be merely slow and reports
// after all, the second result for the same job is recognized as a
// duplicate instead of counting as another check.
package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"vigil/internal/check"
	"vigil/internal/clock"
)

// Job is one check for one region at one slot.
type Job struct {
	ID        string     `json:"id"`
	MonitorID string     `json:"monitor_id"`
	Region    string     `json:"region"`
	Slot      time.Time  `json:"slot"`
	Spec      check.Spec `json:"spec"`
	Attempt   int        `json:"attempt"`
}

// NewID returns a random 128-bit job ID.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Leased is a job plus the lease the probe holds on it.
type Leased struct {
	Job
	TTL time.Duration `json:"lease_ttl"`
}

// Stats are cumulative counters plus current depths, exported as metrics.
type Stats struct {
	Enqueued   uint64
	Superseded uint64 // pending job replaced by a newer slot before any probe took it
	Coalesced  uint64 // slot dropped because the previous one is still running
	Leased     uint64
	Requeued   uint64 // lease expired, job given to another probe
	Expired    uint64 // lease expired and the job was dropped
	Completed  uint64
	Pending    int
	InFlight   int
}

// Queue holds pending and leased jobs, partitioned by region.
type Queue struct {
	clock clock.Clock
	// Grace is added to a job's check timeout to form its lease TTL. It
	// covers queueing inside the probe and the round trip of the report.
	Grace time.Duration
	// MaxAttempts is how many probes may try one job.
	MaxAttempts int

	mu      sync.Mutex
	pending map[string][]*item // region -> FIFO
	leased  map[string]*item   // job ID -> item
	byKey   map[string]*item   // monitor/region -> its current job
	waiters map[string]chan struct{}
	closed  bool
	stats   Stats
}

type item struct {
	job     Job
	probe   string
	expires time.Time
}

// NewQueue returns an empty queue.
func NewQueue(c clock.Clock) *Queue {
	return &Queue{
		clock:       c,
		Grace:       5 * time.Second,
		MaxAttempts: 2,
		pending:     map[string][]*item{},
		leased:      map[string]*item{},
		byKey:       map[string]*item{},
		waiters:     map[string]chan struct{}{},
	}
}

func key(j Job) string { return j.MonitorID + "/" + j.Region }

// Enqueue adds a job. It returns false when the previous job for the same
// monitor and region is still leased: that check is in flight and will
// report soon, and stacking a second one behind it only builds a backlog on
// a slow target. A job still waiting for a probe is replaced by the newer
// slot, because a stale slot is worth less than a fresh one.
func (q *Queue) Enqueue(j Job) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if cur, ok := q.byKey[key(j)]; ok {
		if _, inFlight := q.leased[cur.job.ID]; inFlight {
			q.stats.Coalesced++
			return false
		}
		cur.job = j
		q.stats.Superseded++
		return true
	}
	it := &item{job: j}
	q.pending[j.Region] = append(q.pending[j.Region], it)
	q.byKey[key(j)] = it
	q.stats.Enqueued++
	q.broadcastLocked(j.Region)
	return true
}

// Lease returns up to max jobs for region, waiting up to wait for one to
// arrive. It returns early with no jobs when the queue is closed.
func (q *Queue) Lease(ctx context.Context, probeID, region string, max int, wait time.Duration) ([]Leased, error) {
	deadline := q.clock.Now().Add(wait)
	for {
		q.mu.Lock()
		q.reapLocked()
		out := q.takeLocked(probeID, region, max)
		if len(out) > 0 || q.closed {
			q.mu.Unlock()
			return out, nil
		}
		ch, ok := q.waiters[region]
		if !ok {
			ch = make(chan struct{})
			q.waiters[region] = ch
		}
		q.mu.Unlock()

		remaining := deadline.Sub(q.clock.Now())
		if remaining <= 0 {
			return nil, nil
		}
		timer := q.clock.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-ch:
			timer.Stop()
		case <-timer.C():
			return nil, nil
		}
	}
}

func (q *Queue) takeLocked(probeID, region string, max int) []Leased {
	queue := q.pending[region]
	n := min(max, len(queue))
	if n <= 0 {
		return nil
	}
	now := q.clock.Now()
	out := make([]Leased, 0, n)
	for _, it := range queue[:n] {
		ttl := it.job.Spec.Timeout + q.Grace
		it.probe = probeID
		it.expires = now.Add(ttl)
		q.leased[it.job.ID] = it
		out = append(out, Leased{Job: it.job, TTL: ttl})
	}
	q.pending[region] = append(queue[:0:0], queue[n:]...)
	q.stats.Leased += uint64(n)
	return out
}

// Complete releases the lease for a reported job. ok is false when the job
// is not leased: it was already completed (a duplicate report), expired and
// dropped, or issued by a previous server process.
func (q *Queue) Complete(jobID, region string) (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.leased[jobID]
	if !ok || it.job.Region != region {
		return Job{}, false
	}
	delete(q.leased, jobID)
	if q.byKey[key(it.job)] == it {
		delete(q.byKey, key(it.job))
	}
	q.stats.Completed++
	return it.job, true
}

// Reap expires overdue leases. Lease calls reap lazily; the server also
// calls this periodically so metrics stay current when no probe is polling.
func (q *Queue) Reap() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.reapLocked()
}

func (q *Queue) reapLocked() {
	now := q.clock.Now()
	for id, it := range q.leased {
		if now.Before(it.expires) {
			continue
		}
		delete(q.leased, id)
		if it.job.Attempt+1 < q.MaxAttempts && q.byKey[key(it.job)] == it {
			// Front of the queue: this job is older than anything pending.
			it.job.Attempt++
			it.probe = ""
			region := it.job.Region
			q.pending[region] = append([]*item{it}, q.pending[region]...)
			q.stats.Requeued++
			q.broadcastLocked(region)
			continue
		}
		if q.byKey[key(it.job)] == it {
			delete(q.byKey, key(it.job))
		}
		q.stats.Expired++
	}
}

// Remove drops every job for a monitor, used when it is paused or deleted.
func (q *Queue) Remove(monitorID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for region, items := range q.pending {
		kept := items[:0]
		for _, it := range items {
			if it.job.MonitorID == monitorID {
				delete(q.byKey, key(it.job))
				continue
			}
			kept = append(kept, it)
		}
		q.pending[region] = kept
	}
}

// Stats returns a snapshot of the counters.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	s := q.stats
	for _, items := range q.pending {
		s.Pending += len(items)
	}
	s.InFlight = len(q.leased)
	return s
}

// Close wakes every waiting Lease call and rejects new jobs.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for region := range q.waiters {
		q.broadcastLocked(region)
	}
}

func (q *Queue) broadcastLocked(region string) {
	if ch, ok := q.waiters[region]; ok {
		close(ch)
		delete(q.waiters, region)
	}
}
