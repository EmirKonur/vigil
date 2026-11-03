package engine

import (
	"context"
	"sync"
	"time"

	"vigil/internal/store"
)

// probeRegistry throttles heartbeat writes: a busy probe leases many times a
// second, and the dashboard only needs last-seen to the nearest few seconds.
type probeRegistry struct {
	mu        sync.Mutex
	lastWrite map[string]time.Time
	completed map[string]int64
}

const heartbeatEvery = 5 * time.Second

func (r *probeRegistry) addCompleted(id string, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.completed == nil {
		r.completed = map[string]int64{}
	}
	r.completed[id] += int64(n)
}

func (e *Engine) heartbeat(ctx context.Context, p store.Probe) {
	e.probes.mu.Lock()
	last, seen := e.probes.lastWrite[p.ID]
	if seen && p.LastSeen.Sub(last) < heartbeatEvery {
		e.probes.mu.Unlock()
		return
	}
	e.probes.lastWrite[p.ID] = p.LastSeen
	completed := e.probes.completed[p.ID]
	delete(e.probes.completed, p.ID)
	e.probes.mu.Unlock()

	if err := e.store.UpsertProbe(ctx, p, completed); err != nil {
		if ctx.Err() == nil {
			e.log.Error("recording probe heartbeat", "probe", p.ID, "err", err)
		}
		return
	}
	e.metrics.ProbeLastSeen.WithLabelValues(p.ID, p.Region).Set(float64(p.LastSeen.Unix()))
	if !seen {
		e.log.Info("probe connected", "probe", p.ID, "region", p.Region, "version", p.Version, "remote", p.RemoteAddr)
	}
	e.opts.OnChange(TopicProbes)
}
