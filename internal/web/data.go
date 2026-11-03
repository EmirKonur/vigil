package web

import (
	"context"
	"math"
	"time"

	"vigil/internal/chart"
	"vigil/internal/config"
	"vigil/internal/sketch"
	"vigil/internal/store"
	"vigil/internal/uptime"
)

// timeRange is a latency chart window. Short windows read raw results;
// long ones read hourly rollups, which is what rollups are for.
type timeRange struct {
	Key   string
	Label string
	Span  time.Duration
	Step  time.Duration
	Raw   bool
}

var ranges = []timeRange{
	{"6h", "6 hours", 6 * time.Hour, 5 * time.Minute, true},
	{"24h", "24 hours", 24 * time.Hour, 15 * time.Minute, true},
	{"7d", "7 days", 7 * 24 * time.Hour, time.Hour, false},
	{"30d", "30 days", 30 * 24 * time.Hour, 6 * time.Hour, false},
}

func findRange(key string) timeRange {
	for _, r := range ranges {
		if r.Key == key {
			return r
		}
	}
	return ranges[1]
}

// regionSlots gives every region a fixed color slot by its position in the
// sorted list of all configured regions, so eu-west is the same color on
// every chart.
func regionSlots(cfg *config.Config) map[string]int {
	out := map[string]int{}
	for i, r := range cfg.Regions {
		out[r] = i
	}
	return out
}

// latencyChart buckets a monitor's latency by region over rng.
func (h *Handler) latencyChart(ctx context.Context, m config.Monitor, rng timeRange, slots map[string]int, now time.Time) (chart.Latency, error) {
	n := int(rng.Span / rng.Step)
	start := now.UTC().Truncate(rng.Step).Add(rng.Step - rng.Span)
	end := start.Add(rng.Span)

	type cell struct {
		sk     *sketch.Sketch
		failed bool
		checks int
	}
	cells := map[string][]cell{}
	for _, r := range m.Policy.Regions {
		cs := make([]cell, n)
		for i := range cs {
			cs[i].sk = sketch.New()
		}
		cells[r] = cs
	}
	idx := func(t time.Time) int { return int(t.Sub(start) / rng.Step) }

	if rng.Raw {
		results, err := h.opts.Store.Results(ctx, m.ID, start, end)
		if err != nil {
			return chart.Latency{}, err
		}
		for _, r := range results {
			cs, ok := cells[r.Region]
			i := idx(r.Slot)
			if !ok || i < 0 || i >= n {
				continue
			}
			cs[i].checks++
			if r.OK {
				cs[i].sk.Add(r.LatencyMS)
			} else {
				cs[i].failed = true
			}
		}
	} else {
		rollups, err := h.opts.Store.Rollups(ctx, store.Hour, []string{m.ID}, start, end)
		if err != nil {
			return chart.Latency{}, err
		}
		for _, r := range rollups {
			cs, ok := cells[r.Region]
			i := idx(r.Bucket)
			if !ok || i < 0 || i >= n {
				continue
			}
			cs[i].checks += int(r.Total)
			cs[i].sk.Merge(r.Latency)
			cs[i].failed = cs[i].failed || r.OK < r.Total
		}
	}

	c := chart.Latency{Start: start, Step: rng.Step, Buckets: n, Label: "Median latency by region, last " + rng.Label}
	for _, region := range m.Policy.Regions {
		s := chart.Series{Name: region, Slot: slots[region], Points: make([]chart.Point, n)}
		for i, cl := range cells[region] {
			s.Points[i] = chart.Point{P50: cl.sk.Quantile(0.5), P95: cl.sk.Quantile(0.95), Failed: cl.failed, Checks: cl.checks}
		}
		c.Series = append(c.Series, s)
	}

	incidents, err := h.opts.Store.IncidentsBetween(ctx, []string{m.ID}, start, end)
	if err != nil {
		return chart.Latency{}, err
	}
	for _, iv := range uptime.Intervals(incidents, now) {
		c.Spans = append(c.Spans, chart.Span{From: iv.From, To: iv.To, Major: iv.Major})
	}
	return c, nil
}

// percentiles merges rollups per key and returns p50/p95 for each.
type percentiles struct {
	P50, P95 float64
	Checks   int64
	OK       int64
}

func mergeRollups(rollups []store.Rollup, key func(store.Rollup) string) map[string]percentiles {
	merged := map[string]*sketch.Sketch{}
	counts := map[string][2]int64{}
	for _, r := range rollups {
		k := key(r)
		if merged[k] == nil {
			merged[k] = sketch.New()
		}
		merged[k].Merge(r.Latency)
		c := counts[k]
		counts[k] = [2]int64{c[0] + r.Total, c[1] + r.OK}
	}
	out := map[string]percentiles{}
	for k, s := range merged {
		out[k] = percentiles{P50: s.Quantile(0.5), P95: s.Quantile(0.95), Checks: counts[k][0], OK: counts[k][1]}
	}
	return out
}

// hourlySpark returns per-hour median latency (all regions merged) and
// whether the hour saw failures, for the last 24 hours.
func hourlySpark(rollups []store.Rollup, now time.Time) ([]float64, []bool) {
	start := now.UTC().Truncate(time.Hour).Add(-23 * time.Hour)
	sk := make([]*sketch.Sketch, 24)
	failed := make([]bool, 24)
	for i := range sk {
		sk[i] = sketch.New()
	}
	for _, r := range rollups {
		i := int(r.Bucket.Sub(start) / time.Hour)
		if i < 0 || i >= 24 {
			continue
		}
		sk[i].Merge(r.Latency)
		failed[i] = failed[i] || r.OK < r.Total
	}
	values := make([]float64, 24)
	for i, s := range sk {
		values[i] = s.Quantile(0.5)
	}
	return values, failed
}

// availability computes incident-based uptime over the last span for one
// set of monitors.
func availability(incidents []store.Incident, since time.Time, span time.Duration, now time.Time) float64 {
	return uptime.Ratio(uptime.Intervals(incidents, now), since, now.Add(-span), now)
}

func byMonitor(incidents []store.Incident) map[string][]store.Incident {
	out := map[string][]store.Incident{}
	for _, inc := range incidents {
		out[inc.MonitorID] = append(out[inc.MonitorID], inc)
	}
	return out
}

func earliest(times ...time.Time) time.Time {
	var out time.Time
	for _, t := range times {
		if !t.IsZero() && (out.IsZero() || t.Before(out)) {
			out = t
		}
	}
	return out
}

func nan() float64 { return math.NaN() }
