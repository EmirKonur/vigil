// Package demo seeds synthetic history and runs local targets so that a
// fresh `vigil demo` looks like an installation that has been running for
// a quarter. Everything it writes is synthetic and the UI says so.
package demo

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"vigil/internal/alert"
	"vigil/internal/check"
	"vigil/internal/config"
	"vigil/internal/consensus"
	"vigil/internal/engine"
	"vigil/internal/lease"
	"vigil/internal/scheduler"
	"vigil/internal/sketch"
	"vigil/internal/store"
)

//go:embed vigil.demo.yaml
var demoConfig []byte

// Config returns the demo configuration pointed at the running targets.
func Config(t *Targets) (*config.Config, error) {
	env := map[string]string{"DEMO_HTTP": t.HTTP, "DEMO_TCP": t.TCP, "DEMO_DNS": t.DNS, "DEMO_TLS": t.TLS}
	return config.Parse(demoConfig, func(k string) (string, bool) { v, ok := env[k]; return v, ok })
}

// SearchOutage is how long the live search incident lasts after startup.
const SearchOutage = 12 * time.Minute

type update struct {
	after  time.Duration
	status string
	body   string
}

// scripted is one incident in the synthetic history.
type scripted struct {
	monitor  string
	ago      time.Duration // opened this long before now
	length   time.Duration // zero while still open
	impact   consensus.Impact
	regions  []string
	cause    string
	flapping bool
	updates  []update // operator updates; Vigil's own are added automatically
}

var history = []scripted{
	{monitor: "search", ago: 25 * time.Minute, impact: consensus.ImpactPartial, regions: []string{"ap-south", "us-east"}, cause: "timeout after 5s",
		updates: []update{
			{7 * time.Minute, "identified", "The search cluster serving us-east and ap-south is rejecting queries after a failed index rebuild. Traffic is moving to eu-west."},
			{19 * time.Minute, "update", "Failover is complete for ap-south. us-east is still draining; expect slow results there for about 15 more minutes."},
		}},
	{monitor: "checkout", ago: 2*time.Hour + 50*time.Minute, length: 18 * time.Minute, impact: consensus.ImpactPartial, regions: []string{"eu-west", "us-east"}, cause: "status 502, want 2xx",
		updates: []update{
			{5 * time.Minute, "identified", "A release of the payment provider client raised connection errors. Rolling back to the previous build."},
			{12 * time.Minute, "monitoring", "The rollback is out and error rates are back to normal. Watching for another 15 minutes."},
		}},
	{monitor: "api", ago: 6*24*time.Hour + 5*time.Hour, length: 9 * time.Minute, impact: consensus.ImpactPartial, regions: []string{"eu-west", "us-east"}, cause: "status 503, want 2xx"},
	{monitor: "search", ago: 12*24*time.Hour + 3*time.Hour, length: 35 * time.Minute, impact: consensus.ImpactPartial, regions: []string{"ap-south", "eu-west"}, cause: "timeout after 5s", flapping: true},
	{monitor: "website", ago: 19*24*time.Hour + 9*time.Hour, length: 42 * time.Minute, impact: consensus.ImpactMajor, regions: []string{"ap-south", "eu-west", "us-east"}, cause: "timeout after 5s",
		updates: []update{{6 * time.Minute, "identified", "A CDN configuration change dropped the origin pool. Reverting the change."}}},
	{monitor: "webhooks", ago: 33*24*time.Hour + 14*time.Hour, length: 2*time.Hour + 10*time.Minute, impact: consensus.ImpactPartial, regions: []string{"ap-south", "us-east"}, cause: "status 500, want 2xx",
		updates: []update{
			{20 * time.Minute, "identified", "Delivery workers stalled after a message broker failover. Restarting them and replaying the backlog."},
			{95 * time.Minute, "monitoring", "The backlog is drained. Deliveries are flowing at normal latency."},
		}},
	{monitor: "postgres", ago: 47*24*time.Hour + 2*time.Hour, length: 4 * time.Minute, impact: consensus.ImpactMajor, regions: []string{"ap-south", "eu-west", "us-east"}, cause: "connection refused",
		updates: []update{{2 * time.Minute, "identified", "A planned primary failover ran past its maintenance window."}}},
	{monitor: "dns", ago: 54*24*time.Hour + 20*time.Hour, length: 11 * time.Minute, impact: consensus.ImpactPartial, regions: []string{"ap-south", "us-east"}, cause: "dns: no such host fernhill.test"},
	{monitor: "checkout", ago: 61*24*time.Hour + 7*time.Hour, length: 27 * time.Minute, impact: consensus.ImpactPartial, regions: []string{"ap-south", "eu-west"}, cause: "status 502, want 2xx"},
	{monitor: "api", ago: 78*24*time.Hour + 16*time.Hour, length: time.Hour + 5*time.Minute, impact: consensus.ImpactMajor, regions: []string{"ap-south", "eu-west", "us-east"}, cause: "timeout after 5s",
		updates: []update{{15 * time.Minute, "identified", "The API gateway's certificate store failed to reload after a rotation. Restarting the gateway fleet."}}},
}

// monitorCost scales region latency per monitor so each one has its own
// profile; checks that do less work are faster.
func monitorCost(m config.Monitor) float64 {
	switch m.Spec.Type {
	case check.TypeTCP:
		return 0.35
	case check.TypeDNS:
		return 0.25
	case check.TypeTLS:
		return 0.9
	}
	return map[string]float64{"website": 1.0, "api": 0.8, "checkout": 1.6, "search": 2.1, "webhooks": 0.9}[m.ID]
}

// Stats summarizes what Seed wrote.
type Stats struct {
	Incidents, Results, Rollups int
}

// Options tunes how much history Seed writes.
type Options struct {
	// RawWindow is how much recent history is written as individual check
	// results (the rest is hourly rollups). Defaults to 24 hours, which
	// fills the dashboard's raw 6h and 24h chart ranges.
	RawWindow time.Duration
}

// Seed writes 90 days of synthetic history ending at now.
func Seed(ctx context.Context, st *store.Store, cfg *config.Config, now time.Time, opts Options) (Stats, error) {
	if opts.RawWindow <= 0 {
		opts.RawWindow = 24 * time.Hour
	}
	now = now.UTC().Truncate(time.Second)
	rng := rand.New(rand.NewPCG(2026, 11))
	var stats Stats

	rows := make([]store.Monitor, len(cfg.Monitors))
	for i, m := range cfg.Monitors {
		rows[i] = store.Monitor{ID: m.ID, Name: m.Name, Type: string(m.Spec.Type), Target: m.Spec.Target(), Interval: m.Interval, Regions: m.Policy.Regions}
	}
	if err := st.SyncMonitors(ctx, rows, now); err != nil {
		return stats, err
	}
	created := now.AddDate(0, 0, -92)
	for _, m := range cfg.Monitors {
		if err := st.SetMonitorCreated(ctx, m.ID, created); err != nil {
			return stats, err
		}
	}

	if err := seedIncidents(ctx, st, cfg, now, &stats); err != nil {
		return stats, err
	}
	rawStart := now.Add(-opts.RawWindow).Truncate(time.Hour)
	if err := seedRollups(ctx, st, cfg, created, rawStart, now, rng, &stats); err != nil {
		return stats, err
	}
	if err := seedRaw(ctx, st, cfg, rawStart, now, rng, &stats); err != nil {
		return stats, err
	}
	if _, err := st.RollupHours(ctx, rawStart, now.Add(time.Hour)); err != nil {
		return stats, err
	}
	if _, err := st.RollupDays(ctx, startOfDay(now.AddDate(0, 0, -30)), now.Add(time.Hour)); err != nil {
		return stats, err
	}
	if err := st.SetMeta(ctx, "rollup.hourly.watermark", now.Add(-2*time.Hour).Truncate(time.Hour).Format(time.RFC3339)); err != nil {
		return stats, err
	}
	if err := st.SetMeta(ctx, "demo.seeded_at", now.Format(time.RFC3339)); err != nil {
		return stats, err
	}
	return stats, seedStates(ctx, st, cfg, now)
}

func (s scripted) window(now time.Time) (time.Time, time.Time) {
	opened := now.Add(-s.ago)
	if s.length == 0 {
		return opened, now.Add(time.Hour)
	}
	return opened, opened.Add(s.length)
}

// failing reports whether a scripted incident has region failing at t, and
// with which error.
func failing(monitor, region string, t, now time.Time) (bool, string) {
	for _, s := range history {
		if s.monitor != monitor || !slices.Contains(s.regions, region) {
			continue
		}
		// Checks start failing one detection window before the incident
		// opens: consensus needs consecutive failures first.
		from, to := s.window(now)
		if !t.Before(from.Add(-45*time.Second)) && t.Before(to.Add(-20*time.Second)) {
			return true, s.cause
		}
	}
	return false, ""
}

// stress is a latency multiplier that rises in the minutes before an
// incident in the affected regions, the way saturation usually shows up.
func stress(monitor, region string, t, now time.Time) float64 {
	f := 1.0
	for _, s := range history {
		if s.monitor != monitor || !slices.Contains(s.regions, region) {
			continue
		}
		from, to := s.window(now)
		lead := from.Sub(t)
		switch {
		case lead > 0 && lead < 12*time.Minute:
			f = math.Max(f, 1+2.2*(1-lead.Minutes()/12))
		case !t.Before(from) && t.Before(to.Add(6*time.Minute)):
			f = math.Max(f, 2.6)
		}
	}
	return f
}

// diurnal adds a daily traffic curve peaking mid-afternoon UTC.
func diurnal(t time.Time) float64 {
	h := float64(t.Hour()) + float64(t.Minute())/60
	return 1 + 0.16*math.Sin(2*math.Pi*(h-9)/24)
}

func latency(rng *rand.Rand, m config.Monitor, region string, t, now time.Time) float64 {
	base := RegionLatency[region] * monitorCost(m) * diurnal(t) * stress(m.ID, region, t, now)
	return base * math.Exp(rng.NormFloat64()*0.2)
}

// seedRollups writes hourly rollups for the 30 days before the raw window
// and daily rollups before that. Nothing reads hourly data older than the
// 30-day chart, and writing a quarter of hourly rows makes seeding slow.
func seedRollups(ctx context.Context, st *store.Store, cfg *config.Config, from, to, now time.Time, rng *rand.Rand, stats *Stats) error {
	hourlyFrom := startOfDay(to.AddDate(0, 0, -30))
	return st.Tx(ctx, func(tx *store.Tx) error {
		for _, m := range cfg.Monitors {
			perHour := int(time.Hour / m.Interval)
			for _, region := range m.Policy.Regions {
				var day *store.Rollup
				for h := from.Truncate(time.Hour); h.Before(to); h = h.Add(time.Hour) {
					r := store.Rollup{Resolution: store.Hour, MonitorID: m.ID, Region: region, Bucket: h, Total: int64(perHour), Latency: sketch.New()}
					// Sample the hour at a handful of points, weighting each
					// sample so the sketch counts every check in the hour.
					const samples = 12
					for i := range samples {
						t := h.Add(time.Duration(i) * time.Hour / samples)
						weight := uint64(perHour / samples)
						if i < perHour%samples {
							weight++
						}
						if bad, _ := failing(m.ID, region, t, now); bad || weight == 0 {
							continue
						}
						r.Latency.AddN(latency(rng, m, region, t, now), weight)
					}
					// Background noise: the odd sub-threshold blip that a
					// single-probe monitor would have paged someone for.
					blips := int64(0)
					if rng.Float64() < 0.01 {
						blips = 1 + rng.Int64N(2)
					}
					r.OK = max(int64(r.Latency.Count())-blips, 0)

					if !h.Before(hourlyFrom) {
						if err := tx.UpsertRollup(ctx, r); err != nil {
							return err
						}
						stats.Rollups++
						continue
					}
					if day == nil || !day.Bucket.Equal(startOfDay(h)) {
						if err := flushDay(ctx, tx, day, stats); err != nil {
							return err
						}
						day = &store.Rollup{Resolution: store.Day, MonitorID: m.ID, Region: region, Bucket: startOfDay(h), Latency: sketch.New()}
					}
					day.Total += r.Total
					day.OK += r.OK
					day.Latency.Merge(r.Latency)
				}
				if err := flushDay(ctx, tx, day, stats); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func flushDay(ctx context.Context, tx *store.Tx, day *store.Rollup, stats *Stats) error {
	if day == nil {
		return nil
	}
	stats.Rollups++
	return tx.UpsertRollup(ctx, *day)
}

func startOfDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func seedRaw(ctx context.Context, st *store.Store, cfg *config.Config, from, now time.Time, rng *rand.Rand, stats *Stats) error {
	return st.Tx(ctx, func(tx *store.Tx) error {
		for _, m := range cfg.Monitors {
			for slot := scheduler.NextSlot(m.ID, m.Interval, from); slot.Before(now); slot = slot.Add(m.Interval) {
				for _, region := range m.Policy.Regions {
					r := store.Result{
						ID: lease.NewID(), MonitorID: m.ID, Region: region, ProbeID: region + "-1",
						Slot: slot, OK: true, LatencyMS: math.Round(latency(rng, m, region, slot, now)*10) / 10,
					}
					if m.Spec.Type == check.TypeHTTP {
						r.StatusCode = 200
					}
					if bad, cause := failing(m.ID, region, slot, now); bad {
						r.OK, r.Error, r.LatencyMS = false, cause, 5000
						r.StatusCode = statusFor(cause)
					} else if rng.Float64() < 0.0001 {
						r.OK, r.Error, r.LatencyMS, r.StatusCode = false, "timeout after 5s", 5000, 0
					}
					r.ReceivedAt = slot.Add(time.Duration(r.LatencyMS+40) * time.Millisecond)
					if _, err := tx.InsertResult(ctx, r); err != nil {
						return err
					}
					stats.Results++
				}
			}
		}
		return nil
	})
}

func statusFor(cause string) int {
	var code int
	if _, err := fmt.Sscanf(cause, "status %d", &code); err == nil {
		return code
	}
	return 0
}

func seedIncidents(ctx context.Context, st *store.Store, cfg *config.Config, now time.Time, stats *Stats) error {
	return st.Tx(ctx, func(tx *store.Tx) error {
		for _, s := range history {
			m, ok := cfg.Monitor(s.monitor)
			if !ok {
				continue
			}
			opened, resolved := s.window(now)
			id, err := tx.OpenIncident(ctx, store.Incident{
				MonitorID: m.ID, OpenedAt: opened, Impact: string(s.impact), Regions: s.regions,
				Voting: len(m.Policy.Regions), Cause: s.cause, Flapping: s.flapping,
			})
			if err != nil {
				return err
			}
			updates := []store.Update{{At: opened, Status: "investigating", Author: store.AuthorVigil,
				Body: fmt.Sprintf("Checks are failing from %d of %d regions: %s. Last error: %s.", len(s.regions), len(m.Policy.Regions), strings.Join(s.regions, ", "), s.cause)}}
			for _, u := range s.updates {
				updates = append(updates, store.Update{At: opened.Add(u.after), Status: u.status, Body: u.body, Author: store.AuthorOperator})
			}
			if s.flapping {
				updates = append(updates, store.Update{At: opened.Add(9 * time.Minute), Status: "update", Author: store.AuthorVigil,
					Body: "The check result is flapping. Vigil is keeping this incident open and holding alerts until results are stable for 30m."})
			}
			if s.length > 0 {
				if err := tx.ResolveIncident(ctx, id, resolved); err != nil {
					return err
				}
				updates = append(updates, store.Update{At: resolved, Status: "resolved", Author: store.AuthorVigil,
					Body: fmt.Sprintf("Checks have passed %d times in a row from enough regions. Resolved automatically after %s.", m.Policy.RecoverAfter, alert.HumanDuration(s.length))})
			}
			for _, u := range updates {
				u.IncidentID = id
				if _, err := tx.AddUpdate(ctx, u); err != nil {
					return err
				}
			}
			kinds := []consensus.Alert{consensus.AlertOpened}
			if s.flapping {
				kinds = []consensus.Alert{consensus.AlertOpened, consensus.AlertFlapping}
			}
			if s.length > 0 {
				kinds = append(kinds, consensus.AlertResolved)
			}
			for _, k := range kinds {
				at := opened
				if k == consensus.AlertResolved {
					at = resolved
				}
				msg, _ := json.Marshal(alert.Message{Kind: alert.Kind(k), At: at, Monitor: alert.Monitor{ID: m.ID, Name: m.Name, Target: m.Spec.Target()}, Demo: true})
				for _, n := range m.Notify {
					if _, err := tx.EnqueueNotification(ctx, store.Notification{
						Notifier: n, DedupKey: engine.DedupKey(m.ID, opened, k, 0), IncidentID: id, MonitorID: m.ID,
						Kind: string(k), Message: msg, CreatedAt: at,
					}); err != nil {
						return err
					}
				}
			}
			stats.Incidents++
		}
		// History is history: mark every seeded alert delivered.
		_, err := tx.ExecContext(ctx, `UPDATE notifications SET status = 'sent', attempts = 1, sent_at = created_at + 400`)
		return err
	})
}

// seedStates writes consensus state matching the end of the synthetic
// history, so the live engine continues from it instead of starting cold.
func seedStates(ctx context.Context, st *store.Store, cfg *config.Config, now time.Time) error {
	return st.Tx(ctx, func(tx *store.Tx) error {
		for _, m := range cfg.Monitors {
			state := consensus.New()
			state.Phase = consensus.PhaseUp
			last := scheduler.NextSlot(m.ID, m.Interval, now.Add(-m.Interval))
			if !last.Before(now) {
				last = last.Add(-m.Interval)
			}
			for _, region := range m.Policy.Regions {
				rs := &consensus.RegionState{LastSlot: last, LastSeen: now, ConsecutiveOK: 50}
				if bad, cause := failing(m.ID, region, last, now); bad {
					rs.Failing, rs.ConsecutiveOK, rs.ConsecutiveFail, rs.LastError = true, 0, 100, cause
				}
				state.Regions[region] = rs
			}
			for _, s := range history {
				if s.monitor == m.ID && s.length == 0 {
					opened := now.Add(-s.ago)
					state.Phase, state.Unhealthy = consensus.PhaseDown, true
					state.Flips = []time.Time{opened}
					state.Incident = &consensus.Incident{OpenedAt: opened, Impact: s.impact, Regions: s.regions, LastNotifiedAt: opened}
				}
			}
			if err := tx.SaveState(ctx, m.ID, state, now); err != nil {
				return err
			}
		}
		return nil
	})
}
