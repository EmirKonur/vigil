package engine

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"vigil/internal/check"
	"vigil/internal/clock"
	"vigil/internal/config"
	"vigil/internal/probeapi"
	"vigil/internal/store"
)

const testConfig = `
defaults:
  interval: 30s
  timeout: 5s
  regions: [eu, us]
  fail_after: 1
  recover_after: 1
  notify: [team]
notifiers:
  - {name: team, type: webhook, url: "http://hooks.invalid/team"}
  - {name: pager, type: webhook, url: "http://hooks.invalid/pager"}
monitors:
  - id: edge
    tls: {address: "edge.invalid:443"}
    escalate: {after: 10m, notify: [pager]}
`

type fixture struct {
	t     *testing.T
	ctx   context.Context
	clk   *clock.Fake
	store *store.Store
	eng   *Engine
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "e.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	clk := clock.NewFake(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	eng, err := New(ctx, Options{Store: st, Clock: clk, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(testConfig), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Configure(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, ctx: ctx, clk: clk, store: st, eng: eng}
}

// round dispatches one slot and answers every region's job with res.
func (f *fixture) round(res check.Result) {
	f.t.Helper()
	f.clk.Advance(30 * time.Second)
	f.eng.dispatch("edge", f.clk.Now())
	for _, region := range []string{"eu", "us"} {
		lease, err := f.eng.Lease(f.ctx, probeapi.LeaseRequest{ProbeID: region + "-1", Region: region, Max: 5}, "", true)
		if err != nil {
			f.t.Fatal(err)
		}
		for _, j := range lease.Jobs {
			if _, err := f.eng.Report(f.ctx, probeapi.ReportRequest{ProbeID: region + "-1", Region: region,
				Results: []probeapi.Result{{JobID: j.ID, Result: res}}}); err != nil {
				f.t.Fatal(err)
			}
		}
	}
}

func (f *fixture) outbox() map[string][]string {
	f.t.Helper()
	rows, err := f.store.RecentNotifications(f.ctx, "", 100)
	if err != nil {
		f.t.Fatal(err)
	}
	out := map[string][]string{}
	for i := len(rows) - 1; i >= 0; i-- {
		out[rows[i].Notifier] = append(out[rows[i].Notifier], rows[i].Kind)
	}
	return out
}

func TestCertificateWarningAlertsOncePerCertificate(t *testing.T) {
	f := newFixture(t)
	expiry := f.clk.Now().Add(9 * 24 * time.Hour)
	warn := check.Result{OK: true, Latency: 40 * time.Millisecond, Warning: "certificate expires in 9 days", CertNotAfter: expiry}
	for range 5 {
		f.round(warn)
	}
	if got := f.outbox()["team"]; len(got) != 1 || got[0] != "cert_expiring" {
		t.Fatalf("after five warnings: %v", got)
	}
	// A renewed certificate that is again close to expiry is a new alert.
	warn.CertNotAfter = expiry.Add(30 * 24 * time.Hour)
	f.round(warn)
	if got := f.outbox()["team"]; len(got) != 2 {
		t.Fatalf("after renewal: %v", got)
	}
}

func TestEscalationReachesPagerAndResolutionFollows(t *testing.T) {
	f := newFixture(t)
	f.round(check.Result{OK: true})
	f.round(check.Result{Error: "tls: certificate expired 2026-09-30"})
	f.clk.Advance(11 * time.Minute)
	f.eng.tick(f.ctx)
	f.round(check.Result{OK: true})

	box := f.outbox()
	if got := box["team"]; len(got) != 2 || got[0] != "opened" || got[1] != "resolved" {
		t.Fatalf("team got %v", got)
	}
	// The pager hears about the escalation and then the resolution, never
	// the original page that went to the team.
	if got := box["pager"]; len(got) != 2 || got[0] != "escalated" || got[1] != "resolved" {
		t.Fatalf("pager got %v", got)
	}
}

func TestPausedMonitorIgnoresResults(t *testing.T) {
	f := newFixture(t)
	f.round(check.Result{OK: true})
	// A job leased before the pause reports after it.
	f.clk.Advance(30 * time.Second)
	f.eng.dispatch("edge", f.clk.Now())
	lease, _ := f.eng.Lease(f.ctx, probeapi.LeaseRequest{ProbeID: "eu-1", Region: "eu", Max: 5}, "", true)
	if err := f.eng.SetPaused(f.ctx, "edge", true); err != nil {
		t.Fatal(err)
	}
	for _, j := range lease.Jobs {
		_, _ = f.eng.Report(f.ctx, probeapi.ReportRequest{ProbeID: "eu-1", Region: "eu", Results: []probeapi.Result{{JobID: j.ID, Result: check.Result{Error: "boom"}}}})
	}
	f.eng.dispatch("edge", f.clk.Now().Add(30*time.Second))
	if stats := f.eng.QueueStats(); stats.Pending != 0 {
		t.Fatalf("paused monitor still enqueues jobs: %+v", stats)
	}
	if s, _ := f.eng.Status("edge"); s.State.Regions["eu"].ConsecutiveFail != 0 || !s.Paused {
		t.Fatalf("paused monitor applied a result: %+v", s.State.Regions["eu"])
	}
	if open, _ := f.store.OpenIncidents(f.ctx); len(open) != 0 {
		t.Fatal("incident opened while paused")
	}
}

// BenchmarkIngest measures one result through the whole write path: lease
// completion, idempotent insert, state transition, and state save, each in
// its own SQLite transaction.
func BenchmarkIngest(b *testing.B) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	clk := clock.NewFake(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	eng, _ := New(ctx, Options{Store: st, Clock: clk, Log: slog.New(slog.DiscardHandler)})
	cfg, _ := config.Parse([]byte(testConfig), func(string) (string, bool) { return "", false })
	_ = eng.Configure(ctx, cfg)
	b.ResetTimer()
	for i := range b.N {
		clk.Advance(30 * time.Second)
		eng.dispatch("edge", clk.Now())
		lease, _ := eng.Lease(ctx, probeapi.LeaseRequest{ProbeID: "eu-1", Region: "eu", Max: 1}, "", true)
		for _, j := range lease.Jobs {
			_, _ = eng.Report(ctx, probeapi.ReportRequest{ProbeID: "eu-1", Region: "eu",
				Results: []probeapi.Result{{JobID: j.ID, Result: check.Result{OK: i%50 != 0, Latency: 40 * time.Millisecond}}}})
		}
	}
}
