package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vigil/internal/check"
	"vigil/internal/config"
	"vigil/internal/probe"
	"vigil/internal/probeapi"
	"vigil/internal/store"
)

// alertLog records every webhook delivery by notifier and event kind.
type alertLog struct {
	mu      sync.Mutex
	events  map[string][]string // notifier -> kinds, in arrival order
	impacts []string            // incident impact in generic payloads, in arrival order
}

func (a *alertLog) handler(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var generic struct {
			Incident struct{ Impact string } `json:"incident"`
		}
		_ = json.NewDecoder(r.Body).Decode(&generic)
		a.mu.Lock()
		a.events[name] = append(a.events[name], r.Header.Get("X-Vigil-Event"))
		if generic.Incident.Impact != "" {
			a.impacts = append(a.impacts, generic.Incident.Impact)
		}
		a.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}
}

func (a *alertLog) count(name, kind string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, k := range a.events[name] {
		if k == kind {
			n++
		}
	}
	return n
}

func (a *alertLog) all(name string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.events[name]...)
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

type testServer struct {
	store   *store.Store
	baseURL string
	stop    func()
}

// startServer runs a full server on a random port with the given config.
func startServer(t *testing.T, yaml string, probeRegions []string) *testServer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "vigil.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() }) // runs after h.stop, registered below
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	opts := Options{
		Listener: ln, Store: st, AdminToken: "admin", ProbeToken: "probe-secret", Version: "test", Log: log,
		LoadConfig: func() (*config.Config, error) {
			return config.Parse([]byte(yaml), func(string) (string, bool) { return "", false })
		},
		TickEvery:   100 * time.Millisecond,
		RollupEvery: time.Hour,
		ExtraProbes: func(baseURL string) []*probe.Probe {
			var out []*probe.Probe
			for _, region := range probeRegions {
				ua := UserAgent("test", region)
				out = append(out, probe.New(probe.Options{
					API: probe.NewClient(baseURL, "probe-secret", ua), ID: region + "-1", Region: region, Version: "test",
					Runner: &check.Runner{UserAgent: ua}, Log: log, PollWait: time.Second,
				}))
			}
			return out
		},
	}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, opts) }()
	h := &testServer{store: st, baseURL: "http://" + ln.Addr().String()}
	var once sync.Once
	h.stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("server returned %v", err)
				}
			case <-time.After(20 * time.Second):
				t.Error("server did not shut down within 20s")
			}
		})
	}
	t.Cleanup(h.stop)
	eventually(t, 5*time.Second, "server to answer /healthz", func() bool {
		resp, err := http.Get(h.baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
		}
		return err == nil && resp.StatusCode == http.StatusOK
	})
	return h
}

// TestIncidentLifecycleAcrossThreeProbes drives a target from up to down to
// up with three probes in three regions, and checks that it produces exactly
// one incident and exactly one alert of each kind per channel.
func TestIncidentLifecycleAcrossThreeProbes(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			http.Error(w, "down for the test", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer target.Close()

	alerts := &alertLog{events: map[string][]string{}}
	hooks := http.NewServeMux()
	hooks.HandleFunc("/ops", alerts.handler("ops"))
	hooks.HandleFunc("/chat", alerts.handler("chat"))
	receiver := httptest.NewServer(hooks)
	defer receiver.Close()

	cfg := fmt.Sprintf(`
defaults:
  interval: 1s
  timeout: 800ms
  regions: [ap-south, eu-west, us-east]
  fail_after: 2
  recover_after: 2
  notify: [ops, chat]
notifiers:
  - {name: ops, type: webhook, preset: generic, url: "%[1]s/ops"}
  - {name: chat, type: webhook, preset: slack, url: "%[1]s/chat"}
monitors:
  - id: api
    http:
      url: %[2]s/health
      expect:
        json: [{path: status, equals: ok}]
`, receiver.URL, target.URL)
	h := startServer(t, cfg, []string{"ap-south", "eu-west", "us-east"})
	ctx := context.Background()

	regionsReporting := func() int {
		latest, _ := h.store.LatestPerRegion(ctx, time.Now().Add(-time.Minute))
		return len(latest["api"])
	}
	eventually(t, 10*time.Second, "all three regions to report", func() bool { return regionsReporting() == 3 })
	if open, _ := h.store.OpenIncidents(ctx); len(open) != 0 {
		t.Fatalf("incident opened while healthy: %+v", open)
	}

	healthy.Store(false)
	eventually(t, 15*time.Second, "an incident to open", func() bool {
		open, _ := h.store.OpenIncidents(ctx)
		return len(open) == 1
	})
	eventually(t, 10*time.Second, "opened alerts on both channels", func() bool {
		return alerts.count("ops", "opened") == 1 && alerts.count("chat", "opened") == 1
	})
	// Stay down for several more rounds: nothing new may be raised.
	time.Sleep(3 * time.Second)

	healthy.Store(true)
	eventually(t, 15*time.Second, "the incident to resolve", func() bool {
		open, _ := h.store.OpenIncidents(ctx)
		return len(open) == 0
	})
	eventually(t, 10*time.Second, "resolved alerts on both channels", func() bool {
		return alerts.count("ops", "resolved") == 1 && alerts.count("chat", "resolved") == 1
	})
	time.Sleep(2 * time.Second)
	h.stop()

	incidents, err := h.store.IncidentsBetween(ctx, nil, time.Now().Add(-time.Hour), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 {
		t.Fatalf("want exactly one incident, got %d", len(incidents))
	}
	inc := incidents[0]
	if inc.Open() || inc.Impact != "major" || len(inc.Regions) != 3 {
		t.Errorf("incident = %+v", inc)
	}
	first, last := inc.Updates[0], inc.Updates[len(inc.Updates)-1]
	if first.Status != "investigating" || last.Status != "resolved" {
		t.Errorf("timeline = %+v", inc.Updates)
	}
	// Every region failed, so the opened alert must say major even though
	// the incident opened when only the first two results were in.
	if len(alerts.impacts) == 0 || alerts.impacts[0] != "major" {
		t.Errorf("opened alert impact = %v, want major", alerts.impacts)
	}
	for _, ch := range []string{"ops", "chat"} {
		got := alerts.all(ch)
		if len(got) != 2 || got[0] != "opened" || got[1] != "resolved" {
			t.Errorf("%s received %v, want exactly [opened resolved]", ch, got)
		}
	}
}

func post(t *testing.T, url, token string, body any) (*http.Response, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.Bytes()
}

// TestProbeAPIDeliveryIsIdempotent speaks the wire protocol directly: a
// result delivered twice is acknowledged as a duplicate and stored once.
func TestProbeAPIDeliveryIsIdempotent(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()
	h := startServer(t, fmt.Sprintf("monitors:\n  - id: web\n    interval: 1s\n    timeout: 500ms\n    regions: [lab]\n    http: {url: %q}\n", target.URL), nil)

	if resp, _ := post(t, h.baseURL+probeapi.LeasePath, "wrong", probeapi.LeaseRequest{ProbeID: "p1", Region: "lab"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token: status %d", resp.StatusCode)
	}

	var lease probeapi.LeaseResponse
	eventually(t, 5*time.Second, "a job to lease", func() bool {
		_, body := post(t, h.baseURL+probeapi.LeasePath, "probe-secret", probeapi.LeaseRequest{ProbeID: "p1", Region: "lab", Max: 1, Wait: time.Second})
		_ = json.Unmarshal(body, &lease)
		return len(lease.Jobs) == 1
	})
	job := lease.Jobs[0]
	if job.MonitorID != "web" || job.TTL <= job.Spec.Timeout {
		t.Fatalf("job = %+v", job)
	}

	report := probeapi.ReportRequest{ProbeID: "p1", Region: "lab", Results: []probeapi.Result{
		{JobID: job.ID, Result: check.Result{OK: true, Latency: 12 * time.Millisecond, StatusCode: 200}},
	}}
	statuses := []string{}
	for range 3 {
		var resp probeapi.ReportResponse
		_, body := post(t, h.baseURL+probeapi.ReportPath, "probe-secret", report)
		if err := json.Unmarshal(body, &resp); err != nil || len(resp.Acks) != 1 {
			t.Fatalf("report response %s: %v", body, err)
		}
		statuses = append(statuses, resp.Acks[0].Status)
	}
	if statuses[0] != probeapi.AckAccepted || statuses[1] != probeapi.AckDuplicate || statuses[2] != probeapi.AckDuplicate {
		t.Fatalf("acks = %v", statuses)
	}

	var resp probeapi.ReportResponse
	_, body := post(t, h.baseURL+probeapi.ReportPath, "probe-secret", probeapi.ReportRequest{ProbeID: "p1", Region: "lab",
		Results: []probeapi.Result{{JobID: "never-issued"}}})
	_ = json.Unmarshal(body, &resp)
	if resp.Acks[0].Status != probeapi.AckUnknown {
		t.Fatalf("unknown job ack = %v", resp.Acks)
	}

	results, _ := h.store.Results(context.Background(), "web", time.Now().Add(-time.Minute), time.Now().Add(time.Minute))
	if len(results) != 1 {
		t.Fatalf("stored %d results, want 1", len(results))
	}
}

func TestDashboardRequiresLogin(t *testing.T) {
	h := startServer(t, "", nil)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get(h.baseURL + "/admin")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/admin/login?next=%2Fadmin" {
		t.Fatalf("status %d location %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	req, _ := http.NewRequest(http.MethodGet, h.baseURL+"/admin", nil)
	req.Header.Set("Authorization", "Bearer admin")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("with token: status %d", resp.StatusCode)
	}
	if csp := resp.Header.Get("Content-Security-Policy"); csp == "" {
		t.Fatal("missing CSP header")
	}
}
