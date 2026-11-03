package web

import (
	"context"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"vigil/internal/clock"
	"vigil/internal/demo"
	"vigil/internal/engine"
	"vigil/internal/store"
)

var (
	siteOnce sync.Once
	site     *httptest.Server
	siteErr  error
)

// TestMain cleans up the shared demo site.
func TestMain(m *testing.M) {
	code := m.Run()
	if site != nil {
		site.Close()
	}
	os.Exit(code)
}

// newDemoSite renders the UI over the seeded demo history, shared by every
// test in the package because seeding 90 days takes a few seconds (far more
// under -race). The demo targets are never contacted; nothing runs checks.
func newDemoSite(t *testing.T) *httptest.Server {
	t.Helper()
	siteOnce.Do(func() { site, siteErr = buildDemoSite() })
	if siteErr != nil {
		t.Fatal(siteErr)
	}
	return site
}

func buildDemoSite() (*httptest.Server, error) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "vigil-web-test")
	if err != nil {
		return nil, err
	}
	st, err := store.Open(ctx, filepath.Join(dir, "demo.db"))
	if err != nil {
		return nil, err
	}
	cfg, err := demo.Config(&demo.Targets{HTTP: "http://192.0.2.1", TCP: "192.0.2.1:5432", DNS: "192.0.2.1:53", TLS: "192.0.2.1:443"})
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if _, err := demo.Seed(ctx, st, cfg, now, demo.Options{RawWindow: 3 * time.Hour}); err != nil {
		return nil, err
	}
	log := slog.New(slog.DiscardHandler)
	eng, err := engine.New(ctx, engine.Options{Store: st, Clock: clock.Real{}, Log: log})
	if err != nil {
		return nil, err
	}
	if err := eng.Configure(ctx, cfg); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	New(Options{
		Engine: eng, Store: st, Clock: clock.Real{}, Log: log, Hub: NewHub(time.Second), AdminToken: "secret",
		Version: "test", Demo: true,
		Reload: func(context.Context) error { return nil },
		Config: func() ConfigStatus { return ConfigStatus{Path: "demo", LoadedAt: now} },
	}).Register(mux)
	srv := httptest.NewServer(mux)
	// The store and temp dir live as long as the test binary.
	return srv, nil
}

func get(t *testing.T, srv *httptest.Server, path string, admin bool) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if admin {
		req.Header.Set("Authorization", "Bearer secret")
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestStatusPageShowsSeededState(t *testing.T) {
	srv := newDemoSite(t)
	code, body := get(t, srv, "/status/fernhill", false)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{
		"Partial outage", "Affected: Search", // banner from the open search incident
		"Search: partial outage",            // active incident card
		"Failover is complete for ap-south", // operator update
		`class="bar major"`, `class="bars"`, // 90-day bars with a major day
		"Response time by region", "ap-south", "eu-west", "us-east", // latency table
		"Checkout: partial outage", // resolved incident from today in past incidents
		"Demo data",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("status page missing %q", want)
		}
	}
	if n := strings.Count(body, `<rect class="bar `); n != 6*90 {
		t.Errorf("%d uptime bars, want 90 per component", n)
	}
}

func TestFeedIsValidAtom(t *testing.T) {
	srv := newDemoSite(t)
	code, body := get(t, srv, "/status/fernhill/feed.atom", false)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	var feed struct {
		XMLName xml.Name `xml:"http://www.w3.org/2005/Atom feed"`
		Entries []struct {
			Title   string `xml:"title"`
			ID      string `xml:"id"`
			Updated string `xml:"updated"`
		} `xml:"entry"`
	}
	if err := xml.Unmarshal([]byte(body), &feed); err != nil {
		t.Fatal(err)
	}
	// Ten seeded incidents; the DNS monitor is not on this page.
	if len(feed.Entries) != 9 {
		t.Fatalf("%d entries", len(feed.Entries))
	}
	for _, e := range feed.Entries {
		if _, err := time.Parse(time.RFC3339, e.Updated); err != nil || e.ID == "" {
			t.Fatalf("bad entry %+v", e)
		}
	}
}

func TestAdminPagesRender(t *testing.T) {
	srv := newDemoSite(t)
	for path, want := range map[string]string{
		"/admin":                            "Open incidents",
		"/admin/monitors/checkout?range=6h": `class="inc"`,
		"/admin/monitors/api?range=30d":     `class="line s0"`,
		"/admin/incidents":                  "Resolved",
		"/admin/incidents/1":                "Post an update",
		"/admin/probes":                     "Probe health",
	} {
		code, body := get(t, srv, path, true)
		if code != http.StatusOK || !strings.Contains(body, want) {
			t.Errorf("%s: status %d, contains %q = %v", path, code, want, strings.Contains(body, want))
		}
	}
}

func TestAdminRequiresAuth(t *testing.T) {
	srv := newDemoSite(t)
	code, _ := get(t, srv, "/admin/monitors/api", false)
	if code != http.StatusSeeOther {
		t.Fatalf("unauthenticated dashboard: status %d", code)
	}
	resp, err := http.PostForm(srv.URL+"/admin/login", url.Values{"token": {"wrong"}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token: status %d", resp.StatusCode)
	}
}

func TestOperatorUpdateAppearsOnStatusPage(t *testing.T) {
	srv := newDemoSite(t)
	form := url.Values{"status": {"monitoring"}, "body": {"Index rebuilt; watching error rates."}}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/admin/incidents/1/updates", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := (&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("post update: status %d", resp.StatusCode)
	}
	_, body := get(t, srv, "/status/fernhill", false)
	if !strings.Contains(body, "Index rebuilt; watching error rates.") {
		t.Fatal("operator update not shown publicly")
	}
}

func TestUnknownRoutesGetTheDesigned404(t *testing.T) {
	srv := newDemoSite(t)
	for _, path := range []string{"/nope", "/status/missing", "/status/fernhill/incidents/999"} {
		code, body := get(t, srv, path, false)
		if code != http.StatusNotFound || !strings.Contains(body, "No signal") {
			t.Errorf("%s: status %d", path, code)
		}
	}
}
