package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vigil/internal/check"
)

func env(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
}

const sample = `
defaults:
  interval: 30s
  regions: [eu-west, us-east, ap-south]
  notify: [ops]
  renotify: 1h

notifiers:
  - name: ops
    type: webhook
    preset: slack
    url: ${SLACK_URL}
  - name: oncall
    type: email
    to: [oncall@example.com]
    smtp:
      host: ${SMTP_HOST:-localhost}
      port: 1025
      tls: none
      from: vigil@example.com

monitors:
  - id: api
    name: Public API
    http:
      url: https://api.example.com/health
      expect:
        status: [200]
        json:
          - path: status
            equals: ok
    escalate:
      after: 15m
      notify: [oncall]
  - id: db
    interval: 10s
    regions: [eu-west]
    tcp:
      address: db.internal:5432
  - id: apex
    dns:
      name: example.com
      resolver: 1.1.1.1
  - id: cert
    interval: 1h
    quorum: all
    tls:
      address: example.com

status_pages:
  - slug: public
    title: Example Status
    components:
      - name: API
        monitors: [api]

retention:
  raw: 14d
`

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(sample), env(map[string]string{"SLACK_URL": "https://hooks.slack.test/T1"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Notifiers["ops"].URL; got != "https://hooks.slack.test/T1" {
		t.Errorf("env expansion: url = %q", got)
	}
	if got := cfg.Notifiers["oncall"].SMTP.Host; got != "localhost" {
		t.Errorf("env default: smtp host = %q", got)
	}

	api, _ := cfg.Monitor("api")
	if api.Interval != 30*time.Second || api.Spec.Timeout != 10*time.Second {
		t.Errorf("api interval/timeout = %s/%s", api.Interval, api.Spec.Timeout)
	}
	if api.Policy.Quorum != 2 || len(api.Policy.Regions) != 3 {
		t.Errorf("majority of 3 regions should be 2, got %d of %v", api.Policy.Quorum, api.Policy.Regions)
	}
	if api.Policy.EscalateAfter != 15*time.Minute || api.Escalate[0] != "oncall" {
		t.Errorf("escalation not applied: %+v", api.Policy)
	}
	if api.Policy.Renotify != time.Hour || api.Notify[0] != "ops" {
		t.Errorf("defaults not inherited: renotify=%s notify=%v", api.Policy.Renotify, api.Notify)
	}
	if api.Spec.HTTP.Method != "GET" || *api.Spec.HTTP.Expect.JSON[0].Equals != "ok" {
		t.Errorf("http spec: %+v", api.Spec.HTTP)
	}

	db, _ := cfg.Monitor("db")
	if db.Policy.Quorum != 1 || db.Spec.Type != check.TypeTCP || db.Spec.Timeout != 10*time.Second {
		t.Errorf("db: quorum=%d type=%s timeout=%s", db.Policy.Quorum, db.Spec.Type, db.Spec.Timeout)
	}
	if db.Policy.StaleAfter != 40*time.Second {
		t.Errorf("db stale after = %s", db.Policy.StaleAfter)
	}

	apex, _ := cfg.Monitor("apex")
	if apex.Spec.DNS.Type != "A" || apex.Spec.DNS.Resolver != "1.1.1.1:53" {
		t.Errorf("dns defaults: %+v", apex.Spec.DNS)
	}
	cert, _ := cfg.Monitor("cert")
	if cert.Spec.TLS.Address != "example.com:443" || cert.Policy.Quorum != 3 || cert.Spec.TLS.WarnDays != 14 {
		t.Errorf("tls defaults: %+v quorum=%d", cert.Spec.TLS, cert.Policy.Quorum)
	}

	if time.Duration(cfg.Retention.Raw) != 14*24*time.Hour || time.Duration(cfg.Retention.Daily) != 400*24*time.Hour {
		t.Errorf("retention = %+v", cfg.Retention)
	}
	if strings.Join(cfg.Regions, ",") != "ap-south,eu-west,us-east" {
		t.Errorf("regions = %v", cfg.Regions)
	}
}

func TestParseReportsEveryProblem(t *testing.T) {
	bad := `
notifiers:
  - name: hook
    type: webhook
    url: ftp://nope
monitors:
  - id: Bad_ID
    http: {url: "not a url"}
  - id: twice
    tcp: {address: "db:5432"}
    dns: {name: example.com}
  - id: dup
    tcp: {address: "db:5432"}
    notify: [missing]
  - id: dup
    tcp: {address: "db:5432"}
  - id: quorum
    regions: [a, b]
    quorum: 3
    timeout: 1m
    interval: 30s
    tcp: {address: "db:5432"}
status_pages:
  - slug: main
    components:
      - name: Thing
        monitors: [ghost]
`
	_, err := Parse([]byte(bad), env(nil))
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected ValidationError, got %v", err)
	}
	want := []string{
		"notifiers[0].url: must be an http(s) URL",
		"monitors[Bad_ID].id: must match",
		"monitors[Bad_ID].http.url: must be an http(s) URL",
		"monitors[twice]: exactly one of http, tcp, dns, or tls is required",
		`monitors[dup].notify: unknown notifier "missing"`,
		"monitors[dup].id: duplicate monitor id",
		"monitors[quorum].timeout: must be positive and no longer than the interval",
		"monitors[quorum].quorum: must be between 1 and the number of regions (2)",
		`status_pages[0].components[0].monitors: unknown monitor "ghost"`,
	}
	msg := err.Error()
	for _, w := range want {
		if !strings.Contains(msg, w) {
			t.Errorf("missing problem %q in:\n%s", w, msg)
		}
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	_, err := Parse([]byte("monitors:\n  - id: a\n    intervall: 5s\n    tcp: {address: 'a:1'}\n"), env(nil))
	if err == nil || !strings.Contains(err.Error(), "intervall") {
		t.Fatalf("typo should be rejected, got %v", err)
	}
}

func TestParseRequiresEnvironment(t *testing.T) {
	doc := "# set ${IN_A_COMMENT} before starting\nnotifiers:\n  - name: a\n    type: webhook\n    url: ${HOOK_A}/${HOOK_B}/${HOOK_A}\n"
	_, err := Parse([]byte(doc), env(nil))
	if err == nil || !strings.HasSuffix(err.Error(), "variables: HOOK_A, HOOK_B") {
		t.Fatalf("got %v", err)
	}
}

func TestParseEmptyFile(t *testing.T) {
	cfg, err := Parse(nil, env(nil))
	if err != nil || len(cfg.Monitors) != 0 {
		t.Fatalf("empty config: %v %+v", err, cfg)
	}
}

func TestParseDuration(t *testing.T) {
	cases := map[string]time.Duration{"7d": 7 * 24 * time.Hour, "2w": 14 * 24 * time.Hour, "90s": 90 * time.Second, "off": 0, "1h30m": 90 * time.Minute}
	for in, want := range cases {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Errorf("ParseDuration(%q) = %s, %v; want %s", in, got, err, want)
		}
	}
	if _, err := ParseDuration("7days"); err == nil {
		t.Error("7days should not parse")
	}
}

// TestShippedConfigsAreValid keeps the example configs in the repository
// from rotting.
func TestShippedConfigsAreValid(t *testing.T) {
	files, _ := filepath.Glob("../../examples/*.yaml")
	files = append(files, "../../deploy/compose/vigil.yaml")
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if _, err := Parse(data, func(string) (string, bool) { return "https://hooks.example.test/x", true }); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
