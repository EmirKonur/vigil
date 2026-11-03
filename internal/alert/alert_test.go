package alert

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vigil/internal/clock"
	"vigil/internal/config"
	"vigil/internal/store"
)

var opened = time.Date(2026, 9, 2, 14, 2, 0, 0, time.UTC)

func sample(kind Kind) Message {
	m := Message{
		Kind:       kind,
		DeliveryID: "incident/checkout/1788357720000/" + string(kind),
		At:         opened.Add(14 * time.Minute),
		Monitor:    Monitor{ID: "checkout", Name: "Checkout API", Target: "GET https://checkout.example.com/health"},
		Incident: &Incident{
			ID: 7, OpenedAt: opened, Duration: 14 * time.Minute, Impact: "partial",
			Regions: []string{"eu-west", "us-east"}, Voting: 3,
		},
		Reason:     "timeout after 10s",
		URL:        "https://vigil.example.com/admin/monitors/checkout",
		FlapWindow: 30 * time.Minute,
	}
	return m
}

func TestTemplates(t *testing.T) {
	tests := []struct {
		kind  Kind
		title string
		body  []string
	}{
		{KindOpened, "DOWN: Checkout API (partial outage)", []string{
			"Checkout API is failing from 2 of 3 regions: eu-west, us-east.",
			"Last error: timeout after 10s",
			"Down since 2026-09-02 14:02 UTC.",
			"Target: GET https://checkout.example.com/health",
			"Details: https://vigil.example.com/admin/monitors/checkout",
		}},
		{KindReminder, "STILL DOWN: Checkout API, 14m", nil},
		{KindEscalated, "ESCALATED: Checkout API down for 14m", nil},
		{KindResolved, "RESOLVED: Checkout API after 14m", []string{"Checkout API recovered at 2026-09-02 14:16 UTC. It was down for 14m."}},
		{KindFlapping, "FLAPPING: Checkout API", []string{"stable for 30m"}},
	}
	for _, tt := range tests {
		t.Run(string(tt.kind), func(t *testing.T) {
			m := sample(tt.kind)
			if got := Title(m); got != tt.title {
				t.Errorf("title = %q, want %q", got, tt.title)
			}
			body := Body(m)
			for _, line := range tt.body {
				if !strings.Contains(body, line) {
					t.Errorf("body missing %q:\n%s", line, body)
				}
			}
			if strings.Contains(body, "template error") || strings.Contains(body, "<no value>") {
				t.Errorf("broken render:\n%s", body)
			}
		})
	}

	cert := Message{Kind: KindCertExpiring, Monitor: Monitor{Name: "Edge TLS", Target: "tls://edge.example.com:443"}, Warning: "expires in 9 days (2026-09-11)"}
	if got := Title(cert); got != "CERTIFICATE: Edge TLS expires in 9 days (2026-09-11)" {
		t.Errorf("cert title = %q", got)
	}
}

func TestHumanDuration(t *testing.T) {
	cases := map[time.Duration]string{
		42 * time.Second: "42s", 4 * time.Minute: "4m", 72 * time.Minute: "1h 12m",
		3 * time.Hour: "3h", 74 * time.Hour: "3d 2h", 48 * time.Hour: "2d",
	}
	for d, want := range cases {
		if got := HumanDuration(d); got != want {
			t.Errorf("%s: got %q want %q", d, got, want)
		}
	}
}

type captured struct {
	header http.Header
	body   []byte
}

func receiver(t *testing.T, status int) (*httptest.Server, func() []captured) {
	t.Helper()
	var mu sync.Mutex
	var got []captured
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, captured{r.Header.Clone(), b})
		mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []captured { mu.Lock(); defer mu.Unlock(); return append([]captured(nil), got...) }
}

func TestWebhookPresets(t *testing.T) {
	srv, received := receiver(t, http.StatusOK)
	for _, preset := range []string{"generic", "slack", "discord"} {
		n := New(config.Notifier{Name: preset, Type: "webhook", URL: srv.URL, Preset: preset, Secret: "s3cret"}, "Vigil/test")
		if err := n.Send(context.Background(), sample(KindOpened)); err != nil {
			t.Fatalf("%s: %v", preset, err)
		}
	}
	got := received()
	if len(got) != 3 {
		t.Fatalf("received %d requests", len(got))
	}
	for _, c := range got {
		if c.header.Get("X-Vigil-Delivery") != "incident/checkout/1788357720000/opened" || c.header.Get("X-Vigil-Event") != "opened" {
			t.Errorf("missing delivery headers: %v", c.header)
		}
		mac := hmac.New(sha256.New, []byte("s3cret"))
		mac.Write(c.body)
		if c.header.Get("X-Vigil-Signature") != "sha256="+hex.EncodeToString(mac.Sum(nil)) {
			t.Error("signature does not verify")
		}
	}

	var generic struct {
		Schema, Kind, DeliveryID, Title string
		Monitor                         Monitor
		Incident                        Incident
	}
	_ = json.Unmarshal(got[0].body, &generic)
	if generic.Schema != Schema || generic.Kind != "opened" || generic.Incident.Voting != 3 || generic.Title == "" {
		t.Errorf("generic payload: %s", got[0].body)
	}

	var slack struct {
		Text   string
		Blocks []struct {
			Type string
			Text struct{ Text string }
		}
	}
	_ = json.Unmarshal(got[1].body, &slack)
	if slack.Text != "DOWN: Checkout API (partial outage)" || len(slack.Blocks) != 2 || !strings.HasPrefix(slack.Blocks[0].Text.Text, "*DOWN") {
		t.Errorf("slack payload: %s", got[1].body)
	}

	var discord struct {
		Embeds []struct {
			Title       string
			Description string
			Color       int
			Timestamp   string
		}
	}
	_ = json.Unmarshal(got[2].body, &discord)
	if len(discord.Embeds) != 1 || discord.Embeds[0].Color != 0xC43C25 || discord.Embeds[0].Timestamp != "2026-09-02T14:16:00Z" {
		t.Errorf("discord payload: %s", got[2].body)
	}
}

func TestWebhookErrorClassification(t *testing.T) {
	for status, permanent := range map[int]bool{404: true, 401: true, 429: false, 503: false} {
		srv, _ := receiver(t, status)
		err := New(config.Notifier{Type: "webhook", URL: srv.URL}, "Vigil/test").Send(context.Background(), sample(KindOpened))
		var perm *PermanentError
		if err == nil || errors.As(err, &perm) != permanent {
			t.Errorf("status %d: err=%v permanent=%v, want permanent=%v", status, err, errors.As(err, &perm), permanent)
		}
	}
}

// fakeSMTP accepts one plain SMTP session and records the envelope and data.
type fakeSMTP struct {
	addr string
	mu   sync.Mutex
	rcpt []string
	data string
}

func startSMTP(t *testing.T) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	f := &fakeSMTP{addr: ln.Addr().String()}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		tp := textproto.NewConn(conn)
		_ = tp.PrintfLine("220 fake ESMTP")
		for {
			line, err := tp.ReadLine()
			if err != nil {
				return
			}
			cmd := strings.ToUpper(strings.SplitN(line, " ", 2)[0])
			switch cmd {
			case "EHLO", "HELO":
				_ = tp.PrintfLine("250-fake\r\n250 8BITMIME")
			case "MAIL":
				_ = tp.PrintfLine("250 ok")
			case "RCPT":
				f.mu.Lock()
				f.rcpt = append(f.rcpt, line)
				f.mu.Unlock()
				_ = tp.PrintfLine("250 ok")
			case "DATA":
				_ = tp.PrintfLine("354 go ahead")
				b, _ := tp.ReadDotBytes()
				f.mu.Lock()
				f.data = string(b)
				f.mu.Unlock()
				_ = tp.PrintfLine("250 queued")
			case "QUIT":
				_ = tp.PrintfLine("221 bye")
				return
			default:
				_ = tp.PrintfLine("502 unknown")
			}
		}
	}()
	return f
}

func TestEmail(t *testing.T) {
	srv := startSMTP(t)
	host, port, _ := net.SplitHostPort(srv.addr)
	p, _ := strconv.Atoi(port)
	n := New(config.Notifier{
		Type: "email", To: []string{"oncall@example.com", "lead@example.com"},
		SMTP: config.SMTP{Host: host, Port: p, From: "vigil@example.com", TLS: "none"},
	}, "Vigil/test")
	if err := n.Send(context.Background(), sample(KindResolved)); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.rcpt) != 2 {
		t.Fatalf("rcpt = %v", srv.rcpt)
	}
	for _, want := range []string{
		"Subject: [Vigil] RESOLVED: Checkout API after 14m",
		"To: oncall@example.com, lead@example.com",
		"X-Vigil-Delivery: incident/checkout/1788357720000/resolved",
		"Checkout API recovered at 2026-09-02 14:16 UTC.",
	} {
		if !strings.Contains(srv.data, want) {
			t.Errorf("email missing %q:\n%s", want, srv.data)
		}
	}
}

type flaky struct {
	mu    sync.Mutex
	fails int
	sent  []Message
}

func (f *flaky) Send(_ context.Context, m Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fails > 0 {
		f.fails--
		return errors.New("connection reset")
	}
	f.sent = append(f.sent, m)
	return nil
}

func TestDispatcherRetriesWithBackoff(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "v.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	clk := clock.NewFake(opened)
	d := NewDispatcher(st, clk, slog.New(slog.DiscardHandler))
	hook := &flaky{fails: 2}
	d.SetNotifiers(map[string]Notifier{"hook": hook, "dead": &flaky{fails: 1000}})
	d.MaxAttempts = 3

	msg, _ := json.Marshal(sample(KindOpened))
	_ = st.Tx(ctx, func(tx *store.Tx) error {
		for _, name := range []string{"hook", "dead", "removed"} {
			if _, err := tx.EnqueueNotification(ctx, store.Notification{Notifier: name, DedupKey: "k", MonitorID: "checkout", Kind: "opened", Message: msg, CreatedAt: opened}); err != nil {
				return err
			}
		}
		return nil
	})

	attempt := func() int { n, _ := d.DeliverDue(ctx); return n }
	if n := attempt(); n != 3 {
		t.Fatalf("first pass attempted %d", n)
	}
	if n := attempt(); n != 0 {
		t.Fatalf("retried before backoff elapsed: %d", n)
	}
	clk.Advance(Backoff(1))
	attempt()
	clk.Advance(Backoff(2))
	attempt()
	clk.Advance(time.Hour)
	if n := attempt(); n != 0 {
		t.Fatalf("attempts continued past the limit: %d", n)
	}
	if len(hook.sent) != 1 {
		t.Fatalf("hook received %d messages", len(hook.sent))
	}
	rows, _ := st.RecentNotifications(ctx, "", 10)
	status := map[string]string{}
	for _, r := range rows {
		status[r.Notifier] = r.Status
	}
	if status["hook"] != "sent" || status["dead"] != "failed" || status["removed"] != "failed" {
		t.Fatalf("statuses = %v", status)
	}
}
