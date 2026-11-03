package demo

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"regexp"
	"time"

	"vigil/internal/dnstest"
)

// RegionLatency is the simulated network latency, in milliseconds, between
// each demo region and the demo targets.
var RegionLatency = map[string]float64{"eu-west": 38, "us-east": 92, "ap-south": 176}

// endpointCost scales region latency per endpoint, so services differ.
var endpointCost = map[string]float64{"/": 1.0, "/api/health": 0.8, "/checkout/health": 1.6, "/search": 2.1, "/webhooks/health": 0.9, "/flaky": 1.0}

// TargetOptions configures the demo targets. Empty addresses disable a
// listener.
type TargetOptions struct {
	HTTPAddr, TCPAddr, DNSAddr, TLSAddr string
	// Start anchors the scripted scenarios.
	Start time.Time
	// SearchOutage makes /search fail from us-east and ap-south for this
	// long after Start, so `vigil demo` opens with a live incident that
	// resolves on its own.
	SearchOutage time.Duration
	// Cycle drives /flaky through healthy, regional-failure, and full
	// outage phases of these lengths, repeating. A regional failure (only
	// eu-west fails) must not open an incident; the outage must.
	Cycle [3]time.Duration
	Log   *slog.Logger
}

// Targets are the running demo listeners.
type Targets struct {
	HTTP, TCP, DNS, TLS string
	closers             []func() error
}

// Close stops every listener.
func (t *Targets) Close() error {
	var errs []error
	for _, c := range t.closers {
		errs = append(errs, c())
	}
	return errors.Join(errs...)
}

// StartTargets starts the listeners and returns their bound addresses.
func StartTargets(opts TargetOptions) (*Targets, error) {
	t := &Targets{}
	fail := func(err error) (*Targets, error) { _ = t.Close(); return nil, err }

	if opts.HTTPAddr != "" {
		ln, err := net.Listen("tcp", opts.HTTPAddr)
		if err != nil {
			return fail(err)
		}
		srv := &http.Server{Handler: targetMux(opts), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.Serve(ln) }()
		t.HTTP = "http://" + ln.Addr().String()
		t.closers = append(t.closers, srv.Close)
	}
	if opts.TCPAddr != "" {
		ln, err := net.Listen("tcp", opts.TCPAddr)
		if err != nil {
			return fail(err)
		}
		go acceptAndClose(ln, nil)
		t.TCP = ln.Addr().String()
		t.closers = append(t.closers, ln.Close)
	}
	if opts.DNSAddr != "" {
		srv, err := dnstest.Listen(opts.DNSAddr)
		if err != nil {
			return fail(err)
		}
		srv.Set("fernhill.test", "A", "10.20.0.10")
		srv.Set("www.fernhill.test", "CNAME", "fernhill.test")
		t.DNS = srv.Addr()
		t.closers = append(t.closers, srv.Close)
	}
	if opts.TLSAddr != "" {
		cert, err := selfSigned(opts.Start.Add(18 * 24 * time.Hour))
		if err != nil {
			return fail(err)
		}
		ln, err := tls.Listen("tcp", opts.TLSAddr, &tls.Config{Certificates: []tls.Certificate{cert}})
		if err != nil {
			return fail(err)
		}
		go acceptAndClose(ln, func(c net.Conn) { _ = c.(*tls.Conn).Handshake() })
		t.TLS = ln.Addr().String()
		t.closers = append(t.closers, ln.Close)
	}
	return t, nil
}

func acceptAndClose(ln net.Listener, handle func(net.Conn)) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			if handle != nil {
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				handle(c)
			}
			_ = c.Close()
		}()
	}
}

var regionInUA = regexp.MustCompile(`region=([a-z0-9-]+)`)

func regionOf(r *http.Request) string {
	if m := regionInUA.FindStringSubmatch(r.UserAgent()); m != nil {
		return m[1]
	}
	return ""
}

// FlakyPhase reports where /flaky is in its cycle at t, and when that
// phase ends.
func FlakyPhase(cycle [3]time.Duration, start, t time.Time) (string, time.Time) {
	total := cycle[0] + cycle[1] + cycle[2]
	if total <= 0 {
		return "healthy", time.Time{}
	}
	elapsed := t.Sub(start) % total
	cycleStart := t.Add(-elapsed)
	switch {
	case elapsed < cycle[0]:
		return "healthy", cycleStart.Add(cycle[0])
	case elapsed < cycle[0]+cycle[1]:
		return "regional", cycleStart.Add(cycle[0] + cycle[1])
	default:
		return "outage", cycleStart.Add(total)
	}
}

func targetMux(opts TargetOptions) http.Handler {
	log := opts.Log
	mux := http.NewServeMux()
	delay := func(r *http.Request, path string) {
		base, ok := RegionLatency[regionOf(r)]
		if !ok {
			base = 20
		}
		// Log-normal jitter: mostly near the base, with a realistic tail.
		ms := base * endpointCost[path] * math.Exp(mrand.NormFloat64()*0.18)
		select {
		case <-time.After(time.Duration(ms * float64(time.Millisecond))):
		case <-r.Context().Done():
		}
	}
	jsonOK := func(path string, body any) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			delay(r, path)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(body)
		}
	}

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		delay(r, "/")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, "<!doctype html><title>Fernhill</title><h1>Fernhill</h1><p>Demo target for Vigil.</p>")
	})
	mux.HandleFunc("GET /api/health", jsonOK("/api/health", map[string]string{"status": "ok", "version": "2026.11.1"}))
	mux.HandleFunc("GET /checkout/health", jsonOK("/checkout/health", map[string]string{"status": "ok"}))
	mux.HandleFunc("GET /webhooks/health", jsonOK("/webhooks/health", map[string]any{"status": "ok", "queue_depth": 3}))
	mux.HandleFunc("GET /search", func(w http.ResponseWriter, r *http.Request) {
		if opts.SearchOutage > 0 && time.Since(opts.Start) < opts.SearchOutage {
			switch regionOf(r) {
			case "us-east":
				// Hang past the check timeout.
				select {
				case <-time.After(30 * time.Second):
				case <-r.Context().Done():
				}
				return
			case "ap-south":
				delay(r, "/search")
				http.Error(w, "search cluster unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		jsonOK("/search", map[string]any{"results": 3})(w, r)
	})
	mux.HandleFunc("GET /flaky", func(w http.ResponseWriter, r *http.Request) {
		delay(r, "/flaky")
		phase, _ := FlakyPhase(opts.Cycle, opts.Start, time.Now())
		if phase == "outage" || (phase == "regional" && regionOf(r) == "eu-west") {
			http.Error(w, "upstream unavailable ("+phase+")", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /_scenario", func(w http.ResponseWriter, r *http.Request) {
		phase, until := FlakyPhase(opts.Cycle, opts.Start, time.Now())
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"flaky_phase": phase, "phase_ends": until.UTC().Format(time.RFC3339)})
	})
	mux.HandleFunc("POST /_webhook", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var payload struct {
			Title string `json:"title"`
			Text  string `json:"text"`
		}
		_ = json.Unmarshal(body, &payload)
		title := payload.Title
		if title == "" {
			title = payload.Text
		}
		log.Info("webhook received", "event", r.Header.Get("X-Vigil-Event"), "title", title, "delivery", r.Header.Get("X-Vigil-Delivery"))
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func selfSigned(notAfter time.Time) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "edge.fernhill.test"},
		DNSNames:     []string{"edge.fernhill.test", "localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    notAfter.Add(-90 * 24 * time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
