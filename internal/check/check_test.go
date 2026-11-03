package check

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vigil/internal/dnstest"
)

func ptr[T any](v T) *T { return &v }

func httpSpec(url string, mod func(*HTTP)) Spec {
	h := &HTTP{URL: url, Method: http.MethodGet}
	if mod != nil {
		mod(h)
	}
	return Spec{Type: TypeHTTP, Timeout: 2 * time.Second, HTTP: h}
}

func TestHTTPAssertions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			_, _ = w.Write([]byte("<h1>All good</h1>"))
		case "/json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","checks":[{"name":"db","healthy":true,"lag":0.5}]}`))
		case "/degraded":
			_, _ = w.Write([]byte(`{"status":"degraded"}`))
		case "/broken":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/created":
			w.WriteHeader(http.StatusCreated)
		case "/ua":
			_, _ = w.Write([]byte(r.UserAgent()))
		}
	}))
	defer srv.Close()

	tests := []struct {
		name    string
		path    string
		mod     func(*HTTP)
		wantOK  bool
		wantErr string
		status  int
	}{
		{name: "2xx by default", path: "/ok", wantOK: true, status: 200},
		{name: "5xx fails", path: "/broken", wantErr: "status 503, want 2xx", status: 503},
		{name: "explicit codes", path: "/created", mod: func(h *HTTP) { h.Expect.Status = []string{"200", "201"} }, wantOK: true, status: 201},
		{name: "explicit code mismatch", path: "/ok", mod: func(h *HTTP) { h.Expect.Status = []string{"204"} }, wantErr: "status 200, want 204"},
		{name: "expected failure status", path: "/broken", mod: func(h *HTTP) { h.Expect.Status = []string{"5xx"} }, wantOK: true},
		{name: "keyword present", path: "/ok", mod: func(h *HTTP) { h.Expect.BodyContains = "All good" }, wantOK: true},
		{name: "keyword missing", path: "/ok", mod: func(h *HTTP) { h.Expect.BodyContains = "Welcome" }, wantErr: `body does not contain "Welcome"`},
		{
			name: "json string, bool, and number assertions",
			path: "/json",
			mod: func(h *HTTP) {
				h.Expect.JSON = []JSONAssertion{
					{Path: "status", Equals: ptr("ok")},
					{Path: "checks.0.healthy", Equals: ptr("true")},
					{Path: "checks.0.lag", Equals: ptr("0.5")},
					{Path: "checks.0.name"},
				}
			},
			wantOK: true,
		},
		{name: "json value mismatch", path: "/degraded", mod: func(h *HTTP) { h.Expect.JSON = []JSONAssertion{{Path: "status", Equals: ptr("ok")}} }, wantErr: "json status = degraded, want ok"},
		{name: "json path missing", path: "/json", mod: func(h *HTTP) { h.Expect.JSON = []JSONAssertion{{Path: "checks.3.name"}} }, wantErr: "json checks.3.name: missing"},
		{name: "json on non-json body", path: "/ok", mod: func(h *HTTP) { h.Expect.JSON = []JSONAssertion{{Path: "status"}} }, wantErr: "body is not valid JSON"},
		{name: "user agent identifies the probe", path: "/ua", mod: func(h *HTTP) { h.Expect.BodyContains = "Vigil/test" }, wantOK: true},
	}
	r := &Runner{UserAgent: "Vigil/test (region=eu-west)"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := r.Run(context.Background(), httpSpec(srv.URL+tt.path, tt.mod))
			if res.OK != tt.wantOK || (tt.wantErr != "" && res.Error != tt.wantErr) {
				t.Fatalf("got ok=%v err=%q, want ok=%v err=%q", res.OK, res.Error, tt.wantOK, tt.wantErr)
			}
			if tt.status != 0 && res.StatusCode != tt.status {
				t.Fatalf("status %d, want %d", res.StatusCode, tt.status)
			}
			if res.Latency <= 0 {
				t.Fatal("latency not recorded")
			}
		})
	}
}

func TestHTTPTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	spec := httpSpec(srv.URL, nil)
	spec.Timeout = 150 * time.Millisecond
	start := time.Now()
	res := (&Runner{}).Run(context.Background(), spec)
	if res.OK || res.Error != "timeout after 150ms" {
		t.Fatalf("got ok=%v err=%q", res.OK, res.Error)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %s", elapsed)
	}
}

func TestHTTPSlowResponseAssertion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(60 * time.Millisecond)
	}))
	defer srv.Close()
	res := (&Runner{}).Run(context.Background(), httpSpec(srv.URL, func(h *HTTP) { h.Expect.MaxLatency = 20 * time.Millisecond }))
	if res.OK || !strings.HasPrefix(res.Error, "slow response:") {
		t.Fatalf("got ok=%v err=%q", res.OK, res.Error)
	}
}

func TestHTTPRedirects(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/old", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/new", http.StatusMovedPermanently) })
	mux.HandleFunc("/new", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("moved here")) })
	mux.HandleFunc("/loop", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/loop", http.StatusFound) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	r := &Runner{}

	t.Run("followed by default", func(t *testing.T) {
		res := r.Run(context.Background(), httpSpec(srv.URL+"/old", func(h *HTTP) { h.Expect.BodyContains = "moved here" }))
		if !res.OK || res.StatusCode != 200 {
			t.Fatalf("got ok=%v status=%d err=%q", res.OK, res.StatusCode, res.Error)
		}
	})
	t.Run("not followed when disabled", func(t *testing.T) {
		res := r.Run(context.Background(), httpSpec(srv.URL+"/old", func(h *HTTP) {
			h.FollowRedirects = ptr(false)
			h.Expect.Status = []string{"301"}
		}))
		if !res.OK || res.StatusCode != 301 {
			t.Fatalf("got ok=%v status=%d err=%q", res.OK, res.StatusCode, res.Error)
		}
	})
	t.Run("loops are cut off", func(t *testing.T) {
		res := r.Run(context.Background(), httpSpec(srv.URL+"/loop", func(h *HTTP) { h.MaxRedirects = 3 }))
		if res.OK || res.Error != "stopped after 3 redirects" {
			t.Fatalf("got ok=%v err=%q", res.OK, res.Error)
		}
	})
}

func TestHTTPConnectionRefused(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	_ = ln.Close()
	res := (&Runner{}).Run(context.Background(), httpSpec("http://"+addr, nil))
	if res.OK || res.Error != "connection refused" {
		t.Fatalf("got ok=%v err=%q", res.OK, res.Error)
	}
}

func TestHTTPSVerification(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the rejected handshake is the point
	srv.StartTLS()
	defer srv.Close()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())

	t.Run("unknown authority fails", func(t *testing.T) {
		res := (&Runner{}).Run(context.Background(), httpSpec(srv.URL, nil))
		if res.OK || res.Error != "tls: certificate signed by unknown authority" {
			t.Fatalf("got ok=%v err=%q", res.OK, res.Error)
		}
	})
	t.Run("trusted root passes and records expiry", func(t *testing.T) {
		res := (&Runner{RootCAs: pool}).Run(context.Background(), httpSpec(srv.URL, nil))
		if !res.OK || res.CertNotAfter.IsZero() {
			t.Fatalf("got ok=%v err=%q notAfter=%v", res.OK, res.Error, res.CertNotAfter)
		}
	})
	t.Run("insecure skips verification", func(t *testing.T) {
		res := (&Runner{}).Run(context.Background(), httpSpec(srv.URL, func(h *HTTP) { h.Insecure = true }))
		if !res.OK {
			t.Fatalf("got err=%q", res.Error)
		}
	})
}

// selfSigned returns a certificate for 127.0.0.1 valid until notAfter and a
// pool that trusts it.
func selfSigned(t *testing.T, notAfter time.Time) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "vigil test"},
		NotBefore:             time.Now().Add(-48 * time.Hour),
		NotAfter:              notAfter,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// tlsServer accepts TLS handshakes with a certificate valid until notAfter.
func tlsServer(t *testing.T, notAfter time.Time) (string, *x509.CertPool) {
	t.Helper()
	cert, pool := selfSigned(t, notAfter)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_ = c.(*tls.Conn).Handshake()
				_ = c.Close()
			}()
		}
	}()
	return ln.Addr().String(), pool
}

func TestTLSExpiry(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name        string
		notAfter    time.Time
		insecure    bool
		wantOK      bool
		wantWarning string
		wantErr     string
	}{
		{name: "healthy certificate", notAfter: now.Add(90 * 24 * time.Hour), wantOK: true},
		{name: "inside the warning window", notAfter: now.Add(9*24*time.Hour + time.Hour), wantOK: true, wantWarning: "certificate expires in 9 days"},
		// Verification rejects an expired chain before expiry math runs.
		{name: "expired certificate", notAfter: now.Add(-time.Hour), wantErr: "tls: certificate expired"},
		{name: "expired certificate, verification off", notAfter: now.Add(-time.Hour), insecure: true, wantErr: "certificate expired"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, pool := tlsServer(t, tt.notAfter)
			res := (&Runner{RootCAs: pool}).Run(context.Background(), Spec{
				Type: TypeTLS, Timeout: 2 * time.Second, TLS: &TLS{Address: addr, WarnDays: 14, Insecure: tt.insecure},
			})
			if res.OK != tt.wantOK || !strings.HasPrefix(res.Warning, tt.wantWarning) || !strings.HasPrefix(res.Error, tt.wantErr) {
				t.Fatalf("got ok=%v warning=%q err=%q", res.OK, res.Warning, res.Error)
			}
		})
	}
}

func TestHTTPSWarnsBeforeExpiry(t *testing.T) {
	cert, pool := selfSigned(t, time.Now().Add(5*24*time.Hour+time.Hour))
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	defer srv.Close()

	res := (&Runner{RootCAs: pool}).Run(context.Background(), httpSpec(srv.URL, func(h *HTTP) { h.CertWarnDays = 7 }))
	if !res.OK || !strings.HasPrefix(res.Warning, "certificate expires in 5 days") {
		t.Fatalf("got ok=%v warning=%q err=%q", res.OK, res.Warning, res.Error)
	}
}

func TestTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	open := ln.Addr().String()
	res := (&Runner{}).Run(context.Background(), Spec{Type: TypeTCP, Timeout: time.Second, TCP: &TCP{Address: open}})
	if !res.OK {
		t.Fatalf("open port: %q", res.Error)
	}
	_ = ln.Close()
	res = (&Runner{}).Run(context.Background(), Spec{Type: TypeTCP, Timeout: time.Second, TCP: &TCP{Address: open}})
	if res.OK || res.Error != "connection refused" {
		t.Fatalf("closed port: ok=%v err=%q", res.OK, res.Error)
	}
}

func TestDNS(t *testing.T) {
	srv, err := dnstest.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.Set("api.vigil.test", "A", "10.0.0.7", "10.0.0.8")
	srv.Set("www.vigil.test", "CNAME", "edge.vigil.test")
	srv.Set("edge.vigil.test", "A", "10.0.0.9")
	srv.Set("vigil.test", "TXT", "v=spf1 -all")

	tests := []struct {
		name    string
		spec    DNS
		wantOK  bool
		wantErr string
	}{
		{name: "A record resolves", spec: DNS{Name: "api.vigil.test", Type: "A"}, wantOK: true},
		{name: "A record matches expectation", spec: DNS{Name: "api.vigil.test", Type: "A", Expect: []string{"10.0.0.8"}}, wantOK: true},
		{name: "A record missing expected value", spec: DNS{Name: "api.vigil.test", Type: "A", Expect: []string{"10.0.0.1"}}, wantErr: "dns: A api.vigil.test answered 10.0.0.7, 10.0.0.8, missing 10.0.0.1"},
		{name: "CNAME", spec: DNS{Name: "www.vigil.test", Type: "CNAME", Expect: []string{"edge.vigil.test."}}, wantOK: true},
		{name: "TXT", spec: DNS{Name: "vigil.test", Type: "TXT", Expect: []string{"v=spf1 -all"}}, wantOK: true},
		{name: "NXDOMAIN", spec: DNS{Name: "nope.vigil.test", Type: "A"}, wantErr: "dns: no such host nope.vigil.test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := tt.spec
			spec.Resolver = srv.Addr()
			res := (&Runner{}).Run(context.Background(), Spec{Type: TypeDNS, Timeout: 2 * time.Second, DNS: &spec})
			if res.OK != tt.wantOK || (tt.wantErr != "" && res.Error != tt.wantErr) {
				t.Fatalf("got ok=%v err=%q", res.OK, res.Error)
			}
		})
	}
}
