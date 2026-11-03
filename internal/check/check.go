// Package check defines check specifications and the runners that execute
// them. Specs travel from the server to probes as JSON, so everything here
// is plain data plus a Runner that interprets it.
package check

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"
)

// Type names a check kind.
type Type string

// Check types.
const (
	TypeHTTP Type = "http"
	TypeTCP  Type = "tcp"
	TypeDNS  Type = "dns"
	TypeTLS  Type = "tls"
)

// Spec is a complete, validated check definition. Exactly one of the typed
// fields is set, matching Type.
type Spec struct {
	Type    Type          `json:"type"`
	Timeout time.Duration `json:"timeout"`
	HTTP    *HTTP         `json:"http,omitempty"`
	TCP     *TCP          `json:"tcp,omitempty"`
	DNS     *DNS          `json:"dns,omitempty"`
	TLS     *TLS          `json:"tls,omitempty"`
}

// Target is a short human description of what the spec checks.
func (s Spec) Target() string {
	switch s.Type {
	case TypeHTTP:
		return s.HTTP.Method + " " + s.HTTP.URL
	case TypeTCP:
		return "tcp://" + s.TCP.Address
	case TypeDNS:
		return s.DNS.Type + " " + s.DNS.Name
	case TypeTLS:
		return "tls://" + s.TLS.Address
	}
	return ""
}

// Result is the outcome of one check run.
type Result struct {
	OK           bool          `json:"ok"`
	Latency      time.Duration `json:"latency"`
	Error        string        `json:"error,omitempty"`
	StatusCode   int           `json:"status_code,omitempty"`
	Warning      string        `json:"warning,omitempty"`
	CertNotAfter time.Time     `json:"cert_not_after,omitzero"`
}

// Runner executes specs. The zero value is usable; fields customize the
// environment for probes and tests.
type Runner struct {
	// UserAgent is sent with HTTP checks.
	UserAgent string
	// Now reports the current time for certificate expiry math.
	Now func() time.Time
	// RootCAs overrides the system roots for TLS verification (tests).
	RootCAs *x509.CertPool
}

// Run executes spec, bounded by spec.Timeout and ctx.
func (r *Runner) Run(ctx context.Context, spec Spec) Result {
	timeout := spec.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	var res Result
	switch spec.Type {
	case TypeHTTP:
		res = r.runHTTP(ctx, spec.HTTP)
	case TypeTCP:
		res = r.runTCP(ctx, spec.TCP)
	case TypeDNS:
		res = r.runDNS(ctx, spec.DNS)
	case TypeTLS:
		res = r.runTLS(ctx, spec.TLS)
	default:
		res = Result{Error: fmt.Sprintf("unknown check type %q", spec.Type)}
	}
	if res.Latency == 0 {
		res.Latency = time.Since(start)
	}
	if res.Error != "" && ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		res.Error = fmt.Sprintf("timeout after %s", timeout)
	}
	return res
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// describe turns low-level network errors into the short messages shown in
// the dashboard and alerts.
func describe(err error) string {
	var dnsErr *net.DNSError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection reset by peer"
	case errors.As(err, &dnsErr):
		if dnsErr.IsNotFound {
			return "dns: no such host " + dnsErr.Name
		}
		return "dns: " + dnsErr.Err
	}
	var certErr x509.CertificateInvalidError
	var unknownAuth x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	switch {
	case errors.As(err, &certErr) && certErr.Reason == x509.Expired && certErr.Cert != nil:
		if time.Now().Before(certErr.Cert.NotBefore) {
			return "tls: certificate not valid until " + certErr.Cert.NotBefore.UTC().Format("2006-01-02")
		}
		return "tls: certificate expired " + certErr.Cert.NotAfter.UTC().Format("2006-01-02")
	case errors.As(err, &certErr):
		return "tls: " + certErr.Error()
	case errors.As(err, &unknownAuth):
		return "tls: certificate signed by unknown authority"
	case errors.As(err, &hostErr):
		return "tls: " + hostErr.Error()
	}
	return err.Error()
}

func certWarning(notAfter, now time.Time, warnDays int) (warning string, expired bool) {
	left := notAfter.Sub(now)
	if left <= 0 {
		return fmt.Sprintf("certificate expired %s", notAfter.UTC().Format("2006-01-02")), true
	}
	days := int(left.Hours() / 24)
	if warnDays > 0 && days < warnDays {
		return fmt.Sprintf("certificate expires in %d days (%s)", days, notAfter.UTC().Format("2006-01-02")), false
	}
	return "", false
}
