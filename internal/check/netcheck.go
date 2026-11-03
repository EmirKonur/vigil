package check

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"
)

// TCP checks that a TCP connection can be established.
type TCP struct {
	Address string `yaml:"address" json:"address"`
}

func (r *Runner) runTCP(ctx context.Context, t *TCP) Result {
	start := time.Now()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", t.Address)
	latency := time.Since(start)
	if err != nil {
		return Result{Error: describe(err), Latency: latency}
	}
	_ = conn.Close()
	return Result{OK: true, Latency: latency}
}

// DNS checks that a name resolves, optionally to expected values.
type DNS struct {
	Name string `yaml:"name" json:"name"`
	// Type is one of A, AAAA, CNAME, MX, NS, TXT.
	Type string `yaml:"type" json:"type"`
	// Resolver is a host:port to query instead of the system resolver.
	Resolver string `yaml:"resolver" json:"resolver,omitempty"`
	// Expect lists values that must all be present in the answer.
	Expect []string `yaml:"expect" json:"expect,omitempty"`
}

// DNSTypes are the record types a DNS check may query.
var DNSTypes = []string{"A", "AAAA", "CNAME", "MX", "NS", "TXT"}

func (r *Runner) runDNS(ctx context.Context, d *DNS) Result {
	res := &net.Resolver{}
	if d.Resolver != "" {
		res = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, d.Resolver)
			},
		}
	}
	start := time.Now()
	answers, err := lookupDNS(ctx, res, d.Type, d.Name)
	latency := time.Since(start)
	if err != nil {
		return Result{Error: describe(err), Latency: latency}
	}
	if len(answers) == 0 {
		return Result{Error: fmt.Sprintf("dns: no %s records for %s", d.Type, d.Name), Latency: latency}
	}
	for _, want := range d.Expect {
		if !slices.Contains(answers, normalizeRecord(want)) {
			return Result{
				Error:   fmt.Sprintf("dns: %s %s answered %s, missing %s", d.Type, d.Name, strings.Join(answers, ", "), want),
				Latency: latency,
			}
		}
	}
	return Result{OK: true, Latency: latency}
}

func lookupDNS(ctx context.Context, res *net.Resolver, typ, name string) ([]string, error) {
	var out []string
	switch strings.ToUpper(typ) {
	case "A", "AAAA":
		network := "ip4"
		if strings.EqualFold(typ, "AAAA") {
			network = "ip6"
		}
		ips, err := res.LookupIP(ctx, network, name)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			out = append(out, ip.String())
		}
	case "CNAME":
		cname, err := res.LookupCNAME(ctx, name)
		if err != nil {
			return nil, err
		}
		out = append(out, cname)
	case "MX":
		mxs, err := res.LookupMX(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, mx := range mxs {
			out = append(out, mx.Host)
		}
	case "NS":
		nss, err := res.LookupNS(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, ns := range nss {
			out = append(out, ns.Host)
		}
	case "TXT":
		txts, err := res.LookupTXT(ctx, name)
		if err != nil {
			return nil, err
		}
		out = append(out, txts...)
	default:
		return nil, fmt.Errorf("unsupported record type %q", typ)
	}
	for i := range out {
		out[i] = normalizeRecord(out[i])
	}
	return out, nil
}

func normalizeRecord(v string) string { return strings.TrimSuffix(strings.ToLower(v), ".") }

// TLS checks a TLS endpoint's certificate chain and expiry.
type TLS struct {
	Address    string `yaml:"address" json:"address"`
	ServerName string `yaml:"server_name" json:"server_name,omitempty"`
	// WarnDays reports a warning when the leaf expires within this many days.
	WarnDays int `yaml:"warn_days" json:"warn_days"`
	// Insecure skips chain verification and only reports expiry.
	Insecure bool `yaml:"insecure" json:"insecure,omitempty"`
}

func (r *Runner) runTLS(ctx context.Context, t *TLS) Result {
	serverName := t.ServerName
	if serverName == "" {
		host, _, err := net.SplitHostPort(t.Address)
		if err != nil {
			return Result{Error: err.Error()}
		}
		serverName = host
	}
	dialer := &tls.Dialer{Config: &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: t.Insecure,
		RootCAs:            r.RootCAs,
	}}
	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", t.Address)
	latency := time.Since(start)
	if err != nil {
		return Result{Error: describe(err), Latency: latency}
	}
	defer conn.Close()
	state := conn.(*tls.Conn).ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return Result{Error: "tls: server sent no certificate", Latency: latency}
	}
	notAfter := state.PeerCertificates[0].NotAfter
	res := Result{Latency: latency, CertNotAfter: notAfter}
	warnDays := t.WarnDays
	if warnDays == 0 {
		warnDays = 14
	}
	w, expired := certWarning(notAfter, r.now(), warnDays)
	if expired {
		res.Error = w
		return res
	}
	res.OK = true
	res.Warning = w
	return res
}
