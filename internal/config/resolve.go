package config

import (
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"vigil/internal/check"
	"vigil/internal/consensus"
)

// Defaults applied when neither the monitor nor the defaults block sets a
// value. They favor fewer false alarms over the fastest possible detection:
// with a 30s interval an outage is confirmed in about 90 seconds.
const (
	DefaultInterval     = 30 * time.Second
	DefaultTimeout      = 10 * time.Second
	DefaultFailAfter    = 3
	DefaultRecoverAfter = 2
	DefaultFlapWindow   = 30 * time.Minute
	DefaultFlapFlips    = 4
	DefaultCertWarnDays = 14
	// LocalRegion is the region name of the probe embedded in the server.
	LocalRegion = "local"
)

var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// ValidationError lists every problem found, so a CI run of
// `vigil validate` reports them all at once.
type ValidationError struct{ Problems []string }

func (e *ValidationError) Error() string {
	return "invalid config:\n  " + strings.Join(e.Problems, "\n  ")
}

type problems []string

func (p *problems) add(path, format string, args ...any) {
	*p = append(*p, path+": "+fmt.Sprintf(format, args...))
}

func resolve(f file) (*Config, error) {
	var errs problems
	cfg := &Config{Notifiers: map[string]Notifier{}}

	for i, n := range f.Notifiers {
		path := fmt.Sprintf("notifiers[%d]", i)
		n, ok := resolveNotifier(&errs, path, n)
		if !ok {
			continue
		}
		if _, dup := cfg.Notifiers[n.Name]; dup {
			errs.add(path+".name", "duplicate notifier %q", n.Name)
			continue
		}
		cfg.Notifiers[n.Name] = n
	}

	seen := map[string]bool{}
	regions := map[string]bool{}
	for i, mf := range f.Monitors {
		path := fmt.Sprintf("monitors[%d]", i)
		if mf.ID != "" {
			path = fmt.Sprintf("monitors[%s]", mf.ID)
		}
		if seen[mf.ID] {
			errs.add(path+".id", "duplicate monitor id")
			continue
		}
		seen[mf.ID] = true
		m, ok := resolveMonitor(&errs, path, mf, f.Defaults, cfg.Notifiers)
		if !ok {
			continue
		}
		for _, r := range m.Policy.Regions {
			regions[r] = true
		}
		cfg.Monitors = append(cfg.Monitors, m)
	}
	for r := range regions {
		cfg.Regions = append(cfg.Regions, r)
	}
	sort.Strings(cfg.Regions)

	slugs := map[string]bool{}
	for i, sp := range f.StatusPages {
		path := fmt.Sprintf("status_pages[%d]", i)
		if !slugPattern.MatchString(sp.Slug) {
			errs.add(path+".slug", "must match %s", slugPattern)
		} else if slugs[sp.Slug] {
			errs.add(path+".slug", "duplicate status page %q", sp.Slug)
		}
		slugs[sp.Slug] = true
		if sp.Title == "" {
			sp.Title = sp.Slug
		}
		if len(sp.Components) == 0 {
			errs.add(path+".components", "at least one component is required")
		}
		for j, c := range sp.Components {
			cp := fmt.Sprintf("%s.components[%d]", path, j)
			if c.Name == "" {
				errs.add(cp+".name", "required")
			}
			if len(c.Monitors) == 0 {
				errs.add(cp+".monitors", "at least one monitor is required")
			}
			for _, id := range c.Monitors {
				if !seen[id] {
					errs.add(cp+".monitors", "unknown monitor %q", id)
				}
			}
		}
		cfg.StatusPages = append(cfg.StatusPages, sp)
	}

	cfg.Retention = resolveRetention(&errs, f.Retention)

	if len(errs) > 0 {
		return nil, &ValidationError{Problems: errs}
	}
	return cfg, nil
}

func resolveRetention(errs *problems, r Retention) Retention {
	def := func(d Duration, fallback time.Duration) Duration {
		if d == 0 {
			return Duration(fallback)
		}
		return d
	}
	r.Raw = def(r.Raw, 7*24*time.Hour)
	r.Hourly = def(r.Hourly, 90*24*time.Hour)
	r.Daily = def(r.Daily, 400*24*time.Hour)
	if time.Duration(r.Raw) < 48*time.Hour {
		errs.add("retention.raw", "must be at least 2d so recent hours can be re-rolled")
	}
	if r.Hourly < r.Raw {
		errs.add("retention.hourly", "must be at least retention.raw")
	}
	if time.Duration(r.Daily) < 90*24*time.Hour {
		errs.add("retention.daily", "must be at least 90d to fill status page uptime bars")
	}
	return r
}

func resolveNotifier(errs *problems, path string, n Notifier) (Notifier, bool) {
	before := len(*errs)
	if !slugPattern.MatchString(n.Name) {
		errs.add(path+".name", "must match %s", slugPattern)
	}
	switch n.Type {
	case "webhook":
		if u, err := url.Parse(n.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs.add(path+".url", "must be an http(s) URL")
		}
		if n.Preset == "" {
			n.Preset = "generic"
		}
		if !slices.Contains([]string{"generic", "slack", "discord"}, n.Preset) {
			errs.add(path+".preset", "must be generic, slack, or discord")
		}
	case "email":
		if len(n.To) == 0 {
			errs.add(path+".to", "at least one recipient is required")
		}
		for _, to := range n.To {
			if _, err := mail.ParseAddress(to); err != nil {
				errs.add(path+".to", "invalid address %q", to)
			}
		}
		if n.SMTP.Host == "" {
			errs.add(path+".smtp.host", "required")
		}
		if _, err := mail.ParseAddress(n.SMTP.From); err != nil {
			errs.add(path+".smtp.from", "must be an email address")
		}
		if n.SMTP.TLS == "" {
			n.SMTP.TLS = "starttls"
		}
		if !slices.Contains([]string{"starttls", "tls", "none"}, n.SMTP.TLS) {
			errs.add(path+".smtp.tls", "must be starttls, tls, or none")
		}
		if n.SMTP.Port == 0 {
			n.SMTP.Port = map[string]int{"starttls": 587, "tls": 465, "none": 25}[n.SMTP.TLS]
		}
	default:
		errs.add(path+".type", "must be webhook or email")
	}
	return n, len(*errs) == before
}

func resolveMonitor(errs *problems, path string, mf monitorFile, d monitorFields, notifiers map[string]Notifier) (Monitor, bool) {
	before := len(*errs)
	if !slugPattern.MatchString(mf.ID) {
		errs.add(path+".id", "must match %s", slugPattern)
	}
	m := Monitor{ID: mf.ID, Name: mf.Name, Description: mf.Description}
	if m.Name == "" {
		m.Name = mf.ID
	}

	m.Interval = durationOr(mf.Interval, d.Interval, DefaultInterval)
	timeout := durationOr(mf.Timeout, d.Timeout, min(DefaultTimeout, m.Interval))
	if m.Interval < time.Second {
		errs.add(path+".interval", "must be at least 1s")
	}
	if timeout <= 0 || timeout > m.Interval {
		errs.add(path+".timeout", "must be positive and no longer than the interval (%s)", m.Interval)
	}

	regions := firstNonEmpty(mf.Regions, d.Regions, []string{LocalRegion})
	regions = slices.Clone(regions)
	sort.Strings(regions)
	if len(slices.Compact(slices.Clone(regions))) != len(regions) {
		errs.add(path+".regions", "duplicate region")
	}
	for _, r := range regions {
		if !slugPattern.MatchString(r) {
			errs.add(path+".regions", "invalid region name %q", r)
		}
	}

	quorum, err := parseQuorum(firstString(mf.Quorum, d.Quorum, "majority"), len(regions))
	if err != nil {
		errs.add(path+".quorum", "%v", err)
	}

	m.Policy = consensus.Policy{
		Regions:      regions,
		Quorum:       quorum,
		FailAfter:    intOr(mf.FailAfter, d.FailAfter, DefaultFailAfter),
		RecoverAfter: intOr(mf.RecoverAfter, d.RecoverAfter, DefaultRecoverAfter),
		// Three missed intervals: one late result is jitter, three is a
		// probe that is gone.
		StaleAfter:    3*m.Interval + timeout,
		Renotify:      durationOr(mf.Renotify, d.Renotify, 0),
		FlapWindow:    DefaultFlapWindow,
		FlapThreshold: DefaultFlapFlips,
	}
	if m.Policy.FailAfter < 1 {
		errs.add(path+".fail_after", "must be at least 1")
	}
	if m.Policy.RecoverAfter < 1 {
		errs.add(path+".recover_after", "must be at least 1")
	}
	if flap := firstPtr(mf.Flap, d.Flap); flap != nil {
		m.Policy.FlapThreshold = flap.Threshold
		m.Policy.FlapWindow = time.Duration(flap.Window)
		if flap.Threshold < 0 || (flap.Threshold > 0 && flap.Window <= 0) {
			errs.add(path+".flap", "threshold must be >= 0 and window positive")
		}
	}

	m.Notify = firstNonEmpty(mf.Notify, d.Notify, nil)
	for _, n := range m.Notify {
		if _, ok := notifiers[n]; !ok {
			errs.add(path+".notify", "unknown notifier %q", n)
		}
	}
	if esc := firstPtr(mf.Escalate, d.Escalate); esc != nil {
		m.Policy.EscalateAfter = time.Duration(esc.After)
		m.Escalate = esc.Notify
		if esc.After <= 0 || len(esc.Notify) == 0 {
			errs.add(path+".escalate", "needs a positive 'after' and at least one notifier")
		}
		for _, n := range esc.Notify {
			if _, ok := notifiers[n]; !ok {
				errs.add(path+".escalate.notify", "unknown notifier %q", n)
			}
		}
	}
	m.CertWarnDays = intOr(mf.CertWarnDays, d.CertWarnDays, DefaultCertWarnDays)

	m.Spec = check.Spec{Timeout: timeout}
	kinds := 0
	if mf.HTTP != nil {
		kinds++
		m.Spec.Type, m.Spec.HTTP = check.TypeHTTP, resolveHTTP(errs, path+".http", *mf.HTTP, m.CertWarnDays)
	}
	if mf.TCP != nil {
		kinds++
		m.Spec.Type, m.Spec.TCP = check.TypeTCP, mf.TCP
		if _, _, err := net.SplitHostPort(mf.TCP.Address); err != nil {
			errs.add(path+".tcp.address", "must be host:port")
		}
	}
	if mf.DNS != nil {
		kinds++
		m.Spec.Type, m.Spec.DNS = check.TypeDNS, resolveDNS(errs, path+".dns", *mf.DNS)
	}
	if mf.TLS != nil {
		kinds++
		t := *mf.TLS
		if _, _, err := net.SplitHostPort(t.Address); err != nil {
			t.Address = net.JoinHostPort(t.Address, "443")
		}
		if t.WarnDays == 0 {
			t.WarnDays = m.CertWarnDays
		}
		m.Spec.Type, m.Spec.TLS = check.TypeTLS, &t
	}
	if kinds != 1 {
		errs.add(path, "exactly one of http, tcp, dns, or tls is required")
	}
	return m, len(*errs) == before
}

func resolveHTTP(errs *problems, path string, h check.HTTP, warnDays int) *check.HTTP {
	u, err := url.Parse(h.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs.add(path+".url", "must be an http(s) URL")
	}
	h.Method = strings.ToUpper(h.Method)
	if h.Method == "" {
		h.Method = "GET"
	}
	if h.CertWarnDays == 0 {
		h.CertWarnDays = warnDays
	}
	for _, s := range h.Expect.Status {
		if !validStatusPattern(s) {
			errs.add(path+".expect.status", "invalid status %q (use 200 or 2xx)", s)
		}
	}
	for i, a := range h.Expect.JSON {
		if a.Path == "" {
			errs.add(fmt.Sprintf("%s.expect.json[%d].path", path, i), "required")
		}
	}
	return &h
}

func validStatusPattern(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) == 3 && strings.HasSuffix(s, "xx") {
		return s[0] >= '1' && s[0] <= '5'
	}
	n, err := strconv.Atoi(s)
	return err == nil && n >= 100 && n <= 599
}

func resolveDNS(errs *problems, path string, d check.DNS) *check.DNS {
	if d.Name == "" {
		errs.add(path+".name", "required")
	}
	d.Type = strings.ToUpper(d.Type)
	if d.Type == "" {
		d.Type = "A"
	}
	if !slices.Contains(check.DNSTypes, d.Type) {
		errs.add(path+".type", "must be one of %s", strings.Join(check.DNSTypes, ", "))
	}
	if d.Resolver != "" {
		if _, _, err := net.SplitHostPort(d.Resolver); err != nil {
			d.Resolver = net.JoinHostPort(d.Resolver, "53")
		}
	}
	return &d
}

func parseQuorum(s string, regions int) (int, error) {
	switch s {
	case "majority":
		return regions/2 + 1, nil
	case "all":
		return regions, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("must be majority, all, or a number of regions")
	}
	if n < 1 || n > regions {
		return 0, fmt.Errorf("must be between 1 and the number of regions (%d)", regions)
	}
	return n, nil
}

func durationOr(v, def *Duration, fallback time.Duration) time.Duration {
	if v != nil {
		return time.Duration(*v)
	}
	if def != nil {
		return time.Duration(*def)
	}
	return fallback
}

func intOr(v, def *int, fallback int) int {
	if v != nil {
		return *v
	}
	if def != nil {
		return *def
	}
	return fallback
}

func firstNonEmpty[T any](vs ...[]T) []T {
	for _, v := range vs {
		if len(v) > 0 {
			return v
		}
	}
	return nil
}

func firstString(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func firstPtr[T any](vs ...*T) *T {
	for _, v := range vs {
		if v != nil {
			return v
		}
	}
	return nil
}
