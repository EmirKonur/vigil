// Package config loads and validates vigil.yaml, the source of truth for
// monitors, notifiers, and status pages.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"vigil/internal/check"
	"vigil/internal/consensus"
)

// Config is a fully resolved configuration: defaults applied, references
// checked, and every monitor carrying a ready-to-run spec and policy.
type Config struct {
	Monitors    []Monitor
	Notifiers   map[string]Notifier
	StatusPages []StatusPage
	Retention   Retention
	// Regions is every region referenced by at least one monitor, sorted.
	Regions []string
}

// Monitor is a resolved monitor definition.
type Monitor struct {
	ID          string
	Name        string
	Description string
	Interval    time.Duration
	Spec        check.Spec
	Policy      consensus.Policy
	Notify      []string
	Escalate    []string
	// CertWarnDays is the warning threshold for certificate expiry alerts,
	// for HTTPS and TLS checks.
	CertWarnDays int
}

// Monitor looks up a monitor by ID.
func (c *Config) Monitor(id string) (Monitor, bool) {
	for _, m := range c.Monitors {
		if m.ID == id {
			return m, true
		}
	}
	return Monitor{}, false
}

// StatusPage looks up a status page by slug.
func (c *Config) StatusPage(slug string) (StatusPage, bool) {
	for _, p := range c.StatusPages {
		if p.Slug == slug {
			return p, true
		}
	}
	return StatusPage{}, false
}

// Notifier is a destination for alerts.
type Notifier struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // webhook or email

	URL     string            `yaml:"url"`
	Preset  string            `yaml:"preset"` // generic, slack, or discord
	Secret  string            `yaml:"secret"` // HMAC-SHA256 signing key for webhooks
	Headers map[string]string `yaml:"headers"`

	To   []string `yaml:"to"`
	SMTP SMTP     `yaml:"smtp"`
}

// SMTP holds mail server settings for an email notifier.
type SMTP struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	From     string `yaml:"from"`
	// TLS is starttls (default), tls (implicit, port 465), or none.
	TLS string `yaml:"tls"`
}

// StatusPage is a public page made of components.
type StatusPage struct {
	Slug        string      `yaml:"slug"`
	Title       string      `yaml:"title"`
	Description string      `yaml:"description"`
	Components  []Component `yaml:"components"`
}

// Component groups monitors under one public name.
type Component struct {
	Name        string   `yaml:"name"`
	Description string   `yaml:"description"`
	Monitors    []string `yaml:"monitors"`
}

// Retention controls how long each resolution of data is kept.
type Retention struct {
	Raw    Duration `yaml:"raw"`
	Hourly Duration `yaml:"hourly"`
	Daily  Duration `yaml:"daily"`
}

// Duration is a time.Duration that also accepts "d" and "w" suffixes,
// because retention and renotify periods are naturally written in days.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*d = Duration(v)
	return nil
}

// ParseDuration parses Go durations plus whole days ("7d") and weeks ("2w").
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" || s == "off" {
		return 0, nil
	}
	for suffix, unit := range map[string]time.Duration{"d": 24 * time.Hour, "w": 7 * 24 * time.Hour} {
		if n, ok := strings.CutSuffix(s, suffix); ok {
			v, err := strconv.Atoi(n)
			if err != nil {
				return 0, fmt.Errorf("invalid duration %q", s)
			}
			return time.Duration(v) * unit, nil
		}
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	return v, nil
}

// file mirrors the YAML layout before defaults are applied.
type file struct {
	Defaults    monitorFields `yaml:"defaults"`
	Notifiers   []Notifier    `yaml:"notifiers"`
	Monitors    []monitorFile `yaml:"monitors"`
	StatusPages []StatusPage  `yaml:"status_pages"`
	Retention   Retention     `yaml:"retention"`
}

// monitorFields are the settings that can appear in defaults and be
// overridden per monitor. Pointers distinguish "unset" from zero.
type monitorFields struct {
	Interval     *Duration `yaml:"interval"`
	Timeout      *Duration `yaml:"timeout"`
	Regions      []string  `yaml:"regions"`
	Quorum       string    `yaml:"quorum"`
	FailAfter    *int      `yaml:"fail_after"`
	RecoverAfter *int      `yaml:"recover_after"`
	Renotify     *Duration `yaml:"renotify"`
	Notify       []string  `yaml:"notify"`
	Escalate     *struct {
		After  Duration `yaml:"after"`
		Notify []string `yaml:"notify"`
	} `yaml:"escalate"`
	Flap *struct {
		Window    Duration `yaml:"window"`
		Threshold int      `yaml:"threshold"`
	} `yaml:"flap"`
	CertWarnDays *int `yaml:"cert_warn_days"`
}

type monitorFile struct {
	ID            string `yaml:"id"`
	Name          string `yaml:"name"`
	Description   string `yaml:"description"`
	monitorFields `yaml:",inline"`

	HTTP *check.HTTP `yaml:"http"`
	TCP  *check.TCP  `yaml:"tcp"`
	DNS  *check.DNS  `yaml:"dns"`
	TLS  *check.TLS  `yaml:"tls"`
}

// Load reads, expands, and validates a config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data, os.LookupEnv)
}

// Parse validates YAML config bytes. lookupEnv resolves ${VAR} references.
func Parse(data []byte, lookupEnv func(string) (string, bool)) (*Config, error) {
	expanded, err := expandEnv(data, lookupEnv)
	if err != nil {
		return nil, err
	}
	var f file
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	// An empty file is a valid (if useless) config.
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return resolve(f)
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

// expandEnv substitutes ${VAR} and ${VAR:-default}. Only the braced form is
// recognized so that a literal "$" in a keyword assertion stays literal, and
// comment lines are left alone so documentation can mention variables.
func expandEnv(data []byte, lookup func(string) (string, bool)) ([]byte, error) {
	var missing []string
	lines := bytes.SplitAfter(data, []byte("\n"))
	for i, line := range lines {
		if bytes.HasPrefix(bytes.TrimSpace(line), []byte("#")) {
			continue
		}
		lines[i] = envRef.ReplaceAllFunc(line, func(m []byte) []byte {
			sub := envRef.FindSubmatch(m)
			name := string(sub[1])
			if v, ok := lookup(name); ok && v != "" {
				return []byte(v)
			}
			if len(sub[2]) > 0 {
				return sub[3]
			}
			if !slices.Contains(missing, name) {
				missing = append(missing, name)
			}
			return nil
		})
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("config references unset environment variables: %s", strings.Join(missing, ", "))
	}
	return bytes.Join(lines, nil), nil
}
