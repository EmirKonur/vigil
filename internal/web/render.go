package web

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"html/template"
	"io/fs"
	"math"
	"net/http"
	"strings"
	"time"

	"vigil/internal/alert"
	"vigil/internal/chart"
	"vigil/internal/consensus"
)

// page is what every template receives.
type page struct {
	Title   string
	Nav     string // active admin nav item
	Path    string // request path and query, for htmx self-refresh
	Demo    bool
	Version string
	AssetV  string
	Now     time.Time
	Data    any
}

type views struct {
	pages   map[string]*template.Template
	version string
	assetV  string
	demo    bool
}

// pageFiles lists each page with the shell it renders in.
var pageFiles = map[string]string{
	"status":          "public",
	"status_history":  "public",
	"status_incident": "public",
	"error":           "public",
	"login":           "public",
	"overview":        "admin",
	"monitor":         "admin",
	"incidents":       "admin",
	"incident":        "admin",
	"probes":          "admin",
}

func loadViews(version string) *views {
	base := template.Must(template.New("").Funcs(funcs).ParseFS(templateFS, "templates/layout.html", "templates/parts.html"))
	v := &views{pages: map[string]*template.Template{}, version: version, assetV: assetHash()}
	for name := range pageFiles {
		t := template.Must(base.Clone())
		v.pages[name] = template.Must(t.ParseFS(templateFS, "templates/"+name+".html"))
	}
	return v
}

// assetHash fingerprints the embedded static files for cache busting.
func assetHash() string {
	h := sha256.New()
	_ = fs.WalkDir(staticFS, "static", func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			b, _ := fs.ReadFile(staticFS, path)
			h.Write([]byte(path))
			h.Write(b)
		}
		return nil
	})
	return hex.EncodeToString(h.Sum(nil))[:10]
}

func (v *views) render(w http.ResponseWriter, r *http.Request, status int, name string, p page) {
	t, ok := v.pages[name]
	if !ok {
		http.Error(w, "unknown page "+name, http.StatusInternalServerError)
		return
	}
	p.Version, p.AssetV, p.Now, p.Path = v.version, v.assetV, time.Now().UTC(), r.URL.RequestURI()
	p.Demo = p.Demo || v.demo
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, pageFiles[name], p); err != nil {
		http.Error(w, "template error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = buf.WriteTo(w)
}

type errorView struct {
	Code    int
	Heading string
	Message string
}

func (v *views) error(w http.ResponseWriter, r *http.Request, code int, heading, msg string) {
	v.render(w, r, code, "error", page{Title: fmt.Sprintf("%d %s", code, heading), Data: errorView{code, heading, msg}})
}

var funcs = template.FuncMap{
	"ago":       ago,
	"dur":       alert.HumanDuration,
	"pct":       chart.Percent,
	"ms":        formatMS,
	"clock":     func(t time.Time) string { return t.UTC().Format("15:04:05") },
	"hm":        func(t time.Time) string { return t.UTC().Format("15:04") },
	"date":      func(t time.Time) string { return t.UTC().Format("Jan 2, 2006") },
	"datetime":  func(t time.Time) string { return t.UTC().Format("Jan 2, 15:04 UTC") },
	"iso":       func(t time.Time) string { return t.UTC().Format(time.RFC3339) },
	"phase":     phaseLabel,
	"impact":    impactLabel,
	"upper":     strings.ToUpper,
	"join":      strings.Join,
	"add":       func(a, b int) int { return a + b },
	"zero":      func(t time.Time) bool { return t.IsZero() },
	"plural":    func(n int, one, many string) string { return map[bool]string{true: one, false: many}[n == 1] },
	"statusTag": statusTag,
	"lamp":      lampClass,
}

// lampClass picks the indicator class for a monitor.
func lampClass(p consensus.Phase, paused bool) string {
	if paused {
		return "paused"
	}
	switch p {
	case consensus.PhaseUp:
		return "up"
	case consensus.PhaseDegraded:
		return "degraded"
	case consensus.PhaseDown:
		return "down"
	}
	return "nodata"
}

func ago(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := time.Since(t)
	if d < 0 {
		d = 0
	}
	switch {
	case d < 5*time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	default:
		return alert.HumanDuration(d) + " ago"
	}
}

func formatMS(v float64) string {
	switch {
	case math.IsNaN(v):
		return "n/a"
	case v >= 10000:
		return fmt.Sprintf("%.1fs", v/1000)
	case v >= 100:
		return fmt.Sprintf("%.0f ms", v)
	default:
		return fmt.Sprintf("%.1f ms", v)
	}
}

func phaseLabel(p consensus.Phase) string {
	switch p {
	case consensus.PhaseUp:
		return "Up"
	case consensus.PhaseDegraded:
		return "Degraded"
	case consensus.PhaseDown:
		return "Down"
	}
	return "No data"
}

func impactLabel(impact string) string {
	if impact == string(consensus.ImpactMajor) {
		return "Major outage"
	}
	return "Partial outage"
}

// statusTag maps an incident update status to its public label.
func statusTag(s string) string {
	switch s {
	case "investigating":
		return "Investigating"
	case "identified":
		return "Identified"
	case "monitoring":
		return "Monitoring"
	case "resolved":
		return "Resolved"
	}
	return "Update"
}
