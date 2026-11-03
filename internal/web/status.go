package web

import (
	"context"
	"html/template"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"vigil/internal/chart"
	"vigil/internal/config"
	"vigil/internal/consensus"
	"vigil/internal/engine"
	"vigil/internal/store"
	"vigil/internal/uptime"
)

// Component levels, best to worst. The order matters: a component takes
// the worst level of its monitors and the page takes the worst component.
const (
	levelOperational = iota
	levelNoData
	levelMaintenance
	levelDegraded
	levelPartial
	levelMajor
)

var levelInfo = map[int]struct{ Class, Label, Banner string }{
	levelOperational: {"up", "Operational", "All systems operational"},
	levelNoData:      {"nodata", "No data yet", "Waiting for the first checks"},
	levelMaintenance: {"paused", "Paused", "Some checks are paused"},
	levelDegraded:    {"degraded", "Degraded", "Some systems are degraded"},
	levelPartial:     {"partial", "Partial outage", "Partial outage"},
	levelMajor:       {"major", "Major outage", "Major outage"},
}

type statusView struct {
	Page       config.StatusPage
	Level      int
	Class      string
	Banner     string
	Affected   []string
	Active     []incidentView
	Components []componentView
	Regions    []string
	Latency    []latencyRow
	Past       []dayView
	Updated    time.Time
}

type componentView struct {
	Name        string
	Description string
	Class       string
	Label       string
	Bars        template.HTML
	Uptime      string
}

type latencyRow struct {
	Component string
	Cells     []latencyCell
}

type latencyCell struct {
	P50, P95 float64
	Has      bool
}

type incidentView struct {
	ID       int64
	Title    string
	Impact   string
	Open     bool
	Flapping bool
	Opened   time.Time
	Resolved time.Time
	Duration time.Duration
	Updates  []store.Update // newest first
}

type dayView struct {
	Date      time.Time
	Incidents []incidentView
}

func (h *Handler) statusPageConfig(w http.ResponseWriter, r *http.Request) (config.StatusPage, bool) {
	cfg := h.opts.Engine.Config()
	if cfg == nil {
		h.notFound(w, r)
		return config.StatusPage{}, false
	}
	sp, ok := cfg.StatusPage(r.PathValue("slug"))
	if !ok {
		h.notFound(w, r)
	}
	return sp, ok
}

func pageMonitors(sp config.StatusPage) []string {
	var ids []string
	for _, c := range sp.Components {
		ids = append(ids, c.Monitors...)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// componentsOf maps monitor IDs to the public component names that include
// them, so incidents are titled with names the public recognizes.
func componentsOf(sp config.StatusPage) map[string][]string {
	out := map[string][]string{}
	for _, c := range sp.Components {
		for _, id := range c.Monitors {
			out[id] = append(out[id], c.Name)
		}
	}
	return out
}

func (h *Handler) statusPage(w http.ResponseWriter, r *http.Request) {
	sp, ok := h.statusPageConfig(w, r)
	if !ok {
		return
	}
	v, err := h.buildStatus(r.Context(), sp, h.opts.Clock.Now())
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=15")
	h.views.render(w, r, http.StatusOK, "status", page{Title: sp.Title, Data: v})
}

func (h *Handler) buildStatus(ctx context.Context, sp config.StatusPage, now time.Time) (statusView, error) {
	cfg := h.opts.Engine.Config()
	ids := pageMonitors(sp)
	names := componentsOf(sp)
	statuses := map[string]engine.MonitorStatus{}
	for _, s := range h.opts.Engine.Statuses() {
		statuses[s.Monitor.ID] = s
	}
	stored, err := h.opts.Store.Monitors(ctx)
	if err != nil {
		return statusView{}, err
	}
	incidents, err := h.opts.Store.IncidentsBetween(ctx, ids, now.AddDate(0, 0, -90), now)
	if err != nil {
		return statusView{}, err
	}
	perMonitor := byMonitor(incidents)
	rollups, err := h.opts.Store.Rollups(ctx, store.Hour, ids, now.Add(-24*time.Hour), now.Add(time.Hour))
	if err != nil {
		return statusView{}, err
	}

	v := statusView{Page: sp, Updated: now}
	regionSet := map[string]bool{}
	for _, id := range ids {
		if s, ok := statuses[id]; ok {
			for _, r := range s.Monitor.Policy.Regions {
				regionSet[r] = true
			}
		}
	}
	for _, r := range cfg.Regions {
		if regionSet[r] {
			v.Regions = append(v.Regions, r)
		}
	}

	for _, c := range sp.Components {
		var spans []uptime.Interval
		var since []time.Time
		level := levelOperational
		for _, id := range c.Monitors {
			spans = append(spans, uptime.Intervals(perMonitor[id], now)...)
			since = append(since, stored[id].CreatedAt)
			level = max(level, monitorLevel(statuses[id]))
		}
		start := earliest(since...)
		days := uptime.Days(spans, start, 90, now)
		ratio := uptime.Ratio(spans, start, now.AddDate(0, 0, -90), now)
		info := levelInfo[level]
		v.Components = append(v.Components, componentView{
			Name: c.Name, Description: c.Description, Class: info.Class, Label: info.Label,
			Bars:   chart.UptimeBars(days, c.Name+": "+chart.Percent(ratio)+" uptime over the last 90 days"),
			Uptime: chart.Percent(ratio),
		})
		if level >= levelDegraded {
			v.Affected = append(v.Affected, c.Name)
		}
		v.Level = max(v.Level, level)

		perRegion := mergeRollups(slices.DeleteFunc(slices.Clone(rollups), func(r store.Rollup) bool {
			return !slices.Contains(c.Monitors, r.MonitorID)
		}), func(r store.Rollup) string { return r.Region })
		row := latencyRow{Component: c.Name}
		for _, region := range v.Regions {
			p, ok := perRegion[region]
			row.Cells = append(row.Cells, latencyCell{P50: p.P50, P95: p.P95, Has: ok && p.OK > 0})
		}
		v.Latency = append(v.Latency, row)
	}
	info := levelInfo[v.Level]
	v.Class, v.Banner = info.Class, info.Banner

	byDay := map[string]*dayView{}
	for _, inc := range incidents {
		iv := publicIncident(inc, names[inc.MonitorID], now)
		if inc.Open() {
			v.Active = append(v.Active, iv)
			continue
		}
		key := inc.OpenedAt.UTC().Format("2006-01-02")
		if byDay[key] == nil {
			byDay[key] = &dayView{}
		}
		byDay[key].Incidents = append(byDay[key].Incidents, iv)
	}
	for i := range 14 {
		day := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -i)
		d := dayView{Date: day}
		if got := byDay[day.Format("2006-01-02")]; got != nil {
			d.Incidents = got.Incidents
		}
		v.Past = append(v.Past, d)
	}
	return v, nil
}

func monitorLevel(s engine.MonitorStatus) int {
	if s.State == nil {
		return levelNoData
	}
	if s.Paused {
		return levelMaintenance
	}
	switch s.State.Phase {
	case consensus.PhaseDown:
		if s.State.Incident != nil && s.State.Incident.Impact == consensus.ImpactMajor {
			return levelMajor
		}
		return levelPartial
	case consensus.PhaseDegraded:
		return levelDegraded
	case consensus.PhaseUp:
		return levelOperational
	}
	return levelNoData
}

func publicIncident(inc store.Incident, components []string, now time.Time) incidentView {
	name := strings.Join(components, ", ")
	if name == "" {
		name = inc.MonitorID
	}
	updates := slices.Clone(inc.Updates)
	slices.Reverse(updates)
	return incidentView{
		ID: inc.ID, Title: name + ": " + strings.ToLower(impactLabel(inc.Impact)), Impact: inc.Impact,
		Open: inc.Open(), Flapping: inc.Flapping, Opened: inc.OpenedAt, Resolved: inc.ResolvedAt,
		Duration: inc.Duration(now), Updates: updates,
	}
}

type historyView struct {
	Page   config.StatusPage
	Months []monthView
}

type monthView struct {
	Month     time.Time
	Incidents []incidentView
}

func (h *Handler) statusHistory(w http.ResponseWriter, r *http.Request) {
	sp, ok := h.statusPageConfig(w, r)
	if !ok {
		return
	}
	now := h.opts.Clock.Now()
	incidents, err := h.opts.Store.IncidentsBetween(r.Context(), pageMonitors(sp), now.AddDate(0, 0, -365), now)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	names := componentsOf(sp)
	v := historyView{Page: sp}
	for _, inc := range incidents {
		month := time.Date(inc.OpenedAt.Year(), inc.OpenedAt.Month(), 1, 0, 0, 0, 0, time.UTC)
		if len(v.Months) == 0 || !v.Months[len(v.Months)-1].Month.Equal(month) {
			v.Months = append(v.Months, monthView{Month: month})
		}
		last := &v.Months[len(v.Months)-1]
		last.Incidents = append(last.Incidents, publicIncident(inc, names[inc.MonitorID], now))
	}
	h.views.render(w, r, http.StatusOK, "status_history", page{Title: "Incident history · " + sp.Title, Data: v})
}

type incidentPageView struct {
	Page     config.StatusPage
	Incident incidentView
}

func (h *Handler) statusIncident(w http.ResponseWriter, r *http.Request) {
	sp, ok := h.statusPageConfig(w, r)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.notFound(w, r)
		return
	}
	inc, err := h.opts.Store.Incident(r.Context(), id)
	if err != nil || !slices.Contains(pageMonitors(sp), inc.MonitorID) {
		// Incidents of monitors not on this page are not public here.
		h.notFound(w, r)
		return
	}
	iv := publicIncident(inc, componentsOf(sp)[inc.MonitorID], h.opts.Clock.Now())
	h.views.render(w, r, http.StatusOK, "status_incident", page{Title: iv.Title + " · " + sp.Title, Data: incidentPageView{Page: sp, Incident: iv}})
}
