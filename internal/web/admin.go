package web

import (
	"errors"
	"html/template"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"vigil/internal/chart"
	"vigil/internal/config"
	"vigil/internal/consensus"
	"vigil/internal/engine"
	"vigil/internal/lease"
	"vigil/internal/store"
)

// Probe health thresholds. Probes long-poll every 20 seconds at most, so a
// minute of silence means something is wrong.
const (
	probeStaleAfter   = time.Minute
	probeOfflineAfter = 5 * time.Minute
)

type counts struct{ Total, Up, Degraded, Down, Paused, Unknown int }

type overviewView struct {
	Healthy  int
	Counts   counts
	Monitors []monitorRow
	Open     []adminIncident
	Probes   []probeRow
	Missing  []string
	Queue    lease.Stats
	Config   ConfigStatus
	Flash    string
}

type monitorRow struct {
	ID, Name, Type, Target string
	Phase                  consensus.Phase
	Paused, Flapping       bool
	Regions                []regionLamp
	P50                    float64
	Uptime                 string
	Spark                  template.HTML
}

type regionLamp struct {
	Name  string
	Class string
	Label string // short state, for places that already show the region
	Tip   string
}

type adminIncident struct {
	store.Incident
	Monitor string
	Now     time.Time
}

type probeRow struct {
	store.Probe
	Health string
}

func (h *Handler) overview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := h.opts.Clock.Now()
	statuses := h.opts.Engine.Statuses()
	rollups, err := h.opts.Store.Rollups(ctx, store.Hour, nil, now.Add(-24*time.Hour), now.Add(time.Hour))
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	incidents, err := h.opts.Store.IncidentsBetween(ctx, nil, now.Add(-24*time.Hour), now)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	latest, err := h.opts.Store.LatestPerRegion(ctx, now.Add(-time.Hour))
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	stored, err := h.opts.Store.Monitors(ctx)
	if err != nil {
		h.serverError(w, r, err)
		return
	}

	rollupsBy := map[string][]store.Rollup{}
	for _, ru := range rollups {
		rollupsBy[ru.MonitorID] = append(rollupsBy[ru.MonitorID], ru)
	}
	incidentsBy := byMonitor(incidents)
	v := overviewView{Queue: h.opts.Engine.QueueStats(), Config: h.opts.Config(), Flash: r.URL.Query().Get("flash")}
	names := map[string]string{}
	for _, s := range statuses {
		m := s.Monitor
		names[m.ID] = m.Name
		row := monitorRow{
			ID: m.ID, Name: m.Name, Type: strings.ToUpper(string(m.Spec.Type)), Target: m.Spec.Target(),
			Phase: s.State.Phase, Paused: s.Paused, Flapping: s.State.Flapping,
			Uptime: chart.Percent(availability(incidentsBy[m.ID], stored[m.ID].CreatedAt, 24*time.Hour, now)),
		}
		v.Counts.Total++
		switch {
		case s.Paused:
			v.Counts.Paused++
		case s.State.Phase == consensus.PhaseUp:
			v.Counts.Up++
		case s.State.Phase == consensus.PhaseDegraded:
			v.Counts.Degraded++
		case s.State.Phase == consensus.PhaseDown:
			v.Counts.Down++
		default:
			v.Counts.Unknown++
		}
		for _, region := range m.Policy.Regions {
			row.Regions = append(row.Regions, lampFor(region, s.State.Regions[region], latest[m.ID][region], m.Policy.StaleAfter, now))
		}
		values, failed := hourlySpark(rollupsBy[m.ID], now)
		row.Spark = chart.Sparkline(values, failed, m.Name+": median latency per hour, last 24 hours")
		row.P50 = math.NaN()
		if p, ok := mergeRollups(rollupsBy[m.ID], func(store.Rollup) string { return "" })[""]; ok {
			row.P50 = p.P50
		}
		v.Monitors = append(v.Monitors, row)
	}

	open, err := h.opts.Store.OpenIncidents(ctx)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	for _, inc := range open {
		v.Open = append(v.Open, adminIncident{Incident: inc, Monitor: nameOr(names, inc.MonitorID), Now: now})
	}
	v.Probes, v.Missing, err = h.probeHealth(r, now)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	for _, p := range v.Probes {
		if p.Health == "ok" {
			v.Healthy++
		}
	}
	h.views.render(w, r, http.StatusOK, "overview", page{Title: "Overview", Nav: "overview", Data: v})
}

func nameOr(names map[string]string, id string) string {
	if n, ok := names[id]; ok {
		return n
	}
	return id
}

func lampFor(region string, rs *consensus.RegionState, last store.Result, staleAfter time.Duration, now time.Time) regionLamp {
	l := regionLamp{Name: region, Class: "nodata", Label: "No results yet"}
	switch {
	case rs == nil || rs.LastSeen.IsZero():
	case now.Sub(rs.LastSeen) > staleAfter:
		l.Class, l.Label = "stale", "Stale, last result "+ago(rs.LastSeen)
	case rs.Failing:
		l.Class, l.Label = "down", "Failing, "+strconv.Itoa(rs.ConsecutiveFail)+" in a row"
	case rs.ConsecutiveFail > 0:
		l.Class, l.Label = "degraded", strconv.Itoa(rs.ConsecutiveFail)+" failed, below threshold"
	default:
		l.Class, l.Label = "up", "Up"
		if last.ID != "" && last.OK {
			l.Label += ", " + formatMS(last.LatencyMS)
		}
	}
	l.Tip = region + ": " + l.Label
	if last.ID != "" && !last.OK && last.Error != "" {
		l.Tip += " (" + last.Error + ")"
	}
	return l
}

func (h *Handler) probeHealth(r *http.Request, now time.Time) ([]probeRow, []string, error) {
	probes, err := h.opts.Store.Probes(r.Context(), now.Add(-7*24*time.Hour))
	if err != nil {
		return nil, nil, err
	}
	healthy := map[string]bool{}
	var rows []probeRow
	for _, p := range probes {
		row := probeRow{Probe: p, Health: "ok"}
		switch age := now.Sub(p.LastSeen); {
		case age > probeOfflineAfter:
			row.Health = "offline"
		case age > probeStaleAfter:
			row.Health = "stale"
		default:
			healthy[p.Region] = true
		}
		rows = append(rows, row)
	}
	var missing []string
	if cfg := h.opts.Engine.Config(); cfg != nil {
		for _, region := range cfg.Regions {
			if !healthy[region] {
				missing = append(missing, region)
			}
		}
	}
	return rows, missing, nil
}

type monitorView struct {
	Status        engine.MonitorStatus
	Phase         consensus.Phase
	Range         timeRange
	Ranges        []timeRange
	Chart         template.HTML
	ChartData     string
	Legend        []chart.Series
	Table         []tableRow
	Regions       []regionCard
	Uptime        []uptimeReadout
	P50, P95      float64
	Quorum        int
	Incidents     []adminIncident
	Results       []store.Result
	Notifications []store.Notification
	Slots         map[string]int
}

type regionCard struct {
	Name    string
	Slot    int
	Lamp    regionLamp
	State   *consensus.RegionState
	Last    store.Result
	P50     float64
	P95     float64
	Success string
}

type uptimeReadout struct {
	Label, Value string
}

type tableRow struct {
	At     time.Time
	Values []float64
}

func (h *Handler) monitor(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := h.opts.Clock.Now()
	s, ok := h.opts.Engine.Status(r.PathValue("id"))
	if !ok {
		h.notFound(w, r)
		return
	}
	m := s.Monitor
	cfg := h.opts.Engine.Config()
	slots := regionSlots(cfg)
	rng := findRange(r.URL.Query().Get("range"))

	c, err := h.latencyChart(ctx, m, rng, slots, now)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	v := monitorView{
		Status: s, Phase: s.State.Phase, Range: rng, Ranges: ranges, Slots: slots,
		Chart: chart.LatencyChart(c), ChartData: chart.HoverData(c), Legend: c.Series,
		Quorum: m.Policy.Quorum,
	}
	for i := range c.Buckets {
		row := tableRow{At: c.Start.Add(time.Duration(i) * c.Step)}
		for _, se := range c.Series {
			row.Values = append(row.Values, se.Points[i].P50)
		}
		v.Table = append(v.Table, row)
	}

	stored, err := h.opts.Store.Monitors(ctx)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	since := stored[m.ID].CreatedAt
	incidents, err := h.opts.Store.IncidentsBetween(ctx, []string{m.ID}, now.AddDate(0, 0, -90), now)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	for _, u := range []struct {
		label string
		span  time.Duration
	}{{"24 hours", 24 * time.Hour}, {"7 days", 7 * 24 * time.Hour}, {"30 days", 30 * 24 * time.Hour}, {"90 days", 90 * 24 * time.Hour}} {
		v.Uptime = append(v.Uptime, uptimeReadout{u.label, chart.Percent(availability(incidents, since, u.span, now))})
	}
	for _, inc := range incidents {
		v.Incidents = append(v.Incidents, adminIncident{Incident: inc, Monitor: m.Name, Now: now})
	}

	rollups, err := h.opts.Store.Rollups(ctx, store.Hour, []string{m.ID}, now.Add(-24*time.Hour), now.Add(time.Hour))
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	all := mergeRollups(rollups, func(store.Rollup) string { return "" })[""]
	v.P50, v.P95 = nan(), nan()
	if all.OK > 0 {
		v.P50, v.P95 = all.P50, all.P95
	}
	perRegion := mergeRollups(rollups, func(r store.Rollup) string { return r.Region })
	latest, err := h.opts.Store.LatestPerRegion(ctx, now.Add(-24*time.Hour))
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	for _, region := range m.Policy.Regions {
		card := regionCard{Name: region, Slot: slots[region], State: s.State.Regions[region], Last: latest[m.ID][region], P50: nan(), P95: nan(), Success: "n/a"}
		card.Lamp = lampFor(region, card.State, card.Last, m.Policy.StaleAfter, now)
		if p, ok := perRegion[region]; ok && p.Checks > 0 {
			card.Success = chart.Percent(float64(p.OK) / float64(p.Checks))
			if p.OK > 0 {
				card.P50, card.P95 = p.P50, p.P95
			}
		}
		v.Regions = append(v.Regions, card)
	}

	if v.Results, err = h.opts.Store.LatestResults(ctx, m.ID, 6*len(m.Policy.Regions)); err != nil {
		h.serverError(w, r, err)
		return
	}
	if v.Notifications, err = h.opts.Store.RecentNotifications(ctx, m.ID, 12); err != nil {
		h.serverError(w, r, err)
		return
	}
	h.views.render(w, r, http.StatusOK, "monitor", page{Title: m.Name, Nav: "overview", Data: v})
}

func (h *Handler) pause(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if err := h.opts.Engine.SetPaused(r.Context(), id, paused); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				h.notFound(w, r)
				return
			}
			h.serverError(w, r, err)
			return
		}
		http.Redirect(w, r, safeNext(r.FormValue("next")), http.StatusSeeOther)
	}
}

type incidentsView struct {
	Open     []adminIncident
	Resolved []adminIncident
}

func (h *Handler) incidents(w http.ResponseWriter, r *http.Request) {
	now := h.opts.Clock.Now()
	incidents, err := h.opts.Store.IncidentsBetween(r.Context(), nil, now.AddDate(0, 0, -90), now)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	names := monitorNames(h.opts.Engine.Config())
	var v incidentsView
	for _, inc := range incidents {
		ai := adminIncident{Incident: inc, Monitor: nameOr(names, inc.MonitorID), Now: now}
		if inc.Open() {
			v.Open = append(v.Open, ai)
		} else {
			v.Resolved = append(v.Resolved, ai)
		}
	}
	h.views.render(w, r, http.StatusOK, "incidents", page{Title: "Incidents", Nav: "incidents", Data: v})
}

func monitorNames(cfg *config.Config) map[string]string {
	out := map[string]string{}
	if cfg != nil {
		for _, m := range cfg.Monitors {
			out[m.ID] = m.Name
		}
	}
	return out
}

type incidentDetailView struct {
	Incident      adminIncident
	Updates       []store.Update
	Notifications []store.Notification
	Statuses      []string
	Error         string
}

var operatorStatuses = []string{"investigating", "identified", "monitoring", "update"}

func (h *Handler) incident(w http.ResponseWriter, r *http.Request) {
	h.renderIncident(w, r, http.StatusOK, "")
}

func (h *Handler) renderIncident(w http.ResponseWriter, r *http.Request, status int, formErr string) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.notFound(w, r)
		return
	}
	inc, err := h.opts.Store.Incident(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		h.notFound(w, r)
		return
	}
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	notes, err := h.opts.Store.NotificationsForIncident(r.Context(), id)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	updates := slices.Clone(inc.Updates)
	slices.Reverse(updates)
	v := incidentDetailView{
		Incident:      adminIncident{Incident: inc, Monitor: nameOr(monitorNames(h.opts.Engine.Config()), inc.MonitorID), Now: h.opts.Clock.Now()},
		Updates:       updates,
		Notifications: notes,
		Statuses:      operatorStatuses,
		Error:         formErr,
	}
	h.views.render(w, r, status, "incident", page{Title: "Incident #" + strconv.FormatInt(id, 10), Nav: "incidents", Data: v})
}

func (h *Handler) postUpdate(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.notFound(w, r)
		return
	}
	status := r.FormValue("status")
	body := strings.TrimSpace(r.FormValue("body"))
	switch {
	case !slices.Contains(operatorStatuses, status):
		h.renderIncident(w, r, http.StatusUnprocessableEntity, "Pick a status for the update.")
		return
	case body == "" || len(body) > 2000:
		h.renderIncident(w, r, http.StatusUnprocessableEntity, "Write a message of up to 2000 characters.")
		return
	}
	if err := h.opts.Store.AddOperatorUpdate(r.Context(), id, status, body, h.opts.Clock.Now()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			h.notFound(w, r)
			return
		}
		h.serverError(w, r, err)
		return
	}
	h.opts.Hub.Publish(engine.TopicIncidents)
	http.Redirect(w, r, "/admin/incidents/"+strconv.FormatInt(id, 10), http.StatusSeeOther)
}

type probesView struct {
	Probes  []probeRow
	Missing []string
	Regions []string
	Queue   lease.Stats
}

func (h *Handler) probes(w http.ResponseWriter, r *http.Request) {
	now := h.opts.Clock.Now()
	rows, missing, err := h.probeHealth(r, now)
	if err != nil {
		h.serverError(w, r, err)
		return
	}
	v := probesView{Probes: rows, Missing: missing, Queue: h.opts.Engine.QueueStats()}
	if cfg := h.opts.Engine.Config(); cfg != nil {
		v.Regions = cfg.Regions
	}
	h.views.render(w, r, http.StatusOK, "probes", page{Title: "Probes", Nav: "probes", Data: v})
}

func (h *Handler) reload(w http.ResponseWriter, r *http.Request) {
	flash := "Configuration reloaded."
	if err := h.opts.Reload(r.Context()); err != nil {
		flash = "Reload failed: " + firstLine(err.Error()) + ". The previous configuration is still active."
	}
	http.Redirect(w, r, "/admin?flash="+url.QueryEscape(flash), http.StatusSeeOther)
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	return strings.TrimSuffix(s, ":")
}
