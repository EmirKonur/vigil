// Package chart renders Vigil's charts as inline SVG on the server.
//
// Charts carry classes rather than colors; the stylesheet maps them to the
// theme's tokens, so one SVG works in light and dark mode. Hover readouts
// come from a small script that reads the data-chart attribute, and every
// chart has a text equivalent nearby (a table or a summary label), so the
// script enhances rather than gates.
package chart

import (
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"math"
	"strconv"
	"strings"
	"time"

	"vigil/internal/uptime"
)

// UptimeBars renders one bar per day. label is the accessible summary.
func UptimeBars(days []uptime.Day, label string) template.HTML {
	const barW, gap, h = 6.0, 2.0, 32.0
	w := float64(len(days))*(barW+gap) - gap
	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="bars" viewBox="0 0 %.0f %.0f" preserveAspectRatio="none" role="img" aria-label="%s">`, w, h, html.EscapeString(label))
	for i, d := range days {
		x := float64(i) * (barW + gap)
		tip := d.Date.Format("Mon Jan 2") + ": "
		switch d.Level {
		case uptime.LevelNoData:
			tip += "no data"
		default:
			tip += Percent(d.Ratio)
			if d.Incidents > 0 {
				tip += fmt.Sprintf(", %d incident%s, %s down", d.Incidents, plural(d.Incidents), shortDuration(d.Downtime))
			}
		}
		fmt.Fprintf(&b, `<rect class="bar %s" x="%.1f" y="0" width="%.1f" height="%.0f" rx="1" data-tip="%s"><title>%s</title></rect>`,
			d.Level, x, barW, h, html.EscapeString(tip), html.EscapeString(tip))
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

// Percent formats an availability ratio the way status pages do: enough
// decimals to distinguish 99.9 from 99.99, never rounding up to 100 while
// there was any downtime.
func Percent(r float64) string {
	if r >= 1 {
		return "100%"
	}
	p := math.Floor(r*10000) / 100
	return strings.TrimSuffix(strings.TrimSuffix(fmt.Sprintf("%.2f", p), "0"), ".0") + "%"
}

// Point is one bucket of one series. NaN means no data.
type Point struct {
	P50, P95 float64
	Failed   bool // at least one failed check in the bucket
	Checks   int
}

// Series is one region's line.
type Series struct {
	Name   string
	Slot   int // categorical color slot, stable per region across all charts
	Points []Point
}

// Span marks an incident on the time axis.
type Span struct {
	From, To time.Time
	Major    bool
}

// Latency describes a latency-over-time chart.
type Latency struct {
	Start   time.Time // first bucket start
	Step    time.Duration
	Buckets int
	Series  []Series
	Spans   []Span
	Label   string
}

// palette slots that have validated colors; beyond these a dash pattern is
// added so identity never rests on hue alone.
const slots = 3

const (
	chartW     = 960.0
	padL       = 52.0
	padR       = 92.0
	padT       = 26.0
	plotH      = 220.0
	axisH      = 26.0
	laneH      = 13.0
	lanesGap   = 10.0
	labelSpace = 13.0 // minimum vertical distance between end labels
)

// LatencyChart renders per-region p50 lines with incident bands and a lane
// per region marking buckets that contained failed checks.
func LatencyChart(c Latency) template.HTML {
	plotR := chartW - padR
	lanesTop := padT + plotH + axisH + lanesGap
	h := lanesTop + float64(len(c.Series))*laneH + 4
	end := c.Start.Add(time.Duration(c.Buckets) * c.Step)

	maxV := 0.0
	for _, s := range c.Series {
		for _, p := range s.Points {
			if !math.IsNaN(p.P50) {
				maxV = math.Max(maxV, p.P50)
			}
		}
	}
	ticks, top := niceTicks(maxV)
	x := func(t time.Time) float64 {
		return padL + (plotR-padL)*float64(t.Sub(c.Start))/float64(end.Sub(c.Start))
	}
	bx := func(i int) float64 { return x(c.Start.Add(time.Duration(i)*c.Step + c.Step/2)) }
	y := func(v float64) float64 { return padT + plotH - plotH*v/top }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="chart" viewBox="0 0 %.0f %.0f" role="img" aria-label="%s">`, chartW, h, html.EscapeString(c.Label))

	// Incident bands sit under everything else.
	for _, sp := range c.Spans {
		x0, x1 := math.Max(x(sp.From), padL), math.Min(x(sp.To), plotR)
		if x1 <= x0 {
			continue
		}
		x1 = math.Max(x1, x0+2)
		cls := "inc"
		if sp.Major {
			cls += " major"
		}
		fmt.Fprintf(&b, `<rect class="%s" x="%.1f" y="%.1f" width="%.1f" height="%.1f"/>`, cls, x0, padT, x1-x0, plotH)
		fmt.Fprintf(&b, `<rect class="inc-cap" x="%.1f" y="%.1f" width="%.1f" height="3"/>`, x0, padT, x1-x0)
	}

	// Grid and y labels.
	for _, v := range ticks {
		fmt.Fprintf(&b, `<line class="grid" x1="%.0f" x2="%.0f" y1="%.1f" y2="%.1f"/>`, padL, plotR, y(v), y(v))
		fmt.Fprintf(&b, `<text class="tick" x="%.0f" y="%.1f" text-anchor="end">%s</text>`, padL-8, y(v)+4, tickLabel(v))
	}
	fmt.Fprintf(&b, `<text class="unit" x="%.0f" y="%.0f" text-anchor="end">ms</text>`, padL-8, padT-14)

	// Time axis.
	step, layout := timeStep(end.Sub(c.Start))
	for t := c.Start.Truncate(step).Add(step); t.Before(end); t = t.Add(step) {
		tx := x(t)
		fmt.Fprintf(&b, `<line class="axis-tick" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/>`, tx, tx, padT+plotH, padT+plotH+5)
		fmt.Fprintf(&b, `<text class="tick" x="%.1f" y="%.1f" text-anchor="middle">%s</text>`, tx, padT+plotH+18, t.UTC().Format(layout))
	}
	fmt.Fprintf(&b, `<line class="axis" x1="%.0f" x2="%.0f" y1="%.1f" y2="%.1f"/>`, padL, plotR, padT+plotH, padT+plotH)

	// Lines, broken at gaps so missing data never looks like a straight ramp.
	type endLabel struct {
		y    float64
		name string
		cls  string
	}
	var labels []endLabel
	for _, s := range c.Series {
		cls := seriesClass(s.Slot)
		var d strings.Builder
		pen := false
		lastY := math.NaN()
		for i, p := range s.Points {
			if math.IsNaN(p.P50) {
				pen = false
				continue
			}
			cmd := "L"
			if !pen {
				cmd = "M"
			}
			fmt.Fprintf(&d, "%s%.1f %.1f", cmd, bx(i), y(p.P50))
			pen, lastY = true, y(p.P50)
		}
		if d.Len() > 0 {
			fmt.Fprintf(&b, `<path class="line %s" d="%s"/>`, cls, d.String())
		}
		if !math.IsNaN(lastY) {
			labels = append(labels, endLabel{lastY, s.Name, cls})
		}
	}
	// Direct labels only when they cannot collide; otherwise the legend
	// above the chart carries identity alone.
	collide := false
	for i := range labels {
		for j := i + 1; j < len(labels); j++ {
			if math.Abs(labels[i].y-labels[j].y) < labelSpace {
				collide = true
			}
		}
	}
	if !collide {
		for _, l := range labels {
			fmt.Fprintf(&b, `<line class="key %s" x1="%.0f" x2="%.0f" y1="%.1f" y2="%.1f"/>`, l.cls, plotR+6, plotR+16, l.y, l.y)
			fmt.Fprintf(&b, `<text class="end-label" x="%.0f" y="%.1f">%s</text>`, plotR+20, l.y+4, html.EscapeString(l.name))
		}
	}

	// Failure lanes.
	bw := math.Max((plotR-padL)/float64(max(c.Buckets, 1))-1, 1.5)
	for li, s := range c.Series {
		ly := lanesTop + float64(li)*laneH
		fmt.Fprintf(&b, `<text class="lane-label" x="%.0f" y="%.1f" text-anchor="end">%s</text>`, padL-8, ly+8, html.EscapeString(s.Name))
		fmt.Fprintf(&b, `<line class="lane" x1="%.0f" x2="%.0f" y1="%.1f" y2="%.1f"/>`, padL, plotR, ly+9.5, ly+9.5)
		for i, p := range s.Points {
			if p.Failed {
				fmt.Fprintf(&b, `<rect class="fail" x="%.1f" y="%.1f" width="%.1f" height="8"/>`, bx(i)-bw/2, ly+1, bw)
			}
		}
	}
	fmt.Fprintf(&b, `<line class="xhair" x1="0" x2="0" y1="%.0f" y2="%.1f" visibility="hidden"/>`, padT, lanesTop+float64(len(c.Series))*laneH)
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

// HoverData is the JSON the hover script reads from data-chart.
func HoverData(c Latency) string {
	type series struct {
		Name string     `json:"name"`
		Slot int        `json:"slot"`
		P50  []*float64 `json:"p50"`
		P95  []*float64 `json:"p95"`
		Fail []bool     `json:"fail"`
	}
	out := struct {
		Start  int64    `json:"start"`
		Step   int64    `json:"step"`
		X0     float64  `json:"x0"`
		X1     float64  `json:"x1"`
		W      float64  `json:"w"`
		Series []series `json:"series"`
	}{c.Start.UnixMilli(), c.Step.Milliseconds(), padL, chartW - padR, chartW, nil}
	for _, s := range c.Series {
		js := series{Name: s.Name, Slot: s.Slot}
		for _, p := range s.Points {
			js.P50 = append(js.P50, num(p.P50))
			js.P95 = append(js.P95, num(p.P95))
			js.Fail = append(js.Fail, p.Failed)
		}
		out.Series = append(out.Series, js)
	}
	b, _ := json.Marshal(out)
	return string(b)
}

func num(v float64) *float64 {
	if math.IsNaN(v) {
		return nil
	}
	r := math.Round(v*10) / 10
	return &r
}

// Sparkline renders a small single-series trend with failure ticks.
func Sparkline(values []float64, failed []bool, label string) template.HTML {
	const w, h, base = 120.0, 28.0, 22.0
	n := len(values)
	if n == 0 {
		return ""
	}
	maxV := 0.0
	for _, v := range values {
		if !math.IsNaN(v) {
			maxV = math.Max(maxV, v)
		}
	}
	if maxV == 0 {
		maxV = 1
	}
	step := w / float64(n)
	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="spark" viewBox="0 0 %.0f %.0f" role="img" aria-label="%s">`, w, h, html.EscapeString(label))
	var d strings.Builder
	pen := false
	for i, v := range values {
		if math.IsNaN(v) {
			pen = false
			continue
		}
		cmd := "L"
		if !pen {
			cmd = "M"
		}
		fmt.Fprintf(&d, "%s%.1f %.1f", cmd, step*(float64(i)+0.5), base-2-(base-4)*v/maxV)
		pen = true
	}
	if d.Len() > 0 {
		fmt.Fprintf(&b, `<path class="spark-line" d="%s"/>`, d.String())
	}
	for i, f := range failed {
		if f {
			fmt.Fprintf(&b, `<rect class="fail" x="%.1f" y="%.0f" width="%.1f" height="4"/>`, step*float64(i), base+2, math.Max(step-0.5, 1))
		}
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func seriesClass(slot int) string {
	cls := fmt.Sprintf("s%d", slot%slots)
	if slot >= slots {
		cls += " dashed"
	}
	return cls
}

// niceTicks returns gridline values and the axis top for data up to maxV:
// the smallest round step (1, 2, 2.5, or 5 times a power of ten) that covers
// the data with at most five intervals and a little headroom.
func niceTicks(maxV float64) ([]float64, float64) {
	if maxV <= 0 || math.IsNaN(maxV) {
		maxV = 100
	}
	target := maxV * 1.05
	mag := math.Pow(10, math.Floor(math.Log10(maxV/5)))
	step := 10 * mag
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if math.Ceil(target/(m*mag)) <= 5 {
			step = m * mag
			break
		}
	}
	n := int(math.Ceil(target / step))
	ticks := make([]float64, n+1)
	for i := range ticks {
		ticks[i] = float64(i) * step
	}
	return ticks, ticks[n]
}

// tickLabel prints milliseconds in full with thousands separators, so the
// axis keeps one unit however slow the data gets.
func tickLabel(v float64) string {
	n := int64(math.Round(v))
	if math.Abs(v-float64(n)) > 0.05 {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func timeStep(span time.Duration) (time.Duration, string) {
	for _, s := range []time.Duration{10 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 3 * time.Hour, 6 * time.Hour} {
		if span/s <= 8 {
			return s, "15:04"
		}
	}
	for _, s := range []time.Duration{24 * time.Hour, 2 * 24 * time.Hour, 5 * 24 * time.Hour, 7 * 24 * time.Hour} {
		if span/s <= 8 {
			return s, "Jan 2"
		}
	}
	return 30 * 24 * time.Hour, "Jan 2"
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func shortDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	}
}
