package chart

import (
	"encoding/json"
	"encoding/xml"
	"math"
	"strings"
	"testing"
	"time"

	"vigil/internal/uptime"
)

func wellFormed(t *testing.T, svg string) {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(svg))
	for {
		if _, err := dec.Token(); err != nil {
			if err.Error() == "EOF" {
				return
			}
			t.Fatalf("invalid SVG: %v\n%s", err, svg[:min(len(svg), 400)])
		}
	}
}

func TestPercentNeverRoundsUpToPerfect(t *testing.T) {
	cases := map[float64]string{1: "100%", 0.99999: "99.99%", 0.9995: "99.95%", 0.999: "99.9%", 0.95: "95%", 0.5: "50%"}
	for r, want := range cases {
		if got := Percent(r); got != want {
			t.Errorf("Percent(%v) = %q, want %q", r, got, want)
		}
	}
}

func TestNiceTicks(t *testing.T) {
	for _, maxV := range []float64{0, 7, 42, 180, 950, 4200} {
		ticks, top := niceTicks(maxV)
		if ticks[0] != 0 || top < maxV || len(ticks) < 3 || len(ticks) > 7 {
			t.Errorf("max %v: ticks %v top %v", maxV, ticks, top)
		}
	}
}

func TestTickLabels(t *testing.T) {
	for v, want := range map[float64]string{0: "0", 250: "250", 1200: "1,200", 12500: "12,500", 2.5: "2.5"} {
		if got := tickLabel(v); got != want {
			t.Errorf("tickLabel(%v) = %q, want %q", v, got, want)
		}
	}
}

func TestUptimeBars(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	spans := []uptime.Interval{{From: now.Add(-49 * time.Hour), To: now.Add(-48*time.Hour - 30*time.Minute), Major: true}}
	days := uptime.Days(spans, now.AddDate(0, 0, -60), 90, now)
	svg := string(UptimeBars(days, "90 day uptime <test>"))
	wellFormed(t, svg)
	if n := strings.Count(svg, "<rect "); n != 90 {
		t.Fatalf("%d bars", n)
	}
	if !strings.Contains(svg, `class="bar major"`) || !strings.Contains(svg, `class="bar nodata"`) {
		t.Fatal("missing level classes")
	}
	if !strings.Contains(svg, "&lt;test&gt;") {
		t.Fatal("label not escaped")
	}
}

func TestLatencyChart(t *testing.T) {
	start := time.Date(2026, 9, 30, 6, 0, 0, 0, time.UTC)
	mk := func(base float64, gapAt int) []Point {
		pts := make([]Point, 36)
		for i := range pts {
			pts[i] = Point{P50: base + float64(i%5), P95: base * 1.6}
			if i == gapAt {
				pts[i] = Point{P50: math.NaN(), P95: math.NaN(), Failed: true}
			}
		}
		return pts
	}
	c := Latency{
		Start: start, Step: 10 * time.Minute, Buckets: 36,
		Series: []Series{{Name: "eu-west", Slot: 0, Points: mk(40, 20)}, {Name: "us-east", Slot: 1, Points: mk(95, -1)}, {Name: "ap-south", Slot: 3, Points: mk(180, -1)}},
		Spans:  []Span{{From: start.Add(3 * time.Hour), To: start.Add(3*time.Hour + 20*time.Minute)}},
		Label:  "p50 latency by region",
	}
	svg := string(LatencyChart(c))
	wellFormed(t, svg)
	if n := strings.Count(svg, `class="line `); n != 3 {
		t.Fatalf("%d lines", n)
	}
	// The gap splits eu-west's path into two segments.
	euPath := svg[strings.Index(svg, `class="line s0"`):]
	euPath = euPath[:strings.Index(euPath, "/>")]
	if strings.Count(euPath, "M") != 2 {
		t.Fatalf("expected the gap to break the line: %s", euPath)
	}
	if !strings.Contains(svg, `class="line s0 dashed"`) {
		t.Fatal("fourth slot should reuse a hue with a dash pattern")
	}
	if !strings.Contains(svg, `class="inc"`) || strings.Count(svg, `class="fail"`) != 1 {
		t.Fatal("incident band or failure tick missing")
	}
	if !strings.Contains(svg, ">09:00<") {
		t.Fatal("time axis labels missing")
	}

	var hover struct {
		Series []struct {
			Name string
			P50  []*float64
		}
	}
	if err := json.Unmarshal([]byte(HoverData(c)), &hover); err != nil || len(hover.Series) != 3 || hover.Series[0].P50[20] != nil {
		t.Fatalf("hover data: %v %+v", err, hover)
	}
}

func TestSparkline(t *testing.T) {
	svg := string(Sparkline([]float64{10, 12, math.NaN(), 11}, []bool{false, false, true, false}, "trend"))
	wellFormed(t, svg)
	if !strings.Contains(svg, `class="fail"`) {
		t.Fatal("failure tick missing")
	}
	if Sparkline(nil, nil, "") != "" {
		t.Fatal("empty input should render nothing")
	}
}
