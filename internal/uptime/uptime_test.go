package uptime

import (
	"math"
	"testing"
	"time"

	"vigil/internal/store"
)

var day0 = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

func at(h, m int) time.Time {
	return day0.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
}

func TestUnionMergesOverlaps(t *testing.T) {
	got := Union([]Interval{
		{From: at(10, 0), To: at(10, 30)},
		{From: at(10, 20), To: at(11, 0), Major: true},
		{From: at(12, 0), To: at(12, 5)},
		{From: at(11, 0), To: at(11, 10)}, // touches the merged span
	})
	if len(got) != 2 || !got[0].To.Equal(at(11, 10)) || !got[0].Major || got[1].Major {
		t.Fatalf("union = %+v", got)
	}
}

func TestDowntimeCountsSimultaneousOutagesOnce(t *testing.T) {
	spans := []Interval{
		{From: at(9, 0), To: at(9, 30)},  // monitor A
		{From: at(9, 15), To: at(9, 45)}, // monitor B, same component
	}
	if d := Downtime(spans, day0, day0.Add(24*time.Hour)); d != 45*time.Minute {
		t.Fatalf("downtime = %s", d)
	}
	// Clipped to the window.
	if d := Downtime(spans, at(9, 40), at(10, 0)); d != 5*time.Minute {
		t.Fatalf("clipped downtime = %s", d)
	}
}

func TestRatioExcludesTimeBeforeTracking(t *testing.T) {
	spans := []Interval{{From: at(18, 0), To: at(18, 36)}}
	// Tracking began at noon, so the day has 12 hours of history, 36
	// minutes of which were down: 95%.
	got := Ratio(spans, at(12, 0), day0, day0.Add(24*time.Hour))
	if math.Abs(got-0.95) > 1e-9 {
		t.Fatalf("ratio = %v", got)
	}
}

func TestIntervalsTreatOpenIncidentsAsOngoing(t *testing.T) {
	now := at(15, 0)
	incs := []store.Incident{
		{OpenedAt: at(14, 0), Impact: "major"},
		{OpenedAt: at(10, 0), ResolvedAt: at(10, 12), Impact: "partial"},
	}
	got := Intervals(incs, now)
	if len(got) != 2 || !got[0].To.Equal(now) || !got[0].Major || got[1].To.Sub(got[1].From) != 12*time.Minute {
		t.Fatalf("intervals = %+v", got)
	}
}

func TestDays(t *testing.T) {
	now := day0.AddDate(0, 0, 2).Add(6 * time.Hour) // Aug 3, 06:00
	spans := []Interval{
		{From: at(23, 50), To: at(24, 10)},             // crosses midnight Aug 1 -> 2
		{From: at(30, 0), To: at(30, 30), Major: true}, // Aug 2, 06:00
	}
	days := Days(spans, day0.AddDate(0, 0, -1).Add(12*time.Hour), 5, now)
	if len(days) != 5 || !days[4].Date.Equal(day0.AddDate(0, 0, 2)) {
		t.Fatalf("days = %+v", days)
	}
	want := []struct {
		level     Level
		incidents int
		down      time.Duration
	}{
		{LevelNoData, 0, 0},                 // Jul 30, before tracking
		{LevelUp, 0, 0},                     // Jul 31, tracking began at noon
		{LevelPartial, 1, 10 * time.Minute}, // Aug 1
		{LevelMajor, 2, 40 * time.Minute},   // Aug 2: tail of the first plus the major one
		{LevelUp, 0, 0},                     // Aug 3, partial day so far
	}
	for i, w := range want {
		d := days[i]
		if d.Level != w.level || d.Incidents != w.incidents || d.Downtime != w.down {
			t.Errorf("day %d (%s): level=%s incidents=%d down=%s, want %s %d %s",
				i, d.Date.Format("Jan 2"), d.Level, d.Incidents, d.Downtime, w.level, w.incidents, w.down)
		}
	}
	if r := days[2].Ratio; math.Abs(r-(1-10.0/1440)) > 1e-9 {
		t.Errorf("Aug 1 ratio = %v", r)
	}
}
