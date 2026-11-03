// Package uptime turns incidents into availability numbers.
//
// Availability is measured from incidents, not raw check results: a minute
// counts as down only if the consensus engine said so. One region's flaky
// network path therefore does not dent the public uptime figure, which is
// the whole point of multi-region consensus. Per-region check success rates
// are reported separately, from rollups.
package uptime

import (
	"sort"
	"time"

	"vigil/internal/store"
)

// Interval is a half-open span of downtime.
type Interval struct {
	From, To time.Time
	Major    bool
}

// Intervals converts incidents to downtime spans, treating open incidents as
// ongoing until now.
func Intervals(incidents []store.Incident, now time.Time) []Interval {
	out := make([]Interval, 0, len(incidents))
	for _, inc := range incidents {
		end := inc.ResolvedAt
		if inc.Open() {
			end = now
		}
		if end.After(inc.OpenedAt) {
			out = append(out, Interval{From: inc.OpenedAt, To: end, Major: inc.Impact == "major"})
		}
	}
	return out
}

// Union merges overlapping spans, so two monitors of one component that are
// down at the same time count that time once.
func Union(spans []Interval) []Interval {
	if len(spans) == 0 {
		return nil
	}
	s := append([]Interval(nil), spans...)
	sort.Slice(s, func(i, j int) bool { return s[i].From.Before(s[j].From) })
	out := []Interval{s[0]}
	for _, iv := range s[1:] {
		last := &out[len(out)-1]
		if !iv.From.After(last.To) {
			if iv.To.After(last.To) {
				last.To = iv.To
			}
			last.Major = last.Major || iv.Major
			continue
		}
		out = append(out, iv)
	}
	return out
}

// Downtime is the total length of spans clipped to [from, to).
func Downtime(spans []Interval, from, to time.Time) time.Duration {
	var total time.Duration
	for _, iv := range Union(spans) {
		start, end := maxTime(iv.From, from), minTime(iv.To, to)
		if end.After(start) {
			total += end.Sub(start)
		}
	}
	return total
}

// Ratio is availability over [from, to): 1 minus the downtime fraction.
// Time before tracking began (since) is excluded rather than counted as up.
func Ratio(spans []Interval, since, from, to time.Time) float64 {
	from = maxTime(from, since)
	if !to.After(from) {
		return 1
	}
	return 1 - float64(Downtime(spans, from, to))/float64(to.Sub(from))
}

// Level classifies a day for the uptime bars.
type Level string

// Levels, best to worst.
const (
	LevelNoData  Level = "nodata"
	LevelUp      Level = "up"
	LevelPartial Level = "partial"
	LevelMajor   Level = "major"
)

// Day is one bar of the 90-day uptime chart.
type Day struct {
	Date      time.Time // UTC midnight
	Level     Level
	Ratio     float64
	Downtime  time.Duration
	Incidents int
}

// Days returns n consecutive UTC days ending with today. Days before since
// (when tracking began) have no data.
func Days(spans []Interval, since time.Time, n int, now time.Time) []Day {
	today := startOfDay(now)
	merged := Union(spans)
	days := make([]Day, n)
	for i := range n {
		day := today.AddDate(0, 0, i-n+1)
		end := minTime(day.AddDate(0, 0, 1), now)
		d := Day{Date: day, Level: LevelNoData, Ratio: 1}
		if !since.IsZero() && end.After(maxTime(day, since)) {
			d.Level = LevelUp
			d.Downtime = Downtime(merged, day, end)
			d.Ratio = Ratio(merged, since, day, end)
			for _, iv := range merged {
				if iv.From.Before(end) && iv.To.After(day) {
					d.Incidents++
					if iv.Major {
						d.Level = LevelMajor
					} else if d.Level != LevelMajor {
						d.Level = LevelPartial
					}
				}
			}
		}
		days[i] = d
	}
	return days
}

func startOfDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
