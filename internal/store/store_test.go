package store

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"
)

var t0 = time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "vigil.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMigrationsAreIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vigil.db")
	for range 2 {
		s, err := Open(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
	}
}

func TestInsertResultIsIdempotent(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	r := Result{ID: "job-1", MonitorID: "api", Region: "eu", ProbeID: "eu-1", Slot: t0, ReceivedAt: t0, LatencyMS: 12}
	for i, want := range []bool{true, false, false} {
		var inserted bool
		err := s.Tx(ctx, func(tx *Tx) error {
			var err error
			inserted, err = tx.InsertResult(ctx, r)
			return err
		})
		if err != nil || inserted != want {
			t.Fatalf("attempt %d: inserted=%v err=%v", i, inserted, err)
		}
	}
	if ok, _ := s.ResultExists(ctx, "job-1"); !ok {
		t.Fatal("result not found")
	}
}

func TestNotificationDedup(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	n := Notification{Notifier: "ops", DedupKey: "incident/api/1/opened", MonitorID: "api", Kind: "opened", Message: []byte("{}"), CreatedAt: t0}
	var added []bool
	for range 3 {
		_ = s.Tx(ctx, func(tx *Tx) error {
			ok, err := tx.EnqueueNotification(ctx, n)
			added = append(added, ok)
			return err
		})
	}
	other := n
	other.Notifier = "email"
	_ = s.Tx(ctx, func(tx *Tx) error {
		ok, err := tx.EnqueueNotification(ctx, other)
		added = append(added, ok)
		return err
	})
	if added[0] != true || added[1] || added[2] || !added[3] {
		t.Fatalf("added = %v; same key on another notifier must still be delivered", added)
	}
	due, _ := s.DueNotifications(ctx, t0, 10)
	if len(due) != 2 {
		t.Fatalf("due = %d", len(due))
	}
}

func TestRollupHoursThenDays(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	// Two hours of 10-second checks in one region; hour two has failures.
	err := s.Tx(ctx, func(tx *Tx) error {
		for i := range 720 {
			slot := t0.Add(time.Duration(i) * 10 * time.Second)
			ok := i < 360 || i%10 != 0
			latency := float64(20 + i%50)
			if _, err := tx.InsertResult(ctx, Result{
				ID: slot.Format(time.RFC3339), MonitorID: "api", Region: "eu", ProbeID: "eu-1",
				Slot: slot, ReceivedAt: slot, OK: ok, LatencyMS: latency,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.RollupHours(ctx, t0, t0.Add(2*time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("hourly rollups = %d, %v", n, err)
	}
	// Rerunning over the same range converges on the same rows.
	if _, err := s.RollupHours(ctx, t0, t0.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	hours, _ := s.Rollups(ctx, Hour, []string{"api"}, t0, t0.Add(24*time.Hour))
	if len(hours) != 2 || hours[0].Total != 360 || hours[0].OK != 360 || hours[1].OK != 324 {
		t.Fatalf("hours = %+v", hours)
	}
	if _, err := s.RollupDays(ctx, t0, t0.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	days, _ := s.Rollups(ctx, Day, nil, t0, t0.Add(24*time.Hour))
	if len(days) != 1 || days[0].Total != 720 || days[0].OK != 684 {
		t.Fatalf("days = %+v", days)
	}
	// Latencies cycle 20..69 ms; the merged median sits near 44.
	if p50 := days[0].Latency.Quantile(0.5); math.Abs(p50-44) > 44*0.03 {
		t.Fatalf("daily p50 = %.2f", p50)
	}
}

func TestPruneRespectsRetention(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	now := t0.Add(30 * 24 * time.Hour)
	_ = s.Tx(ctx, func(tx *Tx) error {
		for i, age := range []time.Duration{time.Hour, 8 * 24 * time.Hour} {
			if _, err := tx.InsertResult(ctx, Result{ID: string(rune('a' + i)), MonitorID: "api", Region: "eu", Slot: now.Add(-age), ReceivedAt: now}); err != nil {
				return err
			}
		}
		return nil
	})
	if _, err := s.Prune(ctx, now, Retention{Raw: 7 * 24 * time.Hour, Hourly: 90 * 24 * time.Hour, Daily: 400 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	left, _ := s.Results(ctx, "api", now.Add(-365*24*time.Hour), now)
	if len(left) != 1 || left[0].ID != "a" {
		t.Fatalf("left = %+v", left)
	}
}

func TestIncidentsBetweenIncludesOverlaps(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	err := s.Tx(ctx, func(tx *Tx) error {
		for _, inc := range []Incident{
			{MonitorID: "api", OpenedAt: t0.Add(-2 * time.Hour), Impact: "partial", Regions: []string{"eu"}}, // spans the window start
			{MonitorID: "api", OpenedAt: t0.Add(3 * time.Hour), Impact: "major"},                             // inside, still open
			{MonitorID: "web", OpenedAt: t0.Add(time.Hour), Impact: "major"},                                 // other monitor
		} {
			id, err := tx.OpenIncident(ctx, inc)
			if err != nil {
				return err
			}
			if inc.OpenedAt.Before(t0) {
				if err := tx.ResolveIncident(ctx, id, t0.Add(30*time.Minute)); err != nil {
					return err
				}
			}
			if _, err := tx.AddUpdate(ctx, Update{IncidentID: id, At: inc.OpenedAt, Status: "investigating", Body: "Detected", Author: AuthorVigil}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.IncidentsBetween(ctx, []string{"api"}, t0, t0.Add(24*time.Hour))
	if err != nil || len(got) != 2 {
		t.Fatalf("got %d incidents, %v", len(got), err)
	}
	if !got[0].Open() || got[1].Open() || len(got[1].Updates) != 1 || got[1].Regions[0] != "eu" {
		t.Fatalf("unexpected incidents: %+v", got)
	}
	open, _ := s.OpenIncidents(ctx)
	if len(open) != 2 {
		t.Fatalf("open = %d", len(open))
	}
}
