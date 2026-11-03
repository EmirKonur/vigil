package consensus

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 3, 14, 9, 0, 0, 0, time.UTC)

func testPolicy() Policy {
	return Policy{
		Regions:       []string{"ap", "eu", "us"},
		Quorum:        2,
		FailAfter:     3,
		RecoverAfter:  2,
		StaleAfter:    2 * time.Minute,
		Renotify:      30 * time.Minute,
		EscalateAfter: 15 * time.Minute,
		FlapWindow:    20 * time.Minute,
		FlapThreshold: 4,
	}
}

// step is one input: a single result, a full round of results (one per
// region, as the scheduler issues them), or a clock tick when neither is set.
type step struct {
	at     time.Duration
	region string
	ok     bool
	round  []bool        // results for eu, us, ap at the same slot
	slot   time.Duration // defaults to at; set to replay an old slot
	want   []string      // effects, formatted by describe
	phase  Phase         // checked when non-empty
}

func fail(at time.Duration, region string, want ...string) step {
	return step{at: at, region: region, want: want}
}
func tick(at time.Duration, want ...string) step { return step{at: at, want: want} }

func (s step) expect(p Phase) step { s.phase = p; return s }

// round produces one result per region at the same slot. want is checked
// against the effects of the whole round.
func round(at time.Duration, eu, us, ap bool, want ...string) []step {
	return []step{{at: at, round: []bool{eu, us, ap}, want: want}}
}

func describe(e Effect) string {
	switch e.Kind {
	case EffectOpen, EffectImpactChanged:
		return fmt.Sprintf("%s %s %s", e.Kind, e.Impact, strings.Join(e.Regions, ","))
	case EffectNotify:
		if e.Seq > 0 {
			return fmt.Sprintf("notify %s#%d", e.Alert, e.Seq)
		}
		return "notify " + string(e.Alert)
	default:
		return string(e.Kind)
	}
}

func run(t *testing.T, p Policy, steps []step) *State {
	t.Helper()
	s := New()
	for i, st := range steps {
		now := t0.Add(st.at)
		var effects []Effect
		switch {
		case st.round != nil:
			for j, r := range []string{"eu", "us", "ap"} {
				effects = append(effects, s.ApplyResult(p, Result{Region: r, OK: st.round[j], Slot: now}, now)...)
			}
		case st.region == "":
			effects = s.Tick(p, now)
		default:
			slot := st.slot
			if slot == 0 {
				slot = st.at
			}
			effects = s.ApplyResult(p, Result{Region: st.region, OK: st.ok, Slot: t0.Add(slot)}, now)
		}
		got := make([]string, len(effects))
		for j, e := range effects {
			got[j] = describe(e)
		}
		if !slices.Equal(got, st.want) && !(len(got) == 0 && len(st.want) == 0) {
			t.Fatalf("step %d (t+%s %s ok=%v): effects %q, want %q", i, st.at, st.region, st.ok, got, st.want)
		}
		if st.phase != "" && s.Phase != st.phase {
			t.Fatalf("step %d (t+%s): phase %s, want %s", i, st.at, s.Phase, st.phase)
		}
	}
	return s
}

func seq(parts ...[]step) []step { return slices.Concat(parts...) }

const iv = 30 * time.Second

func TestStateMachine(t *testing.T) {
	tests := []struct {
		name   string
		policy func(*Policy)
		steps  []step
	}{
		{
			name: "a single failing region degrades but never opens an incident",
			steps: seq(
				round(1*iv, false, true, true),
				round(2*iv, false, true, true),
				round(3*iv, false, true, true),
				round(4*iv, false, true, true),
				[]step{tick(4*iv + time.Second).expect(PhaseDegraded)},
			),
		},
		{
			name: "quorum of regions with N consecutive failures opens one incident",
			steps: seq(
				round(1*iv, false, false, true),
				round(2*iv, false, false, true),
				round(3*iv, false, false, true, "open partial eu,us", "notify opened"),
				round(4*iv, false, false, true),
				[]step{tick(4*iv + time.Second).expect(PhaseDown)},
			),
		},
		{
			name: "a success inside the failure streak resets the count",
			steps: seq(
				round(1*iv, false, false, true),
				round(2*iv, false, false, true),
				round(3*iv, true, true, true),
				round(4*iv, false, false, true),
				round(5*iv, false, false, true),
				[]step{tick(5*iv + time.Second).expect(PhaseUp)},
			),
		},
		{
			name: "resolution needs M consecutive successes from enough regions",
			steps: seq(
				round(1*iv, false, false, true),
				round(2*iv, false, false, true),
				round(3*iv, false, false, true, "open partial eu,us", "notify opened"),
				round(4*iv, true, false, true),
				round(5*iv, false, false, true),
				round(6*iv, true, false, true),
				round(7*iv, true, false, true, "resolve", "notify resolved"),
				[]step{tick(7*iv + time.Second).expect(PhaseDegraded)},
			),
		},
		{
			name: "impact widens to major and narrows again as regions recover",
			steps: seq(
				round(1*iv, false, false, true),
				round(2*iv, false, false, true),
				round(3*iv, false, false, false, "open partial eu,us", "notify opened"),
				round(4*iv, false, false, false),
				round(5*iv, false, false, false, "impact_changed major ap,eu,us"),
				round(6*iv, true, false, true),
				// eu recovers first (partial), then ap drops the count below quorum.
				round(7*iv, true, false, true, "impact_changed partial ap,us", "resolve", "notify resolved"),
			),
		},
		{
			name:   "escalation fires once, then reminders follow the renotify interval",
			policy: func(p *Policy) { p.StaleAfter = time.Hour },
			steps: seq(
				round(1*iv, false, false, false),
				round(2*iv, false, false, false),
				// Results within a slot arrive one by one, so the incident opens
				// on the second region and widens on the third.
				round(3*iv, false, false, false, "open partial eu,us", "notify opened", "impact_changed major ap,eu,us"),
				round(14*time.Minute, false, false, false),
				[]step{tick(3*iv+15*time.Minute, "notify escalated")},
				round(25*time.Minute, false, false, false),
				[]step{tick(3*iv + 30*time.Minute)},
				round(40*time.Minute, false, false, false),
				[]step{tick(3*iv+45*time.Minute, "notify reminder#1")},
				round(60*time.Minute, false, false, false),
				round(75*time.Minute, false, false, false),
				[]step{tick(3*iv+75*time.Minute, "notify reminder#2")},
			),
		},
		{
			name:   "renotify and escalation can be disabled",
			policy: func(p *Policy) { p.Renotify, p.EscalateAfter, p.StaleAfter = 0, 0, 2*time.Hour },
			steps: seq(
				round(1*iv, false, false, false),
				round(2*iv, false, false, false),
				// Results within a slot arrive one by one, so the incident opens
				// on the second region and widens on the third.
				round(3*iv, false, false, false, "open partial eu,us", "notify opened", "impact_changed major ap,eu,us"),
				round(90*time.Minute, false, false, false),
				[]step{tick(90*time.Minute + time.Second)},
			),
		},
		{
			name: "replayed and out-of-order results do not count twice",
			steps: []step{
				fail(1*iv, "eu"), fail(1*iv, "us"),
				fail(2*iv, "eu"), fail(2*iv, "us"),
				// At-least-once delivery: the same slot arrives again.
				{at: 2*iv + time.Second, region: "eu", slot: 2 * iv},
				{at: 2*iv + time.Second, region: "us", slot: 2 * iv},
				// A late result from an earlier slot.
				{at: 2*iv + 2*time.Second, region: "us", slot: 1 * iv},
				tick(2*iv + 3*time.Second).expect(PhaseUp),
				fail(3*iv, "eu"), fail(3*iv, "us", "open major eu,us", "notify opened"),
			},
		},
		{
			name: "a region that stops reporting leaves the vote",
			steps: seq(
				round(1*iv, false, true, true),
				round(2*iv, false, true, true),
				round(3*iv, false, true, true),
				// us and ap go silent; eu is the only voter left. With one
				// voter the effective quorum is one region, so eu's failure
				// now opens an incident instead of going unreported.
				[]step{
					fail(4*iv, "eu"),
					fail(5*iv, "eu"),
					tick(3*iv+2*time.Minute+time.Second, "open major eu", "notify opened"),
				},
			),
		},
		{
			name: "no fresh data keeps an open incident open",
			steps: seq(
				round(1*iv, false, false, false),
				round(2*iv, false, false, false),
				// Results within a slot arrive one by one, so the incident opens
				// on the second region and widens on the third.
				round(3*iv, false, false, false, "open partial eu,us", "notify opened", "impact_changed major ap,eu,us"),
				[]step{tick(3*iv + 10*time.Minute).expect(PhaseDown)},
			),
		},
		{
			name: "flapping holds the incident open and goes quiet until it settles",
			steps: seq(
				// flip 1: down
				round(1*iv, false, false, true),
				round(2*iv, false, false, true),
				round(3*iv, false, false, true, "open partial eu,us", "notify opened"),
				// flip 2: up
				round(4*iv, true, true, true),
				round(5*iv, true, true, true, "resolve", "notify resolved"),
				// flip 3: down again
				round(6*iv, false, false, true),
				round(7*iv, false, false, true),
				round(8*iv, false, false, true, "open partial eu,us", "notify opened"),
				// flip 4 reaches the threshold: hold the incident, say so once
				round(9*iv, true, true, true),
				round(10*iv, true, true, true, "notify flapping"),
				// flips 5 and 6 produce no notifications at all
				round(11*iv, false, false, true),
				round(12*iv, false, false, true),
				round(13*iv, false, false, true),
				round(14*iv, true, true, true),
				round(15*iv, true, true, true),
				// healthy, but not yet quiet for a full flap window
				round(20*time.Minute, true, true, true),
				[]step{tick(20*time.Minute + time.Second).expect(PhaseDown)},
				// quiet for the whole window: resolve for real
				round(25*time.Minute, true, true, true),
				[]step{tick(15*iv+20*time.Minute, "resolve", "notify resolved")},
			),
		},
		{
			name:   "an incident that opens while flapping announces flapping instead",
			policy: func(p *Policy) { p.FlapThreshold = 3 },
			steps: seq(
				round(1*iv, false, false, true),
				round(2*iv, false, false, true),
				round(3*iv, false, false, true, "open partial eu,us", "notify opened"),
				round(4*iv, true, true, true),
				round(5*iv, true, true, true, "resolve", "notify resolved"),
				round(6*iv, false, false, true),
				round(7*iv, false, false, true),
				round(8*iv, false, false, true, "open partial eu,us", "notify flapping"),
			),
		},
		{
			name:   "quorum of one behaves like a classic single-probe monitor",
			policy: func(p *Policy) { p.Quorum = 1 },
			steps: seq(
				round(1*iv, false, true, true),
				round(2*iv, false, true, true),
				round(3*iv, false, true, true, "open partial eu", "notify opened"),
			),
		},
		{
			name: "results from regions outside the policy are ignored",
			steps: []step{
				fail(1*iv, "mars"), fail(2*iv, "mars"), fail(3*iv, "mars"),
				tick(3*iv + time.Second).expect(PhaseUnknown),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := testPolicy()
			if tt.policy != nil {
				tt.policy(&p)
			}
			run(t, p, tt.steps)
		})
	}
}

func TestCloneIsDeep(t *testing.T) {
	p := testPolicy()
	s := run(t, p, seq(
		round(1*iv, false, false, true),
		round(2*iv, false, false, true),
		round(3*iv, false, false, true, "open partial eu,us", "notify opened"),
	))
	c := s.Clone()
	c.Regions["eu"].ConsecutiveFail = 99
	c.Incident.Regions[0] = "mars"
	c.Incident.Reminders = 5
	if s.Regions["eu"].ConsecutiveFail == 99 || s.Incident.Regions[0] == "mars" || s.Incident.Reminders == 5 {
		t.Fatal("clone shares memory with the original")
	}
}

func TestResolutionAfterEscalationReachesEscalationChannel(t *testing.T) {
	p := testPolicy()
	p.StaleAfter = time.Hour
	s := run(t, p, seq(
		round(1*iv, false, false, false),
		round(2*iv, false, false, false),
		round(3*iv, false, false, false, "open partial eu,us", "notify opened", "impact_changed major ap,eu,us"),
		[]step{tick(3*iv+15*time.Minute, "notify escalated")},
	))
	var effects []Effect
	for _, at := range []time.Duration{16 * time.Minute, 17 * time.Minute} {
		for _, r := range []string{"eu", "us", "ap"} {
			effects = append(effects, s.ApplyResult(p, Result{Region: r, OK: true, Slot: t0.Add(at)}, t0.Add(at))...)
		}
	}
	last := effects[len(effects)-1]
	if last.Alert != AlertResolved || !last.Escalated {
		t.Fatalf("last effect = %+v, want an escalated resolution", last)
	}
}

func TestResetAfterPauseDiscardsStreaks(t *testing.T) {
	p := testPolicy()
	s := run(t, p, seq(
		round(1*iv, false, false, true),
		round(2*iv, false, false, true),
	))
	s.Reset()
	if s.Phase != PhaseUnknown {
		t.Fatalf("phase after reset = %s", s.Phase)
	}
	// One more failure after resuming must not complete the old streak.
	for _, r := range []string{"eu", "us", "ap"} {
		if eff := s.ApplyResult(p, Result{Region: r, OK: r == "ap", Slot: t0.Add(10 * iv)}, t0.Add(10*iv)); len(eff) != 0 {
			t.Fatalf("unexpected effects after reset: %v", eff)
		}
	}
}

// TestInvariantsUnderRandomInput drives the machine with random results and
// ticks and checks the properties the rest of the system relies on.
func TestInvariantsUnderRandomInput(t *testing.T) {
	regions := []string{"ap", "eu", "us"}
	for seed := range uint64(300) {
		rng := rand.New(rand.NewPCG(seed, seed*7+1))
		p := testPolicy()
		p.FlapThreshold = rng.IntN(5)
		p.Quorum = 1 + rng.IntN(3)
		s := New()
		now := t0
		var open *time.Time
		announced := map[time.Time]int{}
		badRegion := rng.IntN(4) // region with a higher failure rate; 3 means none
		for i := range 400 {
			now = now.Add(time.Duration(1+rng.IntN(45)) * time.Second)
			var effects []Effect
			if rng.IntN(6) == 0 {
				effects = s.Tick(p, now)
			} else {
				ri := rng.IntN(3)
				failRate := 0.15
				if ri == badRegion || (i/40)%3 == 1 {
					failRate = 0.7
				}
				effects = s.ApplyResult(p, Result{Region: regions[ri], OK: rng.Float64() > failRate, Slot: now}, now)
			}
			for _, e := range effects {
				switch e.Kind {
				case EffectOpen:
					if open != nil {
						t.Fatalf("seed %d: opened a second incident while one was open", seed)
					}
					at := e.Incident
					open = &at
				case EffectResolve:
					if open == nil || !open.Equal(e.Incident) {
						t.Fatalf("seed %d: resolved an incident that was not open", seed)
					}
					open = nil
				case EffectNotify:
					if e.Alert == AlertOpened || e.Alert == AlertFlapping {
						announced[e.Incident]++
					}
					if e.Alert == AlertResolved && open != nil {
						t.Fatalf("seed %d: resolved notification while incident still open", seed)
					}
				}
			}
			if (s.Incident != nil) != (open != nil) {
				t.Fatalf("seed %d step %d: state incident %v, effects say open=%v", seed, i, s.Incident != nil, open != nil)
			}
			if s.Flapping && s.Incident == nil {
				t.Fatalf("seed %d step %d: flapping without an open incident", seed, i)
			}
			if (s.Phase == PhaseDown) != (s.Incident != nil) {
				t.Fatalf("seed %d step %d: phase %s with incident=%v", seed, i, s.Phase, s.Incident != nil)
			}
		}
		for at, n := range announced {
			if n > 2 {
				t.Fatalf("seed %d: incident %s announced %d times", seed, at, n)
			}
		}
	}
}

func BenchmarkApplyResult(b *testing.B) {
	p := testPolicy()
	s := New()
	regions := []string{"ap", "eu", "us"}
	now := t0
	for i := range b.N {
		now = now.Add(10 * time.Second)
		s.ApplyResult(p, Result{Region: regions[i%3], OK: i%17 != 0, Slot: now}, now)
	}
}
