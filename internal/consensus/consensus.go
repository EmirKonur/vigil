// Package consensus decides when a monitor is down.
//
// It is a pure state machine: callers feed it check results and clock ticks,
// and it returns the effects (open an incident, notify, resolve) that the
// engine persists. Nothing here touches the network, the database, or the
// wall clock, which keeps every transition table-testable.
//
// The decision happens at two levels, each with its own hysteresis:
//
//  1. Region: a region becomes failing after FailAfter consecutive failed
//     checks and recovers after RecoverAfter consecutive successes.
//  2. Monitor: the monitor is unhealthy while at least Quorum of the regions
//     that are currently reporting are failing.
//
// An incident opens when the monitor turns unhealthy and resolves when it
// turns healthy again, unless the verdict is flapping, in which case the
// incident is held open and notifications are suppressed until it settles.
package consensus

import (
	"slices"
	"time"
)

// Policy holds the thresholds for one monitor. It comes from configuration
// and may change between calls (config reloads); State carries over.
type Policy struct {
	Regions      []string
	Quorum       int // regions that must agree the monitor is failing
	FailAfter    int // consecutive failures before a region counts as failing
	RecoverAfter int // consecutive successes before a failing region recovers
	// StaleAfter excludes a region from the vote when it has not reported
	// for this long, so a dead probe neither keeps an incident open nor
	// blocks one from opening.
	StaleAfter    time.Duration
	Renotify      time.Duration // reminder interval while down; 0 disables
	EscalateAfter time.Duration // escalate once an incident is this old; 0 disables
	FlapWindow    time.Duration
	FlapThreshold int // verdict changes within FlapWindow that mean flapping; 0 disables
}

// Phase is the monitor-level status shown in the UI.
type Phase string

// Phases, from best to worst.
const (
	PhaseUnknown  Phase = "unknown"  // no region has reported recently
	PhaseUp       Phase = "up"       // every reporting region is healthy
	PhaseDegraded Phase = "degraded" // some regions failing, below quorum
	PhaseDown     Phase = "down"     // an incident is open
)

// Impact describes how widespread an open incident is.
type Impact string

// Impacts.
const (
	ImpactPartial Impact = "partial" // quorum reached, but some regions still succeed
	ImpactMajor   Impact = "major"   // every reporting region is failing
)

// RegionState tracks one region's recent results.
type RegionState struct {
	Failing         bool      `json:"failing"`
	ConsecutiveFail int       `json:"consecutive_fail"`
	ConsecutiveOK   int       `json:"consecutive_ok"`
	LastSlot        time.Time `json:"last_slot"` // scheduled time of the last applied result
	LastSeen        time.Time `json:"last_seen"` // server time the last result arrived
	LastError       string    `json:"last_error,omitempty"`
}

// Incident is the open incident, identified within its monitor by OpenedAt.
type Incident struct {
	OpenedAt       time.Time `json:"opened_at"`
	Impact         Impact    `json:"impact"`
	Regions        []string  `json:"regions"`
	LastNotifiedAt time.Time `json:"last_notified_at"`
	Reminders      int       `json:"reminders"`
	Escalated      bool      `json:"escalated"`
	FlapNotified   bool      `json:"flap_notified"`
}

// State is everything the machine remembers about a monitor. It is
// serialized to JSON and persisted after every transition.
type State struct {
	Regions   map[string]*RegionState `json:"regions"`
	Phase     Phase                   `json:"phase"`
	Unhealthy bool                    `json:"unhealthy"`
	Incident  *Incident               `json:"incident,omitempty"`
	Flips     []time.Time             `json:"flips,omitempty"`
	Flapping  bool                    `json:"flapping"`
}

// Result is one check outcome from one region.
type Result struct {
	Region string
	OK     bool
	Slot   time.Time // the scheduler's slot time, not the probe's clock
	Error  string
}

// EffectKind enumerates what the engine must do after a transition.
type EffectKind string

// Effect kinds.
const (
	EffectOpen          EffectKind = "open"
	EffectResolve       EffectKind = "resolve"
	EffectImpactChanged EffectKind = "impact_changed"
	EffectNotify        EffectKind = "notify"
)

// Alert is the kind of notification an EffectNotify asks for.
type Alert string

// Alert kinds.
const (
	AlertOpened    Alert = "opened"
	AlertReminder  Alert = "reminder"
	AlertEscalated Alert = "escalated"
	AlertFlapping  Alert = "flapping"
	AlertResolved  Alert = "resolved"
)

// Effect is one instruction for the engine. Incident identifies the incident
// by its open time.
type Effect struct {
	Kind     EffectKind
	Incident time.Time
	Alert    Alert    // EffectNotify only
	Seq      int      // reminder number, part of the dedup key
	Impact   Impact   // EffectOpen and EffectImpactChanged
	Regions  []string // failing regions at the time of the effect
	Voting   int      // regions that were reporting at the time of the effect
	// Escalated tells the engine to include escalation notifiers, so the
	// people paged at escalation also hear about reminders and resolution.
	Escalated bool
	At        time.Time
}

// New returns the initial state.
func New() *State {
	return &State{Regions: map[string]*RegionState{}, Phase: PhaseUnknown}
}

// Clone returns a deep copy, so the engine can apply a transition, persist
// it, and only then publish the new state.
func (s *State) Clone() *State {
	c := *s
	c.Regions = make(map[string]*RegionState, len(s.Regions))
	for k, v := range s.Regions {
		rs := *v
		c.Regions[k] = &rs
	}
	c.Flips = slices.Clone(s.Flips)
	if s.Incident != nil {
		inc := *s.Incident
		inc.Regions = slices.Clone(s.Incident.Regions)
		c.Incident = &inc
	}
	return &c
}

// ApplyResult feeds one result into the machine. now is the server's clock.
// Results whose slot is not newer than the region's last applied slot are
// ignored: they are retries or late deliveries, and replaying them would
// double-count a failure.
func (s *State) ApplyResult(p Policy, r Result, now time.Time) []Effect {
	if !slices.Contains(p.Regions, r.Region) {
		return nil
	}
	rs := s.region(r.Region)
	if !r.Slot.After(rs.LastSlot) {
		return nil
	}
	rs.LastSlot = r.Slot
	rs.LastSeen = now
	if r.OK {
		rs.ConsecutiveOK++
		rs.ConsecutiveFail = 0
		rs.LastError = ""
		if rs.Failing && rs.ConsecutiveOK >= max(p.RecoverAfter, 1) {
			rs.Failing = false
		}
	} else {
		rs.ConsecutiveFail++
		rs.ConsecutiveOK = 0
		rs.LastError = r.Error
		if !rs.Failing && rs.ConsecutiveFail >= max(p.FailAfter, 1) {
			rs.Failing = true
		}
	}
	return s.evaluate(p, now)
}

// Tick advances timers: staleness, flap settling, escalation, and reminders.
// The engine calls it periodically for every active monitor.
func (s *State) Tick(p Policy, now time.Time) []Effect {
	effects := s.evaluate(p, now)

	if s.Flapping && len(s.Flips) > 0 && now.Sub(s.Flips[len(s.Flips)-1]) >= p.FlapWindow {
		s.Flapping = false
		if s.Incident != nil {
			s.Incident.FlapNotified = false
			if !s.Unhealthy {
				effects = append(effects, s.resolve(p, now)...)
			}
		}
	}

	if inc := s.Incident; inc != nil && s.Unhealthy && !s.Flapping {
		failing, voting := s.tally(p, now)
		if p.EscalateAfter > 0 && !inc.Escalated && now.Sub(inc.OpenedAt) >= p.EscalateAfter {
			inc.Escalated = true
			inc.LastNotifiedAt = now
			effects = append(effects, s.notify(AlertEscalated, 0, failing, voting, now))
		} else if p.Renotify > 0 && now.Sub(inc.LastNotifiedAt) >= p.Renotify {
			inc.Reminders++
			inc.LastNotifiedAt = now
			effects = append(effects, s.notify(AlertReminder, inc.Reminders, failing, voting, now))
		}
	}
	return effects
}

// Reset forgets per-region history, used when a paused monitor resumes so
// that results from before the pause cannot count toward thresholds.
func (s *State) Reset() {
	s.Regions = map[string]*RegionState{}
	if s.Incident == nil {
		s.Phase = PhaseUnknown
		s.Unhealthy = false
	}
}

func (s *State) region(name string) *RegionState {
	if s.Regions == nil {
		s.Regions = map[string]*RegionState{}
	}
	rs, ok := s.Regions[name]
	if !ok {
		rs = &RegionState{}
		s.Regions[name] = rs
	}
	return rs
}

// tally returns the failing regions among those reporting, and how many are
// reporting.
func (s *State) tally(p Policy, now time.Time) (failing []string, voting int) {
	for _, name := range p.Regions {
		rs, ok := s.Regions[name]
		if !ok || rs.LastSeen.IsZero() {
			continue
		}
		if p.StaleAfter > 0 && now.Sub(rs.LastSeen) > p.StaleAfter {
			continue
		}
		voting++
		if rs.Failing {
			failing = append(failing, name)
		}
	}
	return failing, voting
}

// EffectiveQuorum is the number of failing regions needed for an incident
// given how many regions are reporting. When probes go missing the quorum
// shrinks with them: a monitor that goes quiet because two of three probes
// died is worse than one that alerts on a single region's word.
func EffectiveQuorum(p Policy, voting int) int {
	return max(min(p.Quorum, voting), 1)
}

func (s *State) evaluate(p Policy, now time.Time) []Effect {
	failing, voting := s.tally(p, now)
	if voting == 0 {
		// No evidence either way. An open incident stays open: we cannot
		// claim recovery without data.
		if s.Incident == nil {
			s.Phase = PhaseUnknown
		}
		return nil
	}

	unhealthy := len(failing) >= EffectiveQuorum(p, voting)
	if unhealthy != s.Unhealthy {
		s.Unhealthy = unhealthy
		s.recordFlip(p, now)
	}

	var effects []Effect
	switch {
	case unhealthy && s.Incident == nil:
		impact := impactOf(failing, voting)
		s.Incident = &Incident{OpenedAt: now, Impact: impact, Regions: failing, LastNotifiedAt: now}
		effects = append(effects, Effect{Kind: EffectOpen, Incident: now, Impact: impact, Regions: failing, Voting: voting, At: now})
		if s.Flapping {
			s.Incident.FlapNotified = true
			effects = append(effects, s.notify(AlertFlapping, 0, failing, voting, now))
		} else {
			effects = append(effects, s.notify(AlertOpened, 0, failing, voting, now))
		}

	case unhealthy && s.Incident != nil:
		if impact := impactOf(failing, voting); impact != s.Incident.Impact || !slices.Equal(failing, s.Incident.Regions) {
			s.Incident.Impact = impact
			s.Incident.Regions = failing
			effects = append(effects, Effect{Kind: EffectImpactChanged, Incident: s.Incident.OpenedAt, Impact: impact, Regions: failing, Voting: voting, At: now})
		}

	case !unhealthy && s.Incident != nil:
		if s.Flapping {
			if !s.Incident.FlapNotified {
				s.Incident.FlapNotified = true
				effects = append(effects, s.notify(AlertFlapping, 0, failing, voting, now))
			}
		} else {
			effects = append(effects, s.resolve(p, now)...)
		}
	}

	switch {
	case s.Incident != nil:
		s.Phase = PhaseDown
	case len(failing) > 0:
		s.Phase = PhaseDegraded
	default:
		s.Phase = PhaseUp
	}
	return effects
}

func (s *State) resolve(p Policy, now time.Time) []Effect {
	failing, voting := s.tally(p, now)
	opened, escalated := s.Incident.OpenedAt, s.Incident.Escalated
	s.Incident = nil
	if len(failing) > 0 {
		s.Phase = PhaseDegraded
	} else {
		s.Phase = PhaseUp
	}
	return []Effect{
		{Kind: EffectResolve, Incident: opened, Regions: failing, Voting: voting, At: now},
		{Kind: EffectNotify, Incident: opened, Alert: AlertResolved, Regions: failing, Voting: voting, Escalated: escalated, At: now},
	}
}

func (s *State) notify(a Alert, seq int, failing []string, voting int, now time.Time) Effect {
	return Effect{Kind: EffectNotify, Incident: s.Incident.OpenedAt, Alert: a, Seq: seq, Regions: failing, Voting: voting, Escalated: s.Incident.Escalated, At: now}
}

func (s *State) recordFlip(p Policy, now time.Time) {
	if p.FlapThreshold <= 0 {
		return
	}
	cutoff := now.Add(-p.FlapWindow)
	kept := s.Flips[:0]
	for _, f := range s.Flips {
		if f.After(cutoff) {
			kept = append(kept, f)
		}
	}
	s.Flips = append(kept, now)
	if len(s.Flips) >= p.FlapThreshold {
		s.Flapping = true
	}
}

func impactOf(failing []string, voting int) Impact {
	if len(failing) >= voting {
		return ImpactMajor
	}
	return ImpactPartial
}
