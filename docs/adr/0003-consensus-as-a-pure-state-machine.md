# 0003: Consensus as a pure state machine

Status: accepted

## Context

Deciding when a monitor is down is the product. The logic has to combine results from several regions arriving out of order, streak thresholds, staleness, flap detection, reminders, and escalation. Bugs here are either false pages or missed outages, and they show up only in specific sequences of events.

## Decision

Model it as a pure state machine in `internal/consensus`. `ApplyResult(policy, result, now)` and `Tick(policy, now)` mutate a serializable `State` and return a list of effects. The package has no I/O and takes time as an argument. The engine applies a transition to a clone, persists the effects and the new state in one transaction, and swaps the clone in after commit.

The machine has two levels of hysteresis. A region turns failing after `fail_after` consecutive failures and recovers after `recover_after` consecutive passes. The monitor is unhealthy while at least `min(quorum, reporting regions)` regions are failing, where a region reports if it has delivered a result within three intervals plus the timeout. Results whose slot is not newer than the region's last applied slot are ignored.

The quorum shrinks with the number of reporting regions. With three regions and a quorum of two, if two probes die the third region alone can open an incident.

## Alternatives considered

- **Rules in SQL** (for example, count failures in the last N slots per region at query time). Hard to express flap holding and escalation timers, and hard to test without a database.
- **A fixed quorum.** Fewer false alarms when probes die, but the monitor goes silent during exactly the kind of infrastructure trouble that kills probes. We chose to page on reduced evidence and make the reduced quorum visible (the dashboard flags regions without a healthy probe).
- **Majority over all configured regions, counting missing regions as failing.** Turns every probe outage into a customer-facing incident.

## Consequences

The machine is covered by a table of scripted scenarios and a randomized invariant test over hundreds of seeds; both run in milliseconds. Policy can change between calls (config reloads) without migrating state. Results inside one slot arrive one region at a time, so an incident commonly opens as "partial" and widens to "major" milliseconds later; the engine suppresses timeline entries for widening inside the first half interval and delays the opened alert by about one check timeout so it reports the full extent. The shrinking quorum is a deliberate trade toward fewer missed outages, documented in the README.
