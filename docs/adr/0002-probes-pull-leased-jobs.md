# 0002: Probes pull leased jobs over HTTP

Status: accepted

## Context

Remote probes run in other regions, usually behind NAT or a cloud firewall, and they come and go: deploys, spot instance reclamation, network partitions. The server must know which checks ran, must not double-count a result, and must cope with a probe that disappears halfway through a check.

## Decision

Probes pull. A probe long-polls `POST /api/v1/probe/lease` with its region and how many jobs it can take; the server answers with jobs as soon as one is ready or after the wait. Each job carries a lease TTL, a relative duration (check timeout plus 5 seconds). The server tracks expiry on its own monotonic clock; the probe derives its local deadline from the moment the response arrived. An expired lease puts the job back at the front of the region's queue for another probe, once.

Results are posted in batches and acknowledged per job. The job ID is the result's primary key, so redelivery is harmless and the probe can retry until acknowledged. Authentication is a shared bearer token over HTTPS.

## Alternatives considered

- **The server calls probes** (push). Needs inbound connectivity to every probe, which is the hard part of running probes in other people's networks, and the server must track probe addresses and health to route work.
- **Probes schedule their own checks from a copy of the config.** Simpler protocol, but every probe needs the full config, there is no single authority on which slots ran, and consensus would have to reconcile independent clocks. Slots assigned by one scheduler give every region the same slot time to compare.
- **A message broker** (NATS, Redis streams). Gives leasing and redelivery out of the box but adds a second stateful service to a product whose point is being one binary.
- **Absolute lease deadlines.** Simpler to describe, but a probe whose clock runs two minutes fast would abandon every check, and one running slow would report results the server already gave away.

## Consequences

A lease endpoint is a long-lived request per probe; the server holds one goroutine per waiting probe, which is cheap. Probes need only outbound HTTPS. A server restart forgets in-flight job IDs and answers `unknown` for them, costing at most one sample per monitor and region. One shared token means one leaked token compromises all probes; per-probe credentials are on the roadmap.
