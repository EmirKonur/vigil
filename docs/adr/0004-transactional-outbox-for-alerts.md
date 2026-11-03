# 0004: A transactional outbox for alerts

Status: accepted

## Context

An incident must produce exactly one "opened" and one "resolved" notification per channel, even though results are delivered at least once, the server can crash at any point, webhooks fail, and SMTP servers time out. Sending from inside the code path that detects the incident would either block ingest on slow receivers or lose alerts on crash.

## Decision

Alerts are rows in a `notifications` table, inserted in the same transaction as the state change that caused them. The table has a unique index on `(notifier, dedup_key)`, where the key names the alert: `incident/<monitor>/<opened-at-ms>/<kind>`, with a sequence number for reminders and the certificate expiry for certificate warnings. A dispatcher goroutine delivers due rows, records success, or schedules a retry with exponential backoff (15 seconds doubling to 30 minutes, 8 attempts). 4xx responses that retrying cannot fix (400, 401, 403, 404, 410) fail the row immediately.

Every delivery carries the key as `X-Vigil-Delivery` (and in the Message-ID of emails), so receivers can deduplicate the one case the outbox cannot prevent: a crash after a successful send but before the row is marked sent.

## Alternatives considered

- **Send inline and remember sent alerts in memory.** Loses alerts on crash and resends them after restart.
- **Send inline, record afterwards.** A slow webhook stalls result ingest for every monitor.
- **A queue service.** Same guarantees, one more thing to run.

## Consequences

Deduplication is a database constraint rather than a code path, which is easy to test: the integration test counts deliveries per channel. Alerts go out shortly after the transition rather than within it. The message snapshot is rendered at enqueue time, except that non-resolution alerts refresh the incident's current extent just before sending, so an incident that widened in the meantime is described correctly. Delivered rows are pruned with daily retention.
