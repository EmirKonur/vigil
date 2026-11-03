# 0001: SQLite with a pure-Go driver

Status: accepted

## Context

Vigil is meant to be one binary that someone can run on a small VM or a home server. It stores check results (tens of thousands of rows per day for a modest setup), rollups, incidents, the alert outbox, and per-monitor state. Writes are frequent but small, and they come from a single process. Cross-compiling for Linux, macOS, Windows, and FreeBSD on amd64 and arm64 should stay a one-line `go build`.

## Decision

Use SQLite through `modernc.org/sqlite`, a cgo-free translation of SQLite to Go. Run it in WAL mode with two `database/sql` pools: a writer pool capped at one connection whose transactions begin `IMMEDIATE`, and a reader pool sized to the CPU count. Writers never race each other into `SQLITE_BUSY`, and readers proceed alongside the writer, which WAL allows. Schema changes are numbered SQL files embedded in the binary and applied in order at startup, each in its own transaction, recorded in `schema_migrations`.

All timestamps are integer Unix milliseconds and all queries are plain SQL in `internal/store`, with no ORM.

## Alternatives considered

- **`mattn/go-sqlite3`** is the most common driver and somewhat faster, but needs cgo, which turns every cross-compile and every distroless image into a toolchain exercise.
- **Postgres from day one** removes the single-writer limit but makes the smallest deployment two processes plus credentials, for a workload that fits comfortably in one file.
- **An embedded key-value store** (bbolt, Pebble) would need hand-written indexes and rollup queries that SQL gives for free.

## Consequences

One server process owns the database. That rules out running two servers against the same file for high availability; a second server would need a different store. The benchmark in the README measures about 94 µs per result through the full write path on an M2 Pro laptop, so a single writer is not the constraint at the scale this is built for. The transpiled driver is slower under the race detector, which is why the web tests seed a smaller demo history.

## Moving to Postgres

The store package is the only code that knows about SQL, and the queries avoid SQLite-specific features except `ON CONFLICT ... DO NOTHING/UPDATE` (which Postgres supports with the same syntax) and `WITHOUT ROWID`. A Postgres implementation would:

1. Port the migrations: `INTEGER PRIMARY KEY` to `bigint generated always as identity`, `BLOB` to `bytea`, millisecond integers to `timestamptz` (or keep them as `bigint` to avoid touching callers), and the partial indexes as they are.
2. Replace the two pools with one `pgxpool`, and make write transactions `SERIALIZABLE` or lock the monitor's state row (`SELECT ... FOR UPDATE`), because the engine's single mutex only serializes one process. With several servers, the scheduler and lease queue also need a home: either one elected leader runs them, or jobs move into a table leased with `FOR UPDATE SKIP LOCKED`.
3. Extract the store's method set into an interface at the engine boundary, run the existing store tests against both implementations, and add a `vigil migrate --from sqlite --to postgres` command that copies rows table by table.

The rollup sketches are opaque bytes and move unchanged.
