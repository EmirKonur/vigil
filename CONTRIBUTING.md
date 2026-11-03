# Contributing

## Setup

You need Go 1.27 or newer. Docker is only needed for `make up`.

```sh
make test     # go test -race ./...
make lint     # gofmt, go vet, staticcheck (pinned in the Makefile)
make demo     # seeded demo on http://127.0.0.1:4210 (dashboard token: demo)
make up       # server, three probes, flaky target, Mailpit
make help     # every target
```

## Conventions

- Keep the consensus package pure: no I/O, no `time.Now`. New behavior there starts as a row in the `TestStateMachine` table, and should not break the randomized invariants in `TestInvariantsUnderRandomInput`.
- Anything that waits on time takes a `clock.Clock`, so tests can use `clock.Fake`.
- SQL lives in `internal/store` only. Schema changes are new numbered files in `internal/store/migrations`; never edit a migration that has shipped.
- Config changes need a validation rule with a clear message, an entry in `examples/vigil.yaml`, and a case in `internal/config/config_test.go`.
- UI changes must work without JavaScript, keep the CSP (no inline scripts or `style` attributes), and use the tokens in `vigil.css`. Check light and dark mode, and keyboard focus.
- Comments explain why. Log with `slog` key-value pairs.

## Pull requests

Describe the problem first, then the change. If a decision is worth remembering, add an ADR in `docs/adr`.
