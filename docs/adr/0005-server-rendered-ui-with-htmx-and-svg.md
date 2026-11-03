# 0005: Server-rendered UI with htmx and SVG

Status: accepted

## Context

Vigil has two UIs: a public status page that must load fast, work without JavaScript, and be cacheable, and an operator dashboard that should update live. The project is a single Go binary, and a JavaScript build pipeline would be the largest moving part in the repository.

## Decision

Render everything with `html/template` and embed templates, CSS, fonts, and scripts with `embed`. Draw charts (uptime bars, latency lines with incident bands and failure lanes, sparklines) as SVG strings on the server, styled by CSS classes so one SVG works in light and dark mode. Use htmx for live updates: the dashboard listens on a server-sent event stream whose events carry no data, only "this topic changed", and each panel re-fetches its own page and swaps itself in with `hx-select`. Status pages poll once a minute. About 200 lines of hand-written JavaScript add the theme toggle, keyboard shortcuts, the monitor filter, and chart hover readouts; every value they reveal is also in a table or label.

A strict Content Security Policy (`script-src 'self'; style-src 'self'`) applies to every page. Templates contain no inline scripts or style attributes.

## Alternatives considered

- **An SPA (React, Svelte) talking to a JSON API.** Two representations of every view, a node toolchain in CI, and a status page that is blank without JavaScript.
- **A chart library in the browser.** Charts would need JSON endpoints and client rendering; server SVG renders in the first response, prints, and works in feed readers and screenshots.
- **WebSockets for live updates.** Bidirectional when only one direction is needed; SSE reconnects by itself and passes through proxies as plain HTTP.

## Consequences

The SSE invalidation model means a refresh renders the same HTML as a full load, so there is no second rendering path to keep in sync. The cost is that a refresh re-renders a whole page server side to extract one panel; at dashboard scale that is a few milliseconds, and panels throttle refreshes (2 to 15 seconds). Charts are not interactive beyond hover and keyboard stepping, which is enough for an operator view.
