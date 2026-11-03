// Package web serves the operator dashboard and the public status pages.
//
// Pages are server-rendered with html/template. Live updates use htmx: the
// dashboard listens on a server-sent event stream whose events are only
// invalidation signals ("monitors changed"), and each panel re-fetches its
// own page and swaps itself in via hx-select. There are no JSON view
// endpoints to keep in sync with the templates.
package web

import (
	"context"
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"vigil/internal/clock"
	"vigil/internal/engine"
	"vigil/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

// ConfigStatus describes the loaded vigil.yaml for the dashboard.
type ConfigStatus struct {
	Path      string
	LoadedAt  time.Time
	LastError string
}

// Options configures the handlers.
type Options struct {
	Engine       *engine.Engine
	Store        *store.Store
	Clock        clock.Clock
	Log          *slog.Logger
	Hub          *Hub
	AdminToken   string
	SecureCookie bool
	Version      string
	Demo         bool
	BaseURL      string
	// Reload re-reads vigil.yaml.
	Reload func(context.Context) error
	// Config reports the state of the last load.
	Config func() ConfigStatus
}

// Handler serves every UI route.
type Handler struct {
	opts  Options
	auth  *auth
	views *views
	log   *slog.Logger
}

// New builds the handler.
func New(opts Options) *Handler {
	v := loadViews(opts.Version)
	v.demo = opts.Demo
	return &Handler{
		opts:  opts,
		auth:  newAuth(opts.AdminToken, opts.SecureCookie),
		views: v,
		log:   opts.Log,
	}
}

// Register adds the UI routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	static, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", cacheStatic(http.FileServerFS(static))))
	mux.HandleFunc("GET /favicon.svg", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, static, "favicon.svg")
	})

	mux.HandleFunc("GET /{$}", h.root)
	mux.HandleFunc("GET /status/{slug}", h.statusPage)
	mux.HandleFunc("GET /status/{slug}/history", h.statusHistory)
	mux.HandleFunc("GET /status/{slug}/incidents/{id}", h.statusIncident)
	mux.HandleFunc("GET /status/{slug}/feed.atom", h.feed)

	mux.HandleFunc("GET /admin/login", h.loginPage)
	mux.HandleFunc("POST /admin/login", h.login)
	mux.HandleFunc("POST /admin/logout", h.logout)

	admin := func(pattern string, fn http.HandlerFunc) { mux.Handle(pattern, h.requireAdmin(fn)) }
	admin("GET /admin", h.overview)
	admin("GET /admin/monitors/{id}", h.monitor)
	admin("POST /admin/monitors/{id}/pause", h.pause(true))
	admin("POST /admin/monitors/{id}/resume", h.pause(false))
	admin("GET /admin/incidents", h.incidents)
	admin("GET /admin/incidents/{id}", h.incident)
	admin("POST /admin/incidents/{id}/updates", h.postUpdate)
	admin("GET /admin/probes", h.probes)
	admin("POST /admin/reload", h.reload)
	admin("GET /admin/events", h.events)

	mux.HandleFunc("/", h.notFound)
}

func (h *Handler) root(w http.ResponseWriter, r *http.Request) {
	if cfg := h.opts.Engine.Config(); cfg != nil && len(cfg.StatusPages) > 0 {
		http.Redirect(w, r, "/status/"+cfg.StatusPages[0].Slug, http.StatusFound)
		return
	}
	http.Redirect(w, r, "/admin", http.StatusFound)
}

func (h *Handler) notFound(w http.ResponseWriter, r *http.Request) {
	h.views.error(w, r, http.StatusNotFound, "No signal", "Nothing is reporting at this address.")
}

func (h *Handler) serverError(w http.ResponseWriter, r *http.Request, err error) {
	h.log.Error("request failed", "path", r.URL.Path, "err", err)
	h.views.error(w, r, http.StatusInternalServerError, "Fault", "Something went wrong rendering this page. The error was logged.")
}

// cacheStatic lets browsers keep versioned assets for a year. Templates
// append ?v=<content hash>, so a new build busts the cache.
func cacheStatic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("v") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=300")
		}
		next.ServeHTTP(w, r)
	})
}
