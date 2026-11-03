package server

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const contentSecurityPolicy = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
	"font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"

// middleware adds, from the outside in: panic recovery, metrics and access
// logging, security headers, and cross-origin protection for form posts.
func middleware(log *slog.Logger, reg prometheus.Registerer, next http.Handler) http.Handler {
	requests := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "vigil_http_request_duration_seconds", Help: "HTTP request latency by route and status code.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 10, 30},
	}, []string{"route", "code"})
	reg.MustRegister(requests)

	// Browsers send Sec-Fetch-Site/Origin; a cross-site form post to the
	// dashboard is rejected. Probes and curl send neither and are unaffected.
	csrf := http.NewCrossOriginProtection()
	protected := csrf.Handler(next)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				log.Error("panic serving request", "path", r.URL.Path, "panic", p, "stack", string(debug.Stack()))
				if !rec.wrote {
					http.Error(rec, "internal error", http.StatusInternalServerError)
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			elapsed := time.Since(start)
			requests.WithLabelValues(route, strconv.Itoa(rec.status)).Observe(elapsed.Seconds())
			level := slog.LevelInfo
			if quiet(r.URL.Path) && rec.status < 400 {
				level = slog.LevelDebug
			}
			log.Log(r.Context(), level, "http", "method", r.Method, "path", r.URL.Path, "status", rec.status, "ms", elapsed.Milliseconds())
		}()

		h := w.Header()
		h.Set("Content-Security-Policy", contentSecurityPolicy)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		protected.ServeHTTP(rec, r)
	})
}

// quiet paths are high-volume and only logged at debug level.
func quiet(path string) bool {
	return strings.HasPrefix(path, "/api/v1/probe/") || strings.HasPrefix(path, "/static/") ||
		path == "/metrics" || path == "/healthz" || path == "/readyz" || path == "/admin/events"
}

type recorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *recorder) WriteHeader(code int) {
	if !r.wrote {
		r.status, r.wrote = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	r.wrote = true
	return r.ResponseWriter.Write(b)
}

// Flush keeps server-sent events working through the wrapper.
func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
