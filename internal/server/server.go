// Package server assembles a running Vigil server: storage, engine, HTTP
// routes, the embedded probe, config reloading, and orderly shutdown.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"vigil/internal/check"
	"vigil/internal/clock"
	"vigil/internal/config"
	"vigil/internal/engine"
	"vigil/internal/probe"
	"vigil/internal/store"
	"vigil/internal/web"
)

// Options configures a server.
type Options struct {
	Addr       string
	ConfigPath string
	DBPath     string
	AdminToken string
	// ProbeToken authenticates remote probes. Empty disables the remote
	// probe API; the embedded probe does not need it.
	ProbeToken string
	// LocalRegion names the embedded probe's region. Empty disables it.
	LocalRegion string
	BaseURL     string
	Version     string
	Demo        bool
	Log         *slog.Logger
	Clock       clock.Clock
	// Listener, when set, is used instead of listening on Addr.
	Listener net.Listener
	// Store, when set, is used instead of opening DBPath (demo and tests).
	Store *store.Store
	// LoadConfig, when set, replaces reading ConfigPath (tests).
	LoadConfig func() (*config.Config, error)
	// WatchConfig polls the config file for changes.
	WatchConfig bool
	TickEvery   time.Duration
	RollupEvery time.Duration
	// ExtraProbes are started in-process alongside the embedded probe,
	// talking to this server over HTTP (used by `vigil demo`).
	ExtraProbes func(baseURL string) []*probe.Probe
}

type server struct {
	opts   Options
	log    *slog.Logger
	store  *store.Store
	engine *engine.Engine
	hub    *web.Hub

	mu        sync.Mutex
	cfgStatus web.ConfigStatus
	cfgMTime  time.Time
}

// Run serves until ctx is cancelled, then shuts down in order: stop
// scheduling, let probes finish and report, drain HTTP, flush alerts.
func Run(ctx context.Context, opts Options) error {
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	log := opts.Log
	s := &server{opts: opts, log: log, hub: web.NewHub(time.Second)}
	s.cfgStatus.Path = opts.ConfigPath

	st := opts.Store
	if st == nil {
		var err error
		if st, err = store.Open(ctx, opts.DBPath); err != nil {
			return err
		}
		defer st.Close()
	}
	s.store = st

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	build := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "vigil_build_info", Help: "Build version."}, []string{"version"})
	build.WithLabelValues(opts.Version).Set(1)
	reg.MustRegister(build)

	eng, err := engine.New(ctx, engine.Options{
		Store: st, Clock: opts.Clock, Log: log, Metrics: engine.NewMetrics(reg),
		BaseURL: opts.BaseURL, UserAgent: "Vigil/" + opts.Version, Demo: opts.Demo,
		OnChange: s.hub.Publish, TickEvery: opts.TickEvery, RollupEvery: opts.RollupEvery,
	})
	if err != nil {
		return err
	}
	s.engine = eng
	if err := s.reload(ctx); err != nil {
		return err
	}

	ln := opts.Listener
	if ln == nil {
		if ln, err = net.Listen("tcp", opts.Addr); err != nil {
			return err
		}
	}
	baseURL := "http://" + ln.Addr().String()

	mux := http.NewServeMux()
	web.New(web.Options{
		Engine: eng, Store: st, Clock: opts.Clock, Log: log, Hub: s.hub,
		AdminToken: opts.AdminToken, SecureCookie: strings.HasPrefix(opts.BaseURL, "https://"),
		Version: opts.Version, Demo: opts.Demo, BaseURL: opts.BaseURL,
		Reload: s.reload, Config: s.configStatus,
	}).Register(mux)
	s.registerProbeAPI(mux)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := st.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})

	httpSrv := &http.Server{
		Handler:           middleware(log, reg, mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	engineCtx, stopEngine := context.WithCancel(context.WithoutCancel(ctx))
	defer stopEngine()
	probeCtx, stopProbes := context.WithCancel(context.WithoutCancel(ctx))
	defer stopProbes()

	var workers, probes sync.WaitGroup
	workers.Go(func() { _ = eng.Run(engineCtx) })
	if opts.LocalRegion != "" {
		p := probe.New(probe.Options{
			API: localAPI{eng}, ID: hostname() + "-embedded", Region: opts.LocalRegion, Version: opts.Version,
			Runner: &check.Runner{UserAgent: UserAgent(opts.Version, opts.LocalRegion)}, Log: log,
		})
		probes.Go(func() { _ = p.Run(probeCtx) })
	}
	if opts.ExtraProbes != nil {
		for _, p := range opts.ExtraProbes(baseURL) {
			probes.Go(func() { _ = p.Run(probeCtx) })
		}
	}
	workers.Go(func() { s.watchSignals(engineCtx) })
	if opts.WatchConfig && opts.ConfigPath != "" {
		workers.Go(func() { s.watchConfig(engineCtx) })
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()
	log.Info("vigil server listening", "addr", ln.Addr().String(), "version", opts.Version,
		"embedded_probe", opts.LocalRegion, "remote_probes", opts.ProbeToken != "")

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	log.Info("shutting down")
	s.hub.Close()
	// Stopping the engine closes the job queue, which releases every
	// long-polling probe immediately.
	stopEngine()
	stopProbes()
	probes.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	workers.Wait()
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFlush()
	eng.DeliverPending(flushCtx)
	log.Info("stopped")
	return nil
}

func (s *server) loadConfig() (*config.Config, error) {
	if s.opts.LoadConfig != nil {
		return s.opts.LoadConfig()
	}
	return config.Load(s.opts.ConfigPath)
}

// reload loads and applies the config. A bad file leaves the running
// config in place and is reported on the dashboard.
func (s *server) reload(ctx context.Context) error {
	cfg, err := s.loadConfig()
	if err == nil {
		err = s.engine.Configure(ctx, cfg)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.cfgStatus.LastError = err.Error()
		s.log.Error("config not applied", "path", s.opts.ConfigPath, "err", err)
		return err
	}
	s.cfgStatus.LastError = ""
	s.cfgStatus.LoadedAt = s.opts.Clock.Now()
	if fi, statErr := os.Stat(s.opts.ConfigPath); statErr == nil {
		s.cfgMTime = fi.ModTime()
	}
	s.log.Info("config loaded", "path", s.opts.ConfigPath, "monitors", len(cfg.Monitors), "notifiers", len(cfg.Notifiers), "status_pages", len(cfg.StatusPages))
	return nil
}

func (s *server) configStatus() web.ConfigStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfgStatus
}

func (s *server) watchSignals(ctx context.Context) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			s.log.Info("SIGHUP received, reloading config")
			_ = s.reload(ctx)
		}
	}
}

// watchConfig polls the file's modification time. Polling every few
// seconds is simpler than inotify and works on bind mounts and network
// filesystems, where change notifications are unreliable.
func (s *server) watchConfig(ctx context.Context) {
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fi, err := os.Stat(s.opts.ConfigPath)
			if err != nil {
				continue
			}
			s.mu.Lock()
			changed := !fi.ModTime().Equal(s.cfgMTime)
			s.cfgMTime = fi.ModTime()
			s.mu.Unlock()
			if changed {
				s.log.Info("config file changed, reloading", "path", s.opts.ConfigPath)
				_ = s.reload(ctx)
			}
		}
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "vigil"
	}
	return h
}

// UserAgent is the User-Agent probes send with HTTP checks. It names the
// region so target operators can tell probe traffic apart.
func UserAgent(version, region string) string {
	return fmt.Sprintf("Vigil/%s (uptime probe; region=%s)", version, region)
}
