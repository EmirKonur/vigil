// Command vigil is a self-hosted uptime monitor with multi-region probes.
//
//	vigil server               run the API, scheduler, dashboard, and status pages
//	vigil probe --region=NAME  run a remote probe that leases checks from a server
//	vigil demo                 seed 90 days of synthetic history and serve it
//	vigil validate             check a vigil.yaml without starting anything
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"vigil/internal/check"
	"vigil/internal/config"
	"vigil/internal/demo"
	"vigil/internal/probe"
	"vigil/internal/server"
	"vigil/internal/store"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `Vigil %s: uptime monitoring with multi-region consensus.

Usage:
  vigil server        Run the server (API, scheduler, dashboard, status pages)
  vigil probe         Run a remote probe for one region
  vigil demo          Seed 90 days of synthetic history and serve it locally
  vigil demo-target   Run the deliberately flaky demo target (docker compose)
  vigil validate      Validate a config file
  vigil healthcheck   Exit 0 if a server answers /healthz (for containers)
  vigil version       Print the version

Run "vigil <command> -h" for flags. Every flag also reads an environment
variable, shown in its help text.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, version)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "server":
		err = runServer(ctx, args)
	case "probe":
		err = runProbe(ctx, args)
	case "demo":
		err = runDemo(ctx, args)
	case "demo-target":
		err = runDemoTarget(ctx, args)
	case "validate":
		err = runValidate(args)
	case "healthcheck":
		err = runHealthcheck(args)
	case "version", "--version", "-v":
		fmt.Println(version)
	case "help", "-h", "--help":
		fmt.Printf(usage, version)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n"+usage, cmd, version)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "vigil:", err)
		os.Exit(1)
	}
}

// env returns the environment value for key, or fallback.
func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v, err := strconv.ParseBool(env(key, strconv.FormatBool(fallback)))
	if err != nil {
		return fallback
	}
	return v
}

func newLogger() *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(env("VIGIL_LOG_LEVEL", "info"))); err != nil {
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if env("VIGIL_LOG_FORMAT", "text") == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(h)
}

func randomToken() string {
	var b [24]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func runServer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", env("VIGIL_ADDR", ":4210"), "listen address (VIGIL_ADDR)")
	cfgPath := fs.String("config", env("VIGIL_CONFIG", "vigil.yaml"), "config file (VIGIL_CONFIG)")
	dbPath := fs.String("db", env("VIGIL_DB", "vigil.db"), "SQLite database path (VIGIL_DB)")
	localRegion := fs.String("local-region", env("VIGIL_LOCAL_REGION", config.LocalRegion), `region of the embedded probe, or "off" (VIGIL_LOCAL_REGION)`)
	baseURL := fs.String("base-url", env("VIGIL_BASE_URL", ""), "public URL, used for links in alerts (VIGIL_BASE_URL)")
	watch := fs.Bool("watch", envBool("VIGIL_WATCH_CONFIG", true), "reload the config when the file changes (VIGIL_WATCH_CONFIG)")
	_ = fs.Parse(args)

	log := newLogger()
	adminToken := os.Getenv("VIGIL_ADMIN_TOKEN")
	if adminToken == "" {
		adminToken = randomToken()
		log.Warn("VIGIL_ADMIN_TOKEN is not set; generated a token for this run", "token", adminToken)
	}
	probeToken := os.Getenv("VIGIL_PROBE_TOKEN")
	if probeToken == "" {
		log.Info("VIGIL_PROBE_TOKEN is not set; only the embedded probe will run checks")
	}
	region := *localRegion
	if region == "off" {
		region = ""
	}
	return server.Run(ctx, server.Options{
		Addr: *addr, ConfigPath: *cfgPath, DBPath: *dbPath, AdminToken: adminToken, ProbeToken: probeToken,
		LocalRegion: region, BaseURL: *baseURL, Version: version, Log: log, WatchConfig: *watch,
	})
}

func runProbe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	serverURL := fs.String("server", env("VIGIL_SERVER", "http://127.0.0.1:4210"), "server URL (VIGIL_SERVER)")
	region := fs.String("region", env("VIGIL_REGION", ""), "region this probe checks from (VIGIL_REGION)")
	id := fs.String("id", env("VIGIL_PROBE_ID", ""), "probe ID, unique per probe (VIGIL_PROBE_ID; default hostname-region)")
	concurrency := fs.Int("concurrency", 16, "checks run in parallel")
	_ = fs.Parse(args)

	if *region == "" {
		return errors.New("--region is required")
	}
	token := os.Getenv("VIGIL_PROBE_TOKEN")
	if token == "" {
		return errors.New("VIGIL_PROBE_TOKEN must be set to the server's probe token")
	}
	if *id == "" {
		h, _ := os.Hostname()
		*id = h + "-" + *region
	}
	log := newLogger()
	ua := server.UserAgent(version, *region)
	p := probe.New(probe.Options{
		API: probe.NewClient(*serverURL, token, ua), ID: *id, Region: *region, Version: version,
		Concurrency: *concurrency, Runner: &check.Runner{UserAgent: ua}, Log: log,
	})
	log.Info("probe starting", "server", *serverURL, "region", *region, "id", *id)
	return p.Run(ctx)
}

func runDemo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	addr := fs.String("addr", env("VIGIL_ADDR", "127.0.0.1:4210"), "listen address (VIGIL_ADDR)")
	dbPath := fs.String("db", env("VIGIL_DB", "vigil-demo.db"), "demo database, recreated on every run (VIGIL_DB)")
	targetHost := fs.String("target-host", "127.0.0.1", "interface for the demo targets")
	targetPort := fs.Int("target-port", 4211, "first port for the demo targets (HTTP; TCP, DNS, and TLS use the next three after 4212)")
	_ = fs.Parse(args)

	log := newLogger()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(*dbPath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	start := time.Now()
	targets, err := demo.StartTargets(demo.TargetOptions{
		HTTPAddr: fmt.Sprintf("%s:%d", *targetHost, *targetPort),
		TCPAddr:  fmt.Sprintf("%s:%d", *targetHost, *targetPort+2),
		DNSAddr:  fmt.Sprintf("%s:%d", *targetHost, *targetPort+3),
		TLSAddr:  fmt.Sprintf("%s:%d", *targetHost, *targetPort+4),
		Start:    start, SearchOutage: demo.SearchOutage, Log: log.With("component", "demo-target"),
	})
	if err != nil {
		return fmt.Errorf("starting demo targets: %w", err)
	}
	defer targets.Close()

	cfg, err := demo.Config(targets)
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	stats, err := demo.Seed(ctx, st, cfg, start, demo.Options{})
	if err != nil {
		return fmt.Errorf("seeding: %w", err)
	}
	log.Info("seeded synthetic history", "db", *dbPath, "incidents", stats.Incidents, "results", stats.Results, "rollups", stats.Rollups, "took", time.Since(start).Round(time.Millisecond))

	adminToken := env("VIGIL_ADMIN_TOKEN", "demo")
	probeToken := randomToken()
	log.Info("demo ready: open the status page, or sign in to the dashboard", "status", "http://"+*addr+"/", "dashboard", "http://"+*addr+"/admin", "admin_token", adminToken)
	return server.Run(ctx, server.Options{
		Addr: *addr, ConfigPath: "(built-in demo config)", AdminToken: adminToken, ProbeToken: probeToken,
		Version: version, Demo: true, Log: log, Store: st,
		LoadConfig: func() (*config.Config, error) { return demo.Config(targets) },
		ExtraProbes: func(baseURL string) []*probe.Probe {
			var out []*probe.Probe
			for _, region := range []string{"eu-west", "us-east", "ap-south"} {
				ua := server.UserAgent(version, region)
				out = append(out, probe.New(probe.Options{
					API: probe.NewClient(baseURL, probeToken, ua), ID: region + "-1", Region: region, Version: version,
					Runner: &check.Runner{UserAgent: ua}, Log: log.With("component", "probe"),
				}))
			}
			return out
		},
	})
}

func runDemoTarget(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo-target", flag.ExitOnError)
	addr := fs.String("addr", env("VIGIL_TARGET_ADDR", ":4211"), "HTTP listen address (VIGIL_TARGET_ADDR)")
	cycle := fs.String("cycle", env("VIGIL_TARGET_CYCLE", "3m,2m,2m"), "durations of the healthy, regional-failure, and outage phases of /flaky (VIGIL_TARGET_CYCLE)")
	_ = fs.Parse(args)

	parts := strings.Split(*cycle, ",")
	if len(parts) != 3 {
		return errors.New("--cycle needs three durations: healthy,regional,outage")
	}
	var phases [3]time.Duration
	for i, p := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(p))
		if err != nil {
			return fmt.Errorf("--cycle: %w", err)
		}
		phases[i] = d
	}
	log := newLogger()
	t, err := demo.StartTargets(demo.TargetOptions{HTTPAddr: *addr, Start: time.Now(), Cycle: phases, Log: log})
	if err != nil {
		return err
	}
	log.Info("demo target listening", "addr", t.HTTP, "cycle", *cycle, "endpoints", "/flaky /_scenario /_webhook")
	<-ctx.Done()
	return t.Close()
}

func runValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	cfgPath := fs.String("config", env("VIGIL_CONFIG", "vigil.yaml"), "config file (VIGIL_CONFIG)")
	_ = fs.Parse(args)
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	fmt.Printf("%s is valid: %s in %s, %s, %s\n", *cfgPath,
		count(len(cfg.Monitors), "monitor"), count(len(cfg.Regions), "region"),
		count(len(cfg.Notifiers), "notifier"), count(len(cfg.StatusPages), "status page"))
	return nil
}

func count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}

func runHealthcheck(args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ExitOnError)
	url := fs.String("url", env("VIGIL_HEALTHCHECK_URL", "http://127.0.0.1:4210/healthz"), "URL to probe (VIGIL_HEALTHCHECK_URL)")
	_ = fs.Parse(args)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(*url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %s", *url, resp.Status)
	}
	return nil
}
