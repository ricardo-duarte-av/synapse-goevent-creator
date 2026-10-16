// Command goevent-creator serves the client endpoints that create room
// events (send, state, redact, membership, join, knock, profile), in place of
// Synapse's Python event_creator worker, and proxies to that worker whatever
// it does not serve itself.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/allowlist"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/config"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/proxy"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/server"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/synapsecfg"
)

// Set at build time.
var (
	tag       = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	configPath := flag.String("config", "/data/goevent-creator.yaml", "path to the worker config")
	probe := flag.Bool("healthcheck", false, "probe the running worker's /health and exit")
	version := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *version {
		fmt.Printf("goevent-creator %s (%s, built %s)\n", tag, commit, buildTime)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if *probe {
		if err := healthcheck(cfg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	log, err := newLogger(cfg.Log)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := run(cfg, log); err != nil {
		log.Fatal().Err(err).Msg("Worker stopped")
	}
}

func run(cfg *config.Config, log zerolog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	modes := zerolog.Dict()
	for ep, m := range cfg.Modes {
		modes.Str(string(ep), string(m))
	}
	log.Info().Str("version", tag).Str("commit", commit).Dict("modes", modes).
		Msg("Starting goevent-creator")

	rooms, err := allowlist.New(cfg.Rooms)
	if err != nil {
		return err
	}

	// Proxy mode needs only the upstream, and does not insist on
	// homeserver.yaml: it is the first thing deployed. Reading it anyway
	// shows at once whether the mount is right.
	if hs, err := synapsecfg.Load(cfg.HomeserverYAML); err != nil {
		log.Warn().Err(err).Msg("Cannot read homeserver.yaml; fine in proxy mode, required for any other")
	} else {
		log.Info().Str("server_name", hs.ServerName).
			Strs("events_writers", hs.Writers.Events).Msg("Read homeserver.yaml")
	}

	up, err := proxy.New(cfg.Upstream, log)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	mux.Handle("/", server.New(cfg, up, rooms, log))

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No write timeout: a proxied remote join can take minutes, and the
		// upstream timeout bounds it.
	}
	ln, err := listen(cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	// Profiles, on the metrics listener only: it is reachable from the
	// container networks, never from clients.
	metricsMux.HandleFunc("/debug/pprof/", pprof.Index)
	metricsMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	metricsMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	metricsMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	metricsMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	metricsSrv := &http.Server{Addr: cfg.Metrics.Addr, Handler: metricsMux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Metrics server failed")
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		log.Info().Str("socket", cfg.Listen.Socket).Str("addr", cfg.Listen.Addr).
			Strs("upstreams", up.Backends()).Strs("rooms", rooms.Rooms()).Msg("Serving")
		errCh <- srv.Serve(ln)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info().Msg("Shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	return srv.Shutdown(shutdownCtx)
}

func newLogger(cfg config.Log) (zerolog.Logger, error) {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		return zerolog.Nop(), fmt.Errorf("log: %w", err)
	}
	logger := zerolog.New(os.Stdout).Level(level).With().Timestamp().Logger()
	if cfg.Pretty {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"})
	}
	return logger, nil
}

// listen opens the configured listener. For a unix socket it removes a stale
// socket left by an unclean shutdown and applies the configured permissions so
// nginx can connect.
func listen(cfg config.Listen) (net.Listener, error) {
	if cfg.Addr != "" {
		return net.Listen("tcp", cfg.Addr)
	}
	if err := os.Remove(cfg.Socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket %s: %w", cfg.Socket, err)
	}
	l, err := net.Listen("unix", cfg.Socket)
	if err != nil {
		return nil, err
	}
	mode, err := cfg.ParsedSocketMode()
	if err != nil {
		_ = l.Close()
		return nil, err
	}
	if err := os.Chmod(cfg.Socket, mode); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("chmod socket: %w", err)
	}
	return l, nil
}
