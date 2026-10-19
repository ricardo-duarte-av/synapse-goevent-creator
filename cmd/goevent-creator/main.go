// Command goevent-creator serves the client endpoints that create room
// events (send, state, redact, membership, join, knock, profile), in place of
// Synapse's Python event_creator worker, and proxies whatever it does not
// serve itself to Synapse's main process.
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
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/allowlist"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/auth"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/build"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/config"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/creator"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/dbrole"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pdustore"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/proxy"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/pushcalc"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/replication"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/server"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/shadow"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goevent-creator/internal/writer"
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
	hs, hsErr := synapsecfg.Load(cfg.HomeserverYAML)
	if hsErr != nil {
		if cfg.AnyShadow() {
			return hsErr
		}
		log.Warn().Err(hsErr).Msg("Cannot read homeserver.yaml; fine in proxy mode, required for any other")
	} else {
		log.Info().Str("server_name", hs.ServerName).
			Strs("events_writers", hs.Writers.Events).Msg("Read homeserver.yaml")
	}

	var (
		checker *shadow.Checker
		native  *creator.Creator
	)
	if cfg.AnyShadow() {
		d, err := connect(ctx, cfg, hs, log)
		if err != nil {
			return err
		}
		defer d.close()
		checker = d.checker(cfg, hs, log)
		if cfg.AnyNative() {
			if native, err = d.creator(ctx, hs, log); err != nil {
				return err
			}
		}
	}

	up, err := proxy.New(cfg.Upstream, log)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	mux.Handle("/", server.New(cfg, up, rooms, checker, native, log))

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

// deps are what shadow and native mode share.
type deps struct {
	db      *pdustore.Store
	key     *build.Key
	authn   *auth.Authenticator
	push    *pushcalc.Calculator
	closers []func()
}

func (d *deps) close() {
	for i := len(d.closers) - 1; i >= 0; i-- {
		d.closers[i]()
	}
}

// direct is homeserver.yaml's database, reached on PostgreSQL's own socket:
// homeserver.yaml's host is pgcat here, which only knows its own users.
func direct(cfg *config.Config, hs *synapsecfg.Config) (synapsecfg.Database, error) {
	if !hs.Database.Present {
		return synapsecfg.Database{}, errors.New("no database: set database.dsn, or give homeserver.yaml a database block")
	}
	d := hs.Database
	d.Host = cfg.Database.Host
	if d.Host == "" {
		d.Host = "/var/sockets"
	}
	d.Port = 0
	return d, nil
}

// connect opens what shadow mode reads: the database, read-only, the signing
// key, whoami and the push rules.
func connect(ctx context.Context, cfg *config.Config, hs *synapsecfg.Config, log zerolog.Logger) (*deps, error) {
	d := &deps{}
	dsn := cfg.Database.DSN
	if dsn == "" {
		db, err := direct(cfg, hs)
		if err != nil {
			return nil, err
		}
		dsn = db.DSN()
	}
	// Server-enforced: this session cannot write. Writes, in native mode,
	// go through their own role (package writer).
	dsn += " default_transaction_read_only=on"
	maxConns := int32(cfg.Database.MaxConns)
	if maxConns == 0 {
		maxConns = 4
	}
	db, err := pdustore.Open(ctx, dsn, maxConns)
	if err != nil {
		return nil, err
	}
	d.db = db
	d.closers = append(d.closers, db.Close)
	ro, err := db.ReadOnly(ctx)
	if err != nil || !ro {
		d.close()
		return nil, fmt.Errorf("database session is not read-only (%v); refusing to start", err)
	}
	log.Info().Str("database", redactDSN(dsn)).Msg("Connected to the database, read-only")

	line, err := hs.SigningKeyLine()
	if err != nil {
		d.close()
		return nil, err
	}
	if d.key, err = build.ParseKey(hs.ServerName, line); err != nil {
		d.close()
		return nil, err
	}
	log.Info().Str("key_id", string(d.key.ID)).Msg("Loaded the signing key")

	whoami := cfg.Auth.WhoamiSocket
	if whoami == "" {
		whoami = cfg.Upstream.Sockets[0]
	}
	ttl := time.Duration(cfg.Auth.CacheSeconds) * time.Second
	if ttl == 0 {
		ttl = time.Minute
	}
	if d.authn, err = auth.New(auth.Config{
		WhoamiSocket: whoami,
		PositiveTTL:  ttl,
		NegativeTTL:  10 * time.Second,
		MaxEntries:   1024,
		Timeout:      10 * time.Second,
	}); err != nil {
		d.close()
		return nil, err
	}

	pushCfg, err := pushcalc.ConfigFor(hs)
	if err != nil {
		d.close()
		return nil, err
	}
	d.push = pushcalc.New(db, pushCfg)
	return d, nil
}

func (d *deps) checker(cfg *config.Config, hs *synapsecfg.Config, log zerolog.Logger) *shadow.Checker {
	return shadow.New(shadow.Options{
		Store:       d.db,
		Auth:        d.authn,
		Key:         d.key,
		Push:        d.push,
		ServerName:  hs.ServerName,
		Sticky:      hs.Flag("msc4354_enabled", false),
		Concurrency: cfg.Shadow.Concurrency,
		Timeout:     time.Duration(cfg.Shadow.TimeoutSeconds) * time.Second,
		Log:         log,
	})
}

// creator connects what native mode writes with: the write role, created or
// repaired with homeserver.yaml's credentials, and replication to the events
// writer.
func (d *deps) creator(ctx context.Context, hs *synapsecfg.Config, log zerolog.Logger) (*creator.Creator, error) {
	if hs.ReplicationSecret == "" {
		log.Warn().Msg("No worker_replication_secret: replication requests are sent without one")
	}
	for _, w := range hs.Writers.Events {
		if _, ok := hs.Instance(w); !ok {
			return nil, fmt.Errorf("events writer %q is not in instance_map", w)
		}
	}
	admin, err := direct(&config.Config{}, hs)
	if err != nil {
		return nil, err
	}
	password := dbrole.Password(admin.Password, writer.Role)
	ensureCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = dbrole.Ensure(ensureCtx, admin.DSN(), writer.Spec(password))
	cancel()
	if err != nil {
		return nil, fmt.Errorf("write role %s: %w", writer.Role, err)
	}
	role := admin
	role.User, role.Password = writer.Role, password
	w, err := writer.Open(ctx, role.DSN(), 4)
	if err != nil {
		return nil, fmt.Errorf("write role %s: %w", writer.Role, err)
	}
	d.closers = append(d.closers, w.Close)
	log.Info().Str("role", writer.Role).Msg("Connected as the write role")

	repl := replication.NewClient(hs)
	return creator.New(creator.Options{
		Store:        d.db,
		Writer:       w,
		Persister:    repl,
		Main:         repl,
		Auth:         d.authn,
		Key:          d.key,
		Push:         d.push,
		ServerName:   hs.ServerName,
		EventsWriter: hs.Writers.EventsWriter,
		Sticky:       hs.Flag("msc4354_enabled", false),
		Log:          log,

		RedactionAllowedPeriod: hs.RedactionAllowedPeriod,
		Membership:             hs.Membership,
		MSC4293:                hs.Flag("msc4293_enabled", false),
		MSC4155:                hs.Flag("msc4155_enabled", false),
	}), nil
}

// redactDSN hides a password in a DSN, for logs.
func redactDSN(dsn string) string {
	fields := strings.Fields(dsn)
	for i, f := range fields {
		if strings.HasPrefix(f, "password=") {
			fields[i] = "password=***"
		}
	}
	return strings.Join(fields, " ")
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
