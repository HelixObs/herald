package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/HelixObs/gateway/internal/api"
	"github.com/HelixObs/gateway/internal/auth"
	"github.com/HelixObs/gateway/internal/db"
	"github.com/HelixObs/gateway/internal/interceptor"
	"github.com/HelixObs/gateway/internal/metrics"
	"github.com/HelixObs/gateway/internal/notifier"
	notifiercfg "github.com/HelixObs/gateway/internal/notifier/config"
	ghbackend "github.com/HelixObs/gateway/internal/notifier/github"
	slackbackend "github.com/HelixObs/gateway/internal/notifier/slack"
	"github.com/HelixObs/gateway/internal/notifier/silence"
	"github.com/HelixObs/gateway/internal/receiver"
	"github.com/HelixObs/gateway/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}

func run() error {
	cfg := configFromEnv()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// ── Prometheus ────────────────────────────────────────────────────
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	// ── TimescaleDB ───────────────────────────────────────────────────
	dbStore, err := db.New(ctx, cfg.dbURL, m)
	if err != nil {
		return fmt.Errorf("init db: %w", err)
	}
	defer dbStore.Close()

	// ── Notifier ─────────────────────────────────────────────────────────
	cfgLoader := notifiercfg.New(
		cfg.instrumentsDir,
		time.Duration(cfg.notifierMinSlackWindowSecs)*time.Second,
		time.Duration(cfg.notifierMinGithubWindowSecs)*time.Second,
	)
	cfgLoader.Start(ctx, time.Duration(cfg.notifierReloadIntervalSecs)*time.Second)

	silenceStore := silence.New(dbStore, 60*time.Second)

	n := notifier.New(
		cfgLoader,
		silenceStore,
		m,
		cfg.notifierChannelBuffer,
		cfg.uiBaseURL,
		cfg.grafanaURL,
	)
	n.RegisterMessaging("slack", slackbackend.New())
	n.RegisterSCM("github", ghbackend.New(dbStore))
	go n.Start(ctx)

	// ── Auth ──────────────────────────────────────────────────────────
	// JWT_SECRET: comma-separated list of signing secrets.
	// First key signs new tokens; all keys validate (zero-downtime rotation).
	// Empty = auth disabled (dev mode / safe rollout).
	jwtSecrets := strings.Split(cfg.jwtSecret, ",")
	issuer := auth.NewIssuer(jwtSecrets)
	if issuer.Enabled() {
		slog.Info("auth enabled", "keys", len(jwtSecrets))
	} else {
		slog.Warn("auth disabled — set JWT_SECRET to enable enforcement")
	}
	authConfigs := auth.NewConfigStore(cfg.instrumentsDir)
	authConfigs.Start(ctx, 30*time.Second)

	// ── Interceptor ───────────────────────────────────────────────────
	traceStore := store.New(cfg.traceStoreSize, m)
	icp := interceptor.New(traceStore, dbStore, m)
	icp.WithNotifier(n)

	// ── OTLP receiver + downstream forwarder ──────────────────────────
	recv, err := receiver.Dial(icp, cfg.collectorEndpoint)
	if err != nil {
		return fmt.Errorf("init receiver: %w", err)
	}

	// ── gRPC server ───────────────────────────────────────────────────
	// UnaryInterceptor validates Bearer tokens on gRPC calls when auth is enabled.
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(issuer)))
	collectortracepb.RegisterTraceServiceServer(grpcSrv, recv)
	reflection.Register(grpcSrv) // lets grpcurl explore the service

	lis, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.listenAddr, err)
	}
	slog.Info("gateway listening", "addr", cfg.listenAddr, "collector", cfg.collectorEndpoint)

	// ── Prometheus HTTP ───────────────────────────────────────────────
	promMux := http.NewServeMux()
	promMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	promSrv := &http.Server{Addr: cfg.metricsAddr, Handler: promMux}
	go func() {
		slog.Info("metrics listening", "addr", cfg.metricsAddr)
		if err := promSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()

	// ── API HTTP ──────────────────────────────────────────────────────
	// /auth/token is public (issues tokens); all /api/v1/* require Bearer auth.
	apiMux := http.NewServeMux()
	apiMux.Handle("/auth/token", auth.NewTokenHandler(authConfigs, issuer))
	apiMux.Handle("/", auth.BearerAuth(issuer, api.New(dbStore, dbStore, m)))
	apiSrv := &http.Server{Addr: cfg.apiAddr, Handler: apiMux}
	go func() {
		slog.Info("api listening", "addr", cfg.apiAddr)
		if err := apiSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("api server error", "error", err)
		}
	}()

	// ── Serve ─────────────────────────────────────────────────────────
	errCh := make(chan error, 1)
	go func() { errCh <- grpcSrv.Serve(lis) }()

	select {
	case <-ctx.Done():
		slog.Info("shutting down...")
		grpcSrv.GracefulStop()
		promSrv.Shutdown(context.Background())  //nolint:errcheck
		apiSrv.Shutdown(context.Background())   //nolint:errcheck
		return nil
	case err := <-errCh:
		return fmt.Errorf("grpc serve: %w", err)
	}
}

// ── Config ────────────────────────────────────────────────────────────────────

type config struct {
	listenAddr        string
	collectorEndpoint string
	dbURL             string
	metricsAddr       string
	apiAddr           string
	traceStoreSize    int

	// Auth
	jwtSecret string // comma-separated signing secrets; empty = auth disabled

	// Notifier
	instrumentsDir              string
	notifierReloadIntervalSecs  int
	notifierMinSlackWindowSecs  int
	notifierMinGithubWindowSecs int
	notifierChannelBuffer       int
	uiBaseURL                   string
	grafanaURL                  string
}

func configFromEnv() config {
	return config{
		listenAddr:        envOr("GATEWAY_ADDR", ":4317"),
		collectorEndpoint: envOr("COLLECTOR_ENDPOINT", "otel-collector:4317"),
		dbURL:             envOr("DB_URL", "postgres://helix:helix@db:5432/helixobs"),
		metricsAddr:       envOr("METRICS_ADDR", ":2112"),
		apiAddr:           envOr("API_ADDR", ":8080"),
		traceStoreSize:    10_000,

		jwtSecret: os.Getenv("JWT_SECRET"), // empty = dev mode (no enforcement)

		instrumentsDir:              envOr("INSTRUMENTS_DIR", "/instruments"),
		notifierReloadIntervalSecs:  30,
		notifierMinSlackWindowSecs:  5,
		notifierMinGithubWindowSecs: 60,
		notifierChannelBuffer:       1000,
		uiBaseURL:                   envOr("UI_BASE_URL", "http://localhost:8081"),
		grafanaURL:                  envOr("GRAFANA_URL", "http://localhost:3001"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
