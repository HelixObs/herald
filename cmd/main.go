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
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/HelixObs/gateway/internal/api"
	"github.com/HelixObs/gateway/internal/db"
	"github.com/HelixObs/gateway/internal/interceptor"
	"github.com/HelixObs/gateway/internal/metrics"
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
	dbStore, err := db.New(ctx, cfg.dbURL)
	if err != nil {
		return fmt.Errorf("init db: %w", err)
	}
	defer dbStore.Close()

	// ── Interceptor ───────────────────────────────────────────────────
	traceStore := store.New(cfg.traceStoreSize)
	icp := interceptor.New(traceStore, dbStore, m)

	// ── OTLP receiver + downstream forwarder ──────────────────────────
	recv, err := receiver.Dial(icp, cfg.collectorEndpoint)
	if err != nil {
		return fmt.Errorf("init receiver: %w", err)
	}

	// ── gRPC server ───────────────────────────────────────────────────
	grpcSrv := grpc.NewServer()
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
	apiSrv := &http.Server{Addr: cfg.apiAddr, Handler: api.New(dbStore)}
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
}

func configFromEnv() config {
	return config{
		listenAddr:        envOr("GATEWAY_ADDR", ":4317"),
		collectorEndpoint: envOr("COLLECTOR_ENDPOINT", "otel-collector:4317"),
		dbURL:             envOr("DB_URL", "postgres://helix:helix@db:5432/helixobs"),
		metricsAddr:       envOr("METRICS_ADDR", ":2112"),
		apiAddr:           envOr("API_ADDR", ":8080"),
		traceStoreSize:    10_000,
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
