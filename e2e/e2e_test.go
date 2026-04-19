//go:build e2e

// Package e2e contains end-to-end tests for the HelixObs gateway.
//
// Tests require the stack defined in e2e/docker-compose.yml to be running.
// Run via the e2e CI workflow or locally:
//
//	docker compose -f e2e/docker-compose.yml up -d --build --wait
//	go test -v -tags e2e -timeout 60s ./e2e/
//	docker compose -f e2e/docker-compose.yml down -v
package e2e_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// ── Config from env ───────────────────────────────────────────────────────────

func gatewayAddr() string { return envOr("GATEWAY_ADDR", "localhost:4317") }
func metricsURL() string  { return envOr("METRICS_URL", "http://localhost:2112/metrics") }
func apiURL() string      { return envOr("API_URL", "http://localhost:8080") }
func dbURL() string       { return envOr("TEST_DB_URL", "postgres://helix:helix@localhost:5432/helixobs") }

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ── TestMain: wait for services ───────────────────────────────────────────────

func TestMain(m *testing.M) {
	fmt.Println("e2e: waiting for gateway...")
	if err := waitForURL(metricsURL(), 30*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: metrics not ready: %v\n", err)
		os.Exit(1)
	}
	if err := waitForURL(apiURL()+"/", 10*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: api not ready: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("e2e: stack ready")
	os.Exit(m.Run())
}

func waitForMetrics(timeout time.Duration) error { return waitForURL(metricsURL(), timeout) }

func waitForURL(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s after %s", url, timeout)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestEntityWrittenToDBAfterHelixSpan(t *testing.T) {
	entityID := fmt.Sprintf("e2e-entity-%d", rand.Int63())

	sendHelixSpan(t, entityID, "CHIME", "correlator", nil)

	ctx := context.Background()
	conn := mustConnectDB(t)
	defer conn.Close(ctx)

	pollUntil(t, 10*time.Second, func() bool {
		var id string
		err := conn.QueryRow(ctx,
			"SELECT id FROM entities WHERE id = $1", entityID,
		).Scan(&id)
		return err == nil
	}, "entity %q never appeared in DB", entityID)
}

func TestEntityParentIDsStoredInDB(t *testing.T) {
	parentID := fmt.Sprintf("e2e-parent-%d", rand.Int63())
	childID  := fmt.Sprintf("e2e-child-%d",  rand.Int63())

	sendHelixSpan(t, parentID, "CHIME", "x-engine", nil)
	sendHelixSpan(t, childID,  "CHIME", "classifier", []string{parentID})

	ctx := context.Background()
	conn := mustConnectDB(t)
	defer conn.Close(ctx)

	pollUntil(t, 10*time.Second, func() bool {
		var parentIDs []string
		err := conn.QueryRow(ctx,
			"SELECT parent_ids FROM entities WHERE id = $1", childID,
		).Scan(&parentIDs)
		if err != nil {
			return false
		}
		for _, pid := range parentIDs {
			if pid == parentID {
				return true
			}
		}
		return false
	}, "parent_id %q not found on child entity %q", parentID, childID)
}

func TestEntitiesTotalCounterIncremented(t *testing.T) {
	entityID := fmt.Sprintf("e2e-metric-%d", rand.Int63())

	before := scrapeCounter(t, "helix_entities_total")
	sendHelixSpan(t, entityID, "CHIME", "correlator", nil)

	pollUntil(t, 10*time.Second, func() bool {
		return scrapeCounter(t, "helix_entities_total") > before
	}, "helix_entities_total did not increment")
}

func TestNonHelixSpanPassesThrough(t *testing.T) {
	// A span without helix.entity.id must not appear in the DB.
	sendRawSpan(t, map[string]string{
		"some.other.attr": "value",
	})
	// Give the gateway time to process.
	time.Sleep(2 * time.Second)

	ctx := context.Background()
	conn := mustConnectDB(t)
	defer conn.Close(ctx)

	var count int
	conn.QueryRow(ctx,
		"SELECT COUNT(*) FROM entities WHERE instrument_id = 'NON_HELIX'",
	).Scan(&count)
	if count != 0 {
		t.Errorf("expected no rows for non-helix span, got %d", count)
	}
}

// ── OTLP helpers ──────────────────────────────────────────────────────────────

func sendHelixSpan(t *testing.T, entityID, instrumentID, stage string, parentIDs []string) {
	t.Helper()
	attrs := map[string]string{
		"helix.entity.id":     entityID,
		"helix.instrument.id": instrumentID,
	}
	if len(parentIDs) > 0 {
		attrs["helix.parent.ids"] = strings.Join(parentIDs, ",")
	}
	sendRawSpan(t, attrs)
}

func sendRawSpan(t *testing.T, attrs map[string]string) {
	t.Helper()
	conn, err := grpc.NewClient(gatewayAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	client := collectortracepb.NewTraceServiceClient(conn)

	var traceID [16]byte
	var spanID  [8]byte
	rand.Read(traceID[:])
	rand.Read(spanID[:])

	pbAttrs := make([]*commonpb.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		pbAttrs = append(pbAttrs, &commonpb.KeyValue{
			Key:   k,
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
		})
	}

	req := &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					TraceId:           traceID[:],
					SpanId:            spanID[:],
					Name:              "e2e-span",
					StartTimeUnixNano: uint64(time.Now().UnixNano()),
					EndTimeUnixNano:   uint64(time.Now().UnixNano() + 1_000_000),
					Attributes:        pbAttrs,
				}},
			}},
		}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

// ── DB helpers ────────────────────────────────────────────────────────────────

func mustConnectDB(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL())
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	return conn
}

// ── Prometheus helpers ────────────────────────────────────────────────────────

func scrapeCounter(t *testing.T, metricName string) float64 {
	t.Helper()
	resp, err := http.Get(metricsURL())
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var total float64
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, metricName+"{") || line == metricName+" " {
			fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%f", &total)
			total += 0 // accumulate all label combinations
		}
	}
	return total
}

// ── Poll helper ───────────────────────────────────────────────────────────────

func pollUntil(t *testing.T, timeout time.Duration, cond func() bool, msgFmt string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf(msgFmt, args...)
}

