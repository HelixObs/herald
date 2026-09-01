package platformcheck

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"

	"github.com/HelixObs/herald/internal/metrics"
)

// ── Mocks ──────────────────────────────────────────────────────────────────

// mockTraces records every ExportTraceServiceRequest it receives, mirroring
// how Herald's real Receiver.Export is called (in-process, no network).
type mockTraces struct {
	mu       sync.Mutex
	received []*collectortracepb.ExportTraceServiceRequest
	err      error
}

func (m *mockTraces) Export(_ context.Context, req *collectortracepb.ExportTraceServiceRequest) (*collectortracepb.ExportTraceServiceResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, req)
	return &collectortracepb.ExportTraceServiceResponse{}, nil
}

func (m *mockTraces) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.received)
}

func (m *mockTraces) last() *collectortracepb.ExportTraceServiceRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.received) == 0 {
		return nil
	}
	return m.received[len(m.received)-1]
}

// mockLogs implements collectorlogspb.LogsServiceClient directly, so tests
// never need a real gRPC connection to the collector.
type mockLogs struct {
	mu       sync.Mutex
	received []*collectorlogspb.ExportLogsServiceRequest
	err      error
}

func (m *mockLogs) Export(_ context.Context, req *collectorlogspb.ExportLogsServiceRequest, _ ...grpc.CallOption) (*collectorlogspb.ExportLogsServiceResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, req)
	return &collectorlogspb.ExportLogsServiceResponse{}, nil
}

func (m *mockLogs) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.received)
}

func (m *mockLogs) last() *collectorlogspb.ExportLogsServiceRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.received) == 0 {
		return nil
	}
	return m.received[len(m.received)-1]
}

// ── Fake Loki / Tempo servers ────────────────────────────────────────────────

// lokiFake serves both /loki/api/v1/query_range (returning `n` result streams
// for whichever service_name or helix_process_name value is embedded in the
// query) and /loki/api/v1/label/service_name/values (returning activeNames,
// for discovery). Pass activeNames as nil for tests that don't exercise
// discovery.
func lokiFake(t *testing.T, counts map[string]int, activeNames []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/loki/api/v1/query_range":
			q := r.URL.Query().Get("query")
			n := 0
			for key, c := range counts {
				if q == `{service_name="`+key+`"}` || q == `{helix_process_name="`+key+`"}` {
					n = c
				}
			}
			result := make([]json.RawMessage, n)
			for i := range result {
				result[i] = json.RawMessage(`{}`)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data":   map[string]any{"result": result},
			})
		case "/loki/api/v1/label/service_name/values":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": activeNames})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// tempoFake serves both /api/search (returning `n` traces for the
// service_name embedded in the TraceQL query) and
// /api/v2/search/tag/resource.service.name/values (returning activeNames,
// for discovery). Pass activeNames as nil for tests that don't exercise
// discovery.
//
// Rejects requests missing X-Scope-OrgID with 401, mirroring the real
// multi-tenant Tempo — every Tempo query returned 401 in production for 12
// hours because the checker never set this header. Any test config that
// forgets TempoTenant will fail loudly here instead of silently.
func tempoFake(t *testing.T, counts map[string]int, activeNames []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Scope-OrgID") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/search":
			q := r.URL.Query().Get("q")
			n := 0
			for svc, c := range counts {
				if q == `{resource.service.name="`+svc+`"}` {
					n = c
				}
			}
			traces := make([]json.RawMessage, n)
			for i := range traces {
				traces[i] = json.RawMessage(`{}`)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"traces": traces})
		case "/api/v2/search/tag/resource.service.name/values":
			type tagValue struct {
				Value string `json:"value"`
			}
			values := make([]tagValue, len(activeNames))
			for i, n := range activeNames {
				values[i] = tagValue{Value: n}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tagValues": values})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func newTestChecker(t *testing.T, cfg Config, traces *mockTraces, logs *mockLogs) (*Checker, *metrics.Metrics) {
	t.Helper()
	m := metrics.New(prometheus.NewRegistry())
	return New(cfg, traces, logs, m), m
}

func gaugeValue(t *testing.T, g *prometheus.GaugeVec, labels ...string) (float64, bool) {
	t.Helper()
	m, err := g.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	return out.GetGauge().GetValue(), true
}

func counterValue(t *testing.T, c *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	m, err := c.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	return out.GetCounter().GetValue()
}

// ── Canary emission ──────────────────────────────────────────────────────────

func TestSendCanaryTrace(t *testing.T) {
	traces := &mockTraces{}
	c, _ := newTestChecker(t, Config{}, traces, &mockLogs{})

	if err := c.sendCanaryTrace(context.Background(), time.Now()); err != nil {
		t.Fatalf("sendCanaryTrace: %v", err)
	}
	if traces.calls() != 1 {
		t.Fatalf("expected 1 trace export, got %d", traces.calls())
	}

	req := traces.last()
	rs := req.ResourceSpans
	if len(rs) != 1 || len(rs[0].ScopeSpans) != 1 || len(rs[0].ScopeSpans[0].Spans) != 1 {
		t.Fatalf("expected exactly one span, got %+v", req)
	}
	span := rs[0].ScopeSpans[0].Spans[0]
	if len(span.TraceId) != 16 {
		t.Errorf("trace ID must be 16 bytes, got %d", len(span.TraceId))
	}
	if len(span.SpanId) != 8 {
		t.Errorf("span ID must be 8 bytes, got %d", len(span.SpanId))
	}

	foundServiceName := false
	for _, kv := range rs[0].Resource.Attributes {
		if kv.Key == "service.name" && kv.Value.GetStringValue() == CanaryServiceName {
			foundServiceName = true
		}
	}
	if !foundServiceName {
		t.Errorf("expected resource attribute service.name=%q, got %+v", CanaryServiceName, rs[0].Resource.Attributes)
	}

	foundEntityID, foundInstrumentID := false, false
	for _, kv := range span.Attributes {
		if kv.Key == "helix.entity.id" {
			foundEntityID = true
		}
		if kv.Key == "helix.instrument.id" {
			foundInstrumentID = true
		}
	}
	if !foundEntityID || !foundInstrumentID {
		t.Errorf("expected helix.entity.id and helix.instrument.id span attributes, got %+v", span.Attributes)
	}
}

func TestSendCanaryTrace_PropagatesExportError(t *testing.T) {
	traces := &mockTraces{err: errors.New("boom")}
	c, _ := newTestChecker(t, Config{}, traces, &mockLogs{})

	if err := c.sendCanaryTrace(context.Background(), time.Now()); err == nil {
		t.Fatal("expected error from failing exporter, got nil")
	}
}

func TestSendCanaryLog(t *testing.T) {
	logs := &mockLogs{}
	c, _ := newTestChecker(t, Config{}, &mockTraces{}, logs)

	if err := c.sendCanaryLog(context.Background(), time.Now()); err != nil {
		t.Fatalf("sendCanaryLog: %v", err)
	}
	if logs.calls() != 1 {
		t.Fatalf("expected 1 log export, got %d", logs.calls())
	}

	req := logs.last()
	rl := req.ResourceLogs
	if len(rl) != 1 || len(rl[0].ScopeLogs) != 1 || len(rl[0].ScopeLogs[0].LogRecords) != 1 {
		t.Fatalf("expected exactly one log record, got %+v", req)
	}
	record := rl[0].ScopeLogs[0].LogRecords[0]
	if record.Body.GetStringValue() == "" {
		t.Error("expected a non-empty log body")
	}

	foundServiceName := false
	for _, kv := range rl[0].Resource.Attributes {
		if kv.Key == "service.name" && kv.Value.GetStringValue() == CanaryServiceName {
			foundServiceName = true
		}
	}
	if !foundServiceName {
		t.Errorf("expected resource attribute service.name=%q, got %+v", CanaryServiceName, rl[0].Resource.Attributes)
	}
}

// ── Presence recording ───────────────────────────────────────────────────────

func TestRecordPresence_SetsRawCountAndDerivedPresence(t *testing.T) {
	loki := lokiFake(t, map[string]int{"svc-a": 3}, nil)
	defer loki.Close()
	tempo := tempoFake(t, map[string]int{"svc-a": 0}, nil)
	defer tempo.Close()

	cfg := Config{LokiURL: loki.URL, TempoURL: tempo.URL, LokiTenant: "anonymous", TempoTenant: "anonymous"}
	c, m := newTestChecker(t, cfg, &mockTraces{}, &mockLogs{})

	now := time.Now()
	result := c.recordPresence(context.Background(), "svc-a", "svc-a", now.Add(-time.Hour), now)

	if !result["logs"] {
		t.Error("expected logs presence true (3 results)")
	}
	if result["traces"] {
		t.Error("expected traces presence false (0 results)")
	}

	if v, _ := gaugeValue(t, m.PlatformCheckResultCount, "svc-a", "logs"); v != 3 {
		t.Errorf("PlatformCheckResultCount logs = %v, want 3", v)
	}
	if v, _ := gaugeValue(t, m.PlatformCheckResultCount, "svc-a", "traces"); v != 0 {
		t.Errorf("PlatformCheckResultCount traces = %v, want 0", v)
	}
	if v, _ := gaugeValue(t, m.PlatformCheckPresence, "svc-a", "logs"); v != 1 {
		t.Errorf("PlatformCheckPresence logs = %v, want 1", v)
	}
	if v, _ := gaugeValue(t, m.PlatformCheckPresence, "svc-a", "traces"); v != 0 {
		t.Errorf("PlatformCheckPresence traces = %v, want 0", v)
	}
}

func TestRecordPresence_QueryErrorLeavesResultUnset(t *testing.T) {
	// A Loki/Tempo server that always 500s should not panic and should
	// simply omit that signal from the returned map, rather than reporting
	// a false "absent" (which would be indistinguishable from a real gap).
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer broken.Close()

	cfg := Config{LokiURL: broken.URL, TempoURL: broken.URL}
	c, _ := newTestChecker(t, cfg, &mockTraces{}, &mockLogs{})

	now := time.Now()
	result := c.recordPresence(context.Background(), "svc-a", "svc-a", now.Add(-time.Hour), now)

	if _, ok := result["logs"]; ok {
		t.Error("expected no logs entry when the Loki query fails")
	}
	if _, ok := result["traces"]; ok {
		t.Error("expected no traces entry when the Tempo query fails")
	}
}

// ── runOnce: mismatch detection ──────────────────────────────────────────────

func TestRunOnce_FlagsMismatchOnlyWhenAsymmetric(t *testing.T) {
	loki := lokiFake(t, map[string]int{
		CanaryServiceName: 1,
		canaryProcessName: 1, // canary log queryable by helix_process_name too -> full success
		"symmetric-down":  0,
		"symmetric-up":    1,
		"asymmetric":      0, // logs missing
	}, []string{CanaryServiceName, "symmetric-up"}) // discoverable via Loki: only what has log activity
	defer loki.Close()
	tempo := tempoFake(t, map[string]int{
		CanaryServiceName: 1,
		"symmetric-down":  0,
		"symmetric-up":    1,
		"asymmetric":      1, // traces present
	}, []string{CanaryServiceName, "symmetric-up", "asymmetric"}) // discoverable via Tempo: only what has trace activity
	defer tempo.Close()

	cfg := Config{
		LokiURL:          loki.URL,
		TempoURL:         tempo.URL,
		LokiTenant:       "anonymous",
		TempoTenant:      "anonymous",
		LookbackWindow:   time.Hour,
		PropagationDelay: time.Millisecond,
	}
	c, m := newTestChecker(t, cfg, &mockTraces{}, &mockLogs{})

	c.runOnce(context.Background())

	if got := counterValue(t, m.PlatformCheckMismatchTotal, "symmetric-down", "logs"); got != 0 {
		t.Errorf("symmetric-down (both absent) should not be flagged, got %v", got)
	}
	if got := counterValue(t, m.PlatformCheckMismatchTotal, "symmetric-up", "logs"); got != 0 {
		t.Errorf("symmetric-up (both present) should not be flagged, got %v", got)
	}
	if got := counterValue(t, m.PlatformCheckMismatchTotal, "asymmetric", "logs"); got != 1 {
		t.Errorf("asymmetric (traces present, logs missing) should be flagged with missing_side=logs, got %v", got)
	}

	// Canary present on both sides -> last-success timestamp should advance.
	var out dto.Metric
	if err := m.PlatformCheckLastSuccessTimestamp.Write(&out); err != nil {
		t.Fatalf("write gauge: %v", err)
	}
	if out.GetGauge().GetValue() == 0 {
		t.Error("expected PlatformCheckLastSuccessTimestamp to be set when canary is fully present")
	}
}

func TestRunOnce_CanaryPartialPresenceDoesNotAdvanceSuccessTimestamp(t *testing.T) {
	loki := lokiFake(t, map[string]int{CanaryServiceName: 0}, nil) // logs missing
	defer loki.Close()
	tempo := tempoFake(t, map[string]int{CanaryServiceName: 1}, nil) // traces present
	defer tempo.Close()

	cfg := Config{
		LokiURL:          loki.URL,
		TempoURL:         tempo.URL,
		LokiTenant:       "anonymous",
		TempoTenant:      "anonymous",
		LookbackWindow:   time.Hour,
		PropagationDelay: time.Millisecond,
	}
	c, m := newTestChecker(t, cfg, &mockTraces{}, &mockLogs{})

	c.runOnce(context.Background())

	var out dto.Metric
	if err := m.PlatformCheckLastSuccessTimestamp.Write(&out); err != nil {
		t.Fatalf("write gauge: %v", err)
	}
	if out.GetGauge().GetValue() != 0 {
		t.Error("expected PlatformCheckLastSuccessTimestamp to stay unset when the canary's logs side is missing")
	}
}

// TestRunOnce_ReproducesLabelPromotionIncidentSignature is the regression
// test for the actual 2026-08-28 incident: the canary log lands (found by
// service_name — ingestion is fine) but Loki has stopped promoting
// helix_process_name to a queryable label, so the exact query a real
// dashboard runs finds nothing. A checker that only looked at service_name
// presence (as an earlier version of this package did) would have reported
// this as fully healthy.
func TestRunOnce_ReproducesLabelPromotionIncidentSignature(t *testing.T) {
	loki := lokiFake(t, map[string]int{
		CanaryServiceName: 1, // ingestion fine
		canaryProcessName: 0, // but not promoted to a label -> dashboard query finds nothing
	}, nil)
	defer loki.Close()
	tempo := tempoFake(t, map[string]int{CanaryServiceName: 1}, nil)
	defer tempo.Close()

	cfg := Config{
		LokiURL:          loki.URL,
		TempoURL:         tempo.URL,
		LokiTenant:       "anonymous",
		TempoTenant:      "anonymous",
		LookbackWindow:   time.Hour,
		PropagationDelay: time.Millisecond,
	}
	c, m := newTestChecker(t, cfg, &mockTraces{}, &mockLogs{})

	c.runOnce(context.Background())

	if v, _ := gaugeValue(t, m.PlatformCheckPresence, "canary", "logs"); v != 1 {
		t.Errorf("service_name-based presence should read healthy (ingestion never stopped), got %v", v)
	}
	if v, _ := gaugeValue(t, m.PlatformCheckPresence, "canary", "logs_by_label"); v != 0 {
		t.Errorf("helix_process_name-based presence should read broken (this is the incident signature), got %v", v)
	}

	var out dto.Metric
	if err := m.PlatformCheckLastSuccessTimestamp.Write(&out); err != nil {
		t.Fatalf("write gauge: %v", err)
	}
	if out.GetGauge().GetValue() != 0 {
		t.Error("expected PlatformCheckLastSuccessTimestamp to stay unset — a service_name-only check would have missed this incident entirely")
	}
}
