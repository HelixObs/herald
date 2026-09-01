// Package platformcheck periodically validates that Herald's own OTLP paths —
// traces (through Herald's interceptor) and logs (direct to the otel-collector,
// the same way real clients send them) — are actually landing where dashboards
// and alerts expect to find them.
//
// It exists because a Loki label-promotion regression can leave logs flowing
// correctly end-to-end while making them invisible to every query that
// depends on those labels — silently, with no crash and no error-rate spike.
// A synthetic canary with no legitimate reason to ever go quiet closes that
// blind spot; a real service being offline for maintenance does not trip it,
// because maintenance silences traces and logs equally, while a pipeline
// regression like the one this was built for silences only one side.
//
// This package only records Prometheus metrics (see internal/metrics). It
// does not dispatch notifications — that's a deliberate, separate step once
// the metrics here have been observed for long enough to calibrate real
// alert thresholds.
package platformcheck

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/HelixObs/herald/internal/metrics"
)

const (
	canaryInstrumentID = "HELIXOBS-PLATFORM"
	canaryEntityID     = "platform-canary"
	// CanaryServiceName is the OTel service.name the canary emits under —
	// exported so a Grafana panel or ad hoc query can target it directly.
	CanaryServiceName = "helixobs.platform-canary"
	// canaryProcessName is set as the canary log's helix_process_name
	// log-record attribute — the same field real clients set and the same
	// field every "Pipeline Process Logs"-style dashboard filters on. It only
	// becomes a queryable Loki label if Loki's otlp_config actually promotes
	// it; that promotion is precisely the thing that broke silently in the
	// incident this package exists to catch.
	canaryProcessName = "helixobs/platform-canary/heartbeat"

	signalTraces = "traces"
	// signalLogs is "did the canary log land at all", checked via the stable
	// service_name label — proves ingestion, nothing about label promotion.
	signalLogs = "logs"
	// signalLogsByLabel is "is the canary log queryable the way a real
	// dashboard queries it", via {helix_process_name="..."} instead of
	// service_name. This is the signal that actually would have caught the
	// 2026-08-28 incident: service_name-based presence stayed green the
	// entire time, because ingestion never stopped — only this label-based
	// query silently went dark. Only meaningful for the canary source.
	signalLogsByLabel = "logs_by_label"
)

// TraceExporter is the subset of receiver.Receiver used to push the canary
// trace through Herald's real interceptor + forward path, exactly like a
// real client's span, rather than a bespoke shortcut that could drift from
// what actually happens in production.
type TraceExporter interface {
	Export(ctx context.Context, req *collectortracepb.ExportTraceServiceRequest) (*collectortracepb.ExportTraceServiceResponse, error)
}

// Config controls check cadence and what to query.
type Config struct {
	// Interval between check ticks.
	Interval time.Duration
	// LookbackWindow is how far back each tick looks when checking whether a
	// signal is present. Should comfortably exceed Interval so a single slow
	// tick doesn't read as an outage.
	LookbackWindow time.Duration

	// PropagationDelay is how long to wait after emitting the canary before
	// checking whether it landed. Production should set this generously
	// relative to normal OTLP pipeline latency (seconds); tests set it near
	// zero so runOnce doesn't block on a real sleep.
	PropagationDelay time.Duration

	LokiURL    string // e.g. http://loki-gateway.loki.svc.cluster.local
	LokiTenant string // X-Scope-OrgID, e.g. "anonymous"
	TempoURL   string // e.g. http://tempo-gateway.tempo.svc.cluster.local

	// CollectorEndpoint is the otel-collector's OTLP gRPC address, used to
	// send the canary log directly — the same path real clients' LOGS_ENDPOINT
	// points at, bypassing Herald (which only ever sees traces).
	CollectorEndpoint string
}

// Checker runs the periodic platform health check and records its findings
// as Prometheus metrics (internal/metrics.Metrics.PlatformCheck*).
type Checker struct {
	cfg     Config
	traces  TraceExporter
	logs    collectorlogspb.LogsServiceClient
	metrics *metrics.Metrics
	http    *http.Client
	closer  func() error // closes the underlying connection, if New dialed one; nil otherwise
}

// New returns a Checker using the given traces and logs clients directly.
// Use this in tests to inject fakes/mocks. Production code should use Dial,
// which also owns and cleans up the gRPC connection to the collector.
func New(cfg Config, traces TraceExporter, logs collectorlogspb.LogsServiceClient, m *metrics.Metrics) *Checker {
	return &Checker{
		cfg:     cfg,
		traces:  traces,
		logs:    logs,
		metrics: m,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Dial dials the otel-collector's OTLP endpoint for canary log emission and
// returns a Checker. traces is reused from Herald's own already-dialed
// Receiver so the canary trace takes the exact code path a real client's
// span would (interceptor enrichment, DB write, forward to Tempo).
func Dial(cfg Config, traces TraceExporter, m *metrics.Metrics) (*Checker, error) {
	conn, err := grpc.NewClient(cfg.CollectorEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial collector for canary logs %s: %w", cfg.CollectorEndpoint, err)
	}
	c := New(cfg, traces, collectorlogspb.NewLogsServiceClient(conn), m)
	c.closer = conn.Close
	return c, nil
}

// Close releases the collector connection dialed by Dial, if any. Safe to
// call once, after Start's context is cancelled. No-op for a Checker built
// directly via New.
func (c *Checker) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer()
}

// Start runs the check loop until ctx is cancelled.
func (c *Checker) Start(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.runOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Checker) runOnce(ctx context.Context) {
	sentAt := time.Now()
	if err := c.sendCanaryTrace(ctx, sentAt); err != nil {
		slog.Warn("platformcheck: failed to send canary trace", "error", err)
	}
	if err := c.sendCanaryLog(ctx, sentAt); err != nil {
		slog.Warn("platformcheck: failed to send canary log", "error", err)
	}

	select {
	case <-time.After(c.cfg.PropagationDelay):
	case <-ctx.Done():
		return
	}

	start := sentAt.Add(-c.cfg.LookbackWindow)
	end := time.Now()

	canaryOK := c.recordPresence(ctx, "canary", CanaryServiceName, start, end)

	labelCount, err := c.lokiLabelResultCount(ctx, canaryProcessName, start, end)
	if err != nil {
		slog.Warn("platformcheck: canary label query failed", "error", err)
	} else {
		c.metrics.PlatformCheckResultCount.WithLabelValues("canary", signalLogsByLabel).Set(float64(labelCount))
		c.metrics.PlatformCheckPresence.WithLabelValues("canary", signalLogsByLabel).Set(boolToFloat(labelCount > 0))
		canaryOK[signalLogsByLabel] = labelCount > 0
		if canaryOK[signalLogs] && !canaryOK[signalLogsByLabel] {
			// The exact signature of the 2026-08-28 incident: the log landed
			// (service_name-based query finds it) but isn't queryable the way
			// a real dashboard queries it (helix_process_name-based query
			// doesn't) — a label-promotion regression, not an ingestion outage.
			slog.Warn("platformcheck: canary log present by service_name but not by helix_process_name — label promotion may be broken")
		}
	}

	if canaryOK[signalTraces] && canaryOK[signalLogs] && canaryOK[signalLogsByLabel] {
		c.metrics.PlatformCheckLastSuccessTimestamp.Set(float64(time.Now().Unix()))
	}

	for _, svc := range c.discoverServiceNames(ctx, start, end) {
		present := c.recordPresence(ctx, svc, svc, start, end)
		tracesPresent, logsPresent := present[signalTraces], present[signalLogs]
		// Both present or both absent is the normal case — including a real
		// instrument legitimately down for maintenance, which silences both
		// equally. Only the asymmetric case points at a pipeline-specific
		// bug (like the Loki label-promotion regression this was built for)
		// rather than the instrument itself being offline.
		if tracesPresent != logsPresent {
			missing := signalLogs
			if !tracesPresent {
				missing = signalTraces
			}
			c.metrics.PlatformCheckMismatchTotal.WithLabelValues(svc, missing).Inc()
			slog.Warn("platformcheck: trace/log presence mismatch",
				"service_name", svc, "traces_present", tracesPresent, "logs_present", logsPresent)
		}
	}
}

// discoverServiceNames returns the union of service_name values seen recently
// by Loki and Tempo, excluding the canary's own identity (checked
// separately, unconditionally, above).
//
// Deliberately a union, not an intersection: a service whose logs silently
// stopped being labeled correctly (the actual incident this package was
// built for) would vanish from a Loki-sourced list and never get checked
// again at exactly the moment checking it matters most. Discovering from
// Tempo too means the still-healthy side keeps the service visible.
func (c *Checker) discoverServiceNames(ctx context.Context, start, end time.Time) []string {
	seen := make(map[string]struct{})

	lokiNames, err := c.lokiActiveServiceNames(ctx, start, end)
	if err != nil {
		slog.Warn("platformcheck: loki service discovery failed", "error", err)
	}
	for _, n := range lokiNames {
		seen[n] = struct{}{}
	}

	tempoNames, err := c.tempoActiveServiceNames(ctx, start, end)
	if err != nil {
		slog.Warn("platformcheck: tempo service discovery failed", "error", err)
	}
	for _, n := range tempoNames {
		seen[n] = struct{}{}
	}

	delete(seen, CanaryServiceName)

	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	return out
}

// recordPresence queries Tempo and Loki for serviceName's activity in
// [start, end]. It records both the raw result count (PlatformCheckResultCount)
// and the count-derived presence boolean (PlatformCheckPresence) for both
// signals under source, and returns the derived booleans. The raw count is
// recorded even when it's zero, and independently of the presence cutoff, so
// the "count > 0" threshold below isn't the only thing the dashboard can see —
// it can be recomputed at a different cutoff directly from the raw metric.
func (c *Checker) recordPresence(ctx context.Context, source, serviceName string, start, end time.Time) map[string]bool {
	result := make(map[string]bool, 2)

	tracesCount, err := c.tempoTraceCount(ctx, serviceName, start, end)
	if err != nil {
		slog.Warn("platformcheck: tempo query failed", "service_name", serviceName, "error", err)
	} else {
		c.metrics.PlatformCheckResultCount.WithLabelValues(source, signalTraces).Set(float64(tracesCount))
		c.metrics.PlatformCheckPresence.WithLabelValues(source, signalTraces).Set(boolToFloat(tracesCount > 0))
		result[signalTraces] = tracesCount > 0
	}

	logsCount, err := c.lokiResultCount(ctx, serviceName, start, end)
	if err != nil {
		slog.Warn("platformcheck: loki query failed", "service_name", serviceName, "error", err)
	} else {
		c.metrics.PlatformCheckResultCount.WithLabelValues(source, signalLogs).Set(float64(logsCount))
		c.metrics.PlatformCheckPresence.WithLabelValues(source, signalLogs).Set(boolToFloat(logsCount > 0))
		result[signalLogs] = logsCount > 0
	}

	return result
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// ── canary emission ─────────────────────────────────────────────────────────

func (c *Checker) sendCanaryTrace(ctx context.Context, at time.Time) error {
	startNs := uint64(at.UnixNano())
	req := &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				strAttr("service.name", CanaryServiceName),
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					TraceId:           randBytes(16),
					SpanId:            randBytes(8),
					Name:              "platform-canary-heartbeat",
					StartTimeUnixNano: startNs,
					EndTimeUnixNano:   startNs + uint64(time.Millisecond),
					Attributes: []*commonpb.KeyValue{
						strAttr("helix.entity.id", canaryEntityID),
						strAttr("helix.instrument.id", canaryInstrumentID),
					},
				}},
			}},
		}},
	}
	_, err := c.traces.Export(ctx, req)
	return err
}

func (c *Checker) sendCanaryLog(ctx context.Context, at time.Time) error {
	req := &collectorlogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				strAttr("service.name", CanaryServiceName),
			}},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano:   uint64(at.UnixNano()),
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
					SeverityText:   "INFO",
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "platform canary heartbeat"},
					},
					// Mirrors the log-record attributes real clients set
					// (see helixobs/client-python) so this log is queryable
					// via the exact LogQL shape real dashboards use, not just
					// by service_name.
					Attributes: []*commonpb.KeyValue{
						strAttr("helix_process_name", canaryProcessName),
						strAttr("helix_instrument_id", canaryInstrumentID),
						strAttr("level", "info"),
					},
				}},
			}},
		}},
	}
	_, err := c.logs.Export(ctx, req)
	return err
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// ── Loki / Tempo presence queries ───────────────────────────────────────────

// resultLimit caps how many results each query asks for. High enough that
// the raw count metric reflects real magnitude rather than saturating at a
// tiny cap, low enough to keep each check cheap.
const resultLimit = "100"

// lokiResultCount returns the number of Loki result streams matching
// serviceName in [start, end] (capped at resultLimit).
func (c *Checker) lokiResultCount(ctx context.Context, serviceName string, start, end time.Time) (int, error) {
	return c.lokiQueryCount(ctx, fmt.Sprintf(`{service_name=%q}`, serviceName), start, end)
}

// lokiLabelResultCount is like lokiResultCount but matches on
// helix_process_name instead of service_name — the same label real
// dashboards filter on. It only returns non-zero if that label is both
// present on the log line AND actually promoted to a queryable Loki index
// label; a promotion regression makes this go to zero while lokiResultCount
// (keyed on the unrelated, stable service_name label) stays non-zero.
func (c *Checker) lokiLabelResultCount(ctx context.Context, processName string, start, end time.Time) (int, error) {
	return c.lokiQueryCount(ctx, fmt.Sprintf(`{helix_process_name=%q}`, processName), start, end)
}

func (c *Checker) lokiQueryCount(ctx context.Context, query string, start, end time.Time) (int, error) {
	q := url.Values{}
	q.Set("query", query)
	q.Set("start", fmt.Sprintf("%d", start.UnixNano()))
	q.Set("end", fmt.Sprintf("%d", end.UnixNano()))
	q.Set("limit", resultLimit)

	reqURL := strings.TrimRight(c.cfg.LokiURL, "/") + "/loki/api/v1/query_range?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, err
	}
	if c.cfg.LokiTenant != "" {
		req.Header.Set("X-Scope-OrgID", c.cfg.LokiTenant)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("loki query_range: unexpected status %d", resp.StatusCode)
	}

	var out struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return len(out.Data.Result), nil
}

// tempoTraceCount returns the number of Tempo traces matching serviceName in
// [start, end] (capped at resultLimit).
func (c *Checker) tempoTraceCount(ctx context.Context, serviceName string, start, end time.Time) (int, error) {
	q := url.Values{}
	q.Set("q", fmt.Sprintf(`{resource.service.name=%q}`, serviceName))
	q.Set("start", fmt.Sprintf("%d", start.Unix()))
	q.Set("end", fmt.Sprintf("%d", end.Unix()))
	q.Set("limit", resultLimit)

	reqURL := strings.TrimRight(c.cfg.TempoURL, "/") + "/api/search?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("tempo search: unexpected status %d", resp.StatusCode)
	}

	var out struct {
		Traces []json.RawMessage `json:"traces"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return len(out.Traces), nil
}

// lokiActiveServiceNames returns the distinct service_name values Loki has
// seen in [start, end].
func (c *Checker) lokiActiveServiceNames(ctx context.Context, start, end time.Time) ([]string, error) {
	q := url.Values{}
	q.Set("start", fmt.Sprintf("%d", start.UnixNano()))
	q.Set("end", fmt.Sprintf("%d", end.UnixNano()))

	reqURL := strings.TrimRight(c.cfg.LokiURL, "/") + "/loki/api/v1/label/service_name/values?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	if c.cfg.LokiTenant != "" {
		req.Header.Set("X-Scope-OrgID", c.cfg.LokiTenant)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("loki label values: unexpected status %d", resp.StatusCode)
	}

	var out struct {
		Data []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// tempoActiveServiceNames returns the distinct resource.service.name values
// Tempo has seen in [start, end].
func (c *Checker) tempoActiveServiceNames(ctx context.Context, start, end time.Time) ([]string, error) {
	q := url.Values{}
	q.Set("start", fmt.Sprintf("%d", start.Unix()))
	q.Set("end", fmt.Sprintf("%d", end.Unix()))

	reqURL := strings.TrimRight(c.cfg.TempoURL, "/") + "/api/v2/search/tag/resource.service.name/values?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tempo tag values: unexpected status %d", resp.StatusCode)
	}

	var out struct {
		TagValues []struct {
			Value string `json:"value"`
		} `json:"tagValues"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(out.TagValues))
	for _, v := range out.TagValues {
		names = append(names, v.Value)
	}
	return names, nil
}
