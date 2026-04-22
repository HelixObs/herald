// Package metrics registers and exposes the HelixObs gateway Prometheus metrics.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all counters, gauges, and histograms emitted by the gateway.
type Metrics struct {
	// ── Pipeline throughput ───────────────────────────────────────────

	// EntitiesTotal counts every entity span processed.
	EntitiesTotal *prometheus.CounterVec

	// SpansReceivedTotal counts every OTLP span arriving at the gateway.
	SpansReceivedTotal prometheus.Counter

	// SpansPassthroughTotal counts spans forwarded without helix processing.
	SpansPassthroughTotal prometheus.Counter

	// SpanProcessingDuration measures end-to-end gateway processing time per span.
	SpanProcessingDuration *prometheus.HistogramVec

	// ── Entity events ─────────────────────────────────────────────────

	// ErrorsTotal counts helix.error span events.
	ErrorsTotal *prometheus.CounterVec

	// EventsTotal counts all helix.* span events by name.
	EventsTotal *prometheus.CounterVec

	// ── Parent resolution ─────────────────────────────────────────────

	// ParentResolutionTotal counts parent lookups by outcome (success/failed).
	ParentResolutionTotal *prometheus.CounterVec

	// ParentResolutionFailedTotal counts unresolved parent IDs (kept for back-compat).
	ParentResolutionFailedTotal *prometheus.CounterVec

	// ── Trace store ───────────────────────────────────────────────────

	// TraceStoreSize is the current number of entries in the trace store.
	TraceStoreSize prometheus.Gauge

	// TraceStoreHitsTotal counts successful parent lookups in the store.
	TraceStoreHitsTotal prometheus.Counter

	// TraceStoreMissesTotal counts failed parent lookups (parent not yet seen).
	TraceStoreMissesTotal prometheus.Counter

	// TraceStoreEvictionsTotal counts entries evicted when the store is full.
	TraceStoreEvictionsTotal prometheus.Counter

	// ── TimescaleDB writes ────────────────────────────────────────────

	// DBWritesTotal counts write attempts by table and outcome.
	DBWritesTotal *prometheus.CounterVec

	// DBWriteDuration measures write latency per table.
	DBWriteDuration *prometheus.HistogramVec

	// DBWriteErrorsTotal counts best-effort write failures (kept for back-compat).
	DBWriteErrorsTotal prometheus.Counter

	// DBConnectionsInUse is the number of connections currently acquired.
	DBConnectionsInUse prometheus.Gauge

	// DBConnectionsTotal is the total pool size.
	DBConnectionsTotal prometheus.Gauge
}

// New registers all metrics with reg and returns a Metrics instance.
// Pass prometheus.DefaultRegisterer for production; use a fresh
// prometheus.NewRegistry() in tests to avoid cross-test pollution.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EntitiesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_entities_total",
			Help: "Total entity spans processed.",
		}, []string{"instrument_id", "stage", "status"}),

		SpansReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "helix_spans_received_total",
			Help: "Total OTLP spans received by the gateway.",
		}),

		SpansPassthroughTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "helix_spans_passthrough_total",
			Help: "Spans forwarded unchanged (no helix.entity.id).",
		}),

		SpanProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "helix_span_processing_duration_seconds",
			Help:    "End-to-end gateway processing time per helix span.",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5},
		}, []string{"instrument_id"}),

		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_errors_total",
			Help: "Total helix.error span events recorded.",
		}, []string{"instrument_id"}),

		EventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_events_total",
			Help: "Total helix.* span events recorded, by event name.",
		}, []string{"instrument_id", "event_name"}),

		ParentResolutionTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_parent_resolution_total",
			Help: "Parent entity ID lookups by outcome (success/failed).",
		}, []string{"instrument_id", "result"}),

		ParentResolutionFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_parent_resolution_failed_total",
			Help: "Parent entity IDs that could not be resolved to a span link.",
		}, []string{"instrument_id"}),

		TraceStoreSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "helix_trace_store_size",
			Help: "Current number of entries in the in-process trace store.",
		}),

		TraceStoreHitsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "helix_trace_store_hits_total",
			Help: "Successful parent lookups in the trace store.",
		}),

		TraceStoreMissesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "helix_trace_store_misses_total",
			Help: "Failed parent lookups — parent not yet seen by this gateway.",
		}),

		TraceStoreEvictionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "helix_trace_store_evictions_total",
			Help: "Trace store entries evicted when the store reached capacity.",
		}),

		DBWritesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_db_writes_total",
			Help: "TimescaleDB write attempts by table and outcome.",
		}, []string{"table", "status"}),

		DBWriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "helix_db_write_duration_seconds",
			Help:    "TimescaleDB write latency per table.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"table"}),

		DBWriteErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "helix_db_write_errors_total",
			Help: "Best-effort TimescaleDB write failures (entity or event rows).",
		}),

		DBConnectionsInUse: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "helix_db_connections_in_use",
			Help: "DB connection pool connections currently acquired.",
		}),

		DBConnectionsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "helix_db_connections_total",
			Help: "DB connection pool total size.",
		}),
	}

	reg.MustRegister(
		m.EntitiesTotal,
		m.SpansReceivedTotal,
		m.SpansPassthroughTotal,
		m.SpanProcessingDuration,
		m.ErrorsTotal,
		m.EventsTotal,
		m.ParentResolutionTotal,
		m.ParentResolutionFailedTotal,
		m.TraceStoreSize,
		m.TraceStoreHitsTotal,
		m.TraceStoreMissesTotal,
		m.TraceStoreEvictionsTotal,
		m.DBWritesTotal,
		m.DBWriteDuration,
		m.DBWriteErrorsTotal,
		m.DBConnectionsInUse,
		m.DBConnectionsTotal,
	)
	return m
}

// ── Interface adapters ────────────────────────────────────────────────────────
// These satisfy the storeMetrics and dbMetrics interfaces in their respective
// packages without creating import cycles.

func (m *Metrics) TraceStoreHit()            { m.TraceStoreHitsTotal.Inc() }
func (m *Metrics) TraceStoreMiss()           { m.TraceStoreMissesTotal.Inc() }
func (m *Metrics) TraceStoreEviction()       { m.TraceStoreEvictionsTotal.Inc() }
func (m *Metrics) TraceStoreSetSize(n int)   { m.TraceStoreSize.Set(float64(n)) }

func (m *Metrics) DBWriteRecord(table, status string, dur time.Duration) {
	m.DBWritesTotal.WithLabelValues(table, status).Inc()
	m.DBWriteDuration.WithLabelValues(table).Observe(dur.Seconds())
	if status == "failed" {
		m.DBWriteErrorsTotal.Inc()
	}
}

func (m *Metrics) DBPoolStats(inUse, total int) {
	m.DBConnectionsInUse.Set(float64(inUse))
	m.DBConnectionsTotal.Set(float64(total))
}
