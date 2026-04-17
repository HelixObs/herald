// Package metrics registers and exposes the HelixObs gateway Prometheus metrics.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds all counters and histograms emitted by the gateway.
type Metrics struct {
	// EntitiesTotal counts every entity span processed, labelled by
	// instrument, pipeline stage, and terminal status (ok / error).
	EntitiesTotal *prometheus.CounterVec

	// ErrorsTotal counts helix.error span events — one per affected entity.
	ErrorsTotal *prometheus.CounterVec

	// EventsTotal counts all helix.* span events by name.
	EventsTotal *prometheus.CounterVec

	// ParentResolutionFailedTotal counts parent entity IDs in
	// helix.parent.ids that could not be resolved to a span link
	// because the parent had not yet been seen by this gateway instance.
	ParentResolutionFailedTotal *prometheus.CounterVec

	// DBWriteErrorsTotal counts best-effort TimescaleDB write failures.
	DBWriteErrorsTotal prometheus.Counter
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

		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_errors_total",
			Help: "Total helix.error span events recorded.",
		}, []string{"instrument_id"}),

		EventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_events_total",
			Help: "Total helix.* span events recorded, by event name.",
		}, []string{"instrument_id", "event_name"}),

		ParentResolutionFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "helix_parent_resolution_failed_total",
			Help: "Parent entity IDs that could not be resolved to a span link.",
		}, []string{"instrument_id"}),

		DBWriteErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "helix_db_write_errors_total",
			Help: "Best-effort TimescaleDB write failures (entity or event rows).",
		}),
	}

	reg.MustRegister(
		m.EntitiesTotal,
		m.ErrorsTotal,
		m.EventsTotal,
		m.ParentResolutionFailedTotal,
		m.DBWriteErrorsTotal,
	)
	return m
}
