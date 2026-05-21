package metrics_test

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HelixObs/herald/internal/metrics"
)

func TestNewRegistersAllMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	if m == nil {
		t.Fatal("New returned nil")
	}

	// Unregister returns true iff the collector was previously registered.
	// This is the correct way to verify registration for CounterVec metrics,
	// which only appear in Gather() after at least one label set is observed.
	cases := []struct {
		name string
		c    prometheus.Collector
	}{
		{"EntitiesTotal", m.EntitiesTotal},
		{"ErrorsTotal", m.ErrorsTotal},
		{"EventsTotal", m.EventsTotal},
		{"ParentResolutionFailedTotal", m.ParentResolutionFailedTotal},
		{"DBWriteErrorsTotal", m.DBWriteErrorsTotal},
	}
	for _, tc := range cases {
		if !reg.Unregister(tc.c) {
			t.Errorf("%s was not registered", tc.name)
		}
	}
}

func TestDoubleRegistrationPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.New(reg)
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on double registration, got none")
		}
	}()
	metrics.New(reg) // must panic
}

func TestEntitiesTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.EntitiesTotal.WithLabelValues("CHIME", "correlator", "ok").Inc()
	m.EntitiesTotal.WithLabelValues("CHIME", "correlator", "ok").Inc()
	m.EntitiesTotal.WithLabelValues("CHIME", "classifier", "error").Inc()

	expected := `
		# HELP helix_entities_total Total entity spans processed.
		# TYPE helix_entities_total counter
		helix_entities_total{instrument_id="CHIME",stage="classifier",status="error"} 1
		helix_entities_total{instrument_id="CHIME",stage="correlator",status="ok"} 2
	`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), "helix_entities_total"); err != nil {
		t.Error(err)
	}
}

func TestErrorsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.ErrorsTotal.WithLabelValues("CHIME").Inc()
	m.ErrorsTotal.WithLabelValues("CHIME").Inc()
	m.ErrorsTotal.WithLabelValues("HIRAX").Inc()

	expected := `
		# HELP helix_errors_total Total helix.error span events recorded.
		# TYPE helix_errors_total counter
		helix_errors_total{instrument_id="CHIME"} 2
		helix_errors_total{instrument_id="HIRAX"} 1
	`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), "helix_errors_total"); err != nil {
		t.Error(err)
	}
}

func TestEventsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.EventsTotal.WithLabelValues("CHIME", "helix.event.rfi_flagged").Inc()
	m.EventsTotal.WithLabelValues("CHIME", "helix.event.candidate_promoted").Add(3)
	m.EventsTotal.WithLabelValues("CHIME", "helix.error").Inc()

	val := testutil.ToFloat64(m.EventsTotal.WithLabelValues("CHIME", "helix.event.candidate_promoted"))
	if val != 3 {
		t.Errorf("expected helix.event.candidate_promoted=3, got %v", val)
	}
}

func TestParentResolutionFailedTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.ParentResolutionFailedTotal.WithLabelValues("CHIME").Add(5)

	val := testutil.ToFloat64(m.ParentResolutionFailedTotal.WithLabelValues("CHIME"))
	if val != 5 {
		t.Errorf("expected 5, got %v", val)
	}
}

func TestDBWriteErrorsTotal(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.DBWriteErrorsTotal.Inc()
	m.DBWriteErrorsTotal.Inc()

	val := testutil.ToFloat64(m.DBWriteErrorsTotal)
	if val != 2 {
		t.Errorf("expected 2, got %v", val)
	}
}

func TestIndependentRegistriesDoNotShareState(t *testing.T) {
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()
	m1 := metrics.New(reg1)
	m2 := metrics.New(reg2)

	m1.EntitiesTotal.WithLabelValues("CHIME", "stage", "ok").Add(10)
	m2.EntitiesTotal.WithLabelValues("CHIME", "stage", "ok").Add(1)

	if testutil.ToFloat64(m1.EntitiesTotal.WithLabelValues("CHIME", "stage", "ok")) ==
		testutil.ToFloat64(m2.EntitiesTotal.WithLabelValues("CHIME", "stage", "ok")) {
		t.Error("registries should be independent")
	}
}

// ── Interface adapter tests ───────────────────────────────────────────────────

func TestTraceStoreAdapters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.TraceStoreHit()
	m.TraceStoreHit()
	m.TraceStoreMiss()
	m.TraceStoreEviction()
	m.TraceStoreSetSize(10)
	m.RecordTraceStoreLookup(time.Millisecond)

	if v := testutil.ToFloat64(m.TraceStoreHitsTotal); v != 2 {
		t.Errorf("expected TraceStoreHitsTotal=2, got %v", v)
	}
	if v := testutil.ToFloat64(m.TraceStoreMissesTotal); v != 1 {
		t.Errorf("expected TraceStoreMissesTotal=1, got %v", v)
	}
	if v := testutil.ToFloat64(m.TraceStoreEvictionsTotal); v != 1 {
		t.Errorf("expected TraceStoreEvictionsTotal=1, got %v", v)
	}
	if v := testutil.ToFloat64(m.TraceStoreSize); v != 10 {
		t.Errorf("expected TraceStoreSize=10, got %v", v)
	}
}

func TestDBAdapters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.DBWriteRecord("entities", "success", time.Millisecond)
	m.DBWriteRecord("entities", "failed", time.Millisecond)
	m.DBPoolStats(5, 40)
	m.DBQueryRecord("entity_graph", "success", time.Millisecond)

	if v := testutil.ToFloat64(m.DBWritesTotal.WithLabelValues("entities", "success")); v != 1 {
		t.Errorf("expected DBWritesTotal entities:success=1, got %v", v)
	}
	if v := testutil.ToFloat64(m.DBWritesTotal.WithLabelValues("entities", "failed")); v != 1 {
		t.Errorf("expected DBWritesTotal entities:failed=1, got %v", v)
	}
	// DBWriteRecord with "failed" also increments DBWriteErrorsTotal.
	if v := testutil.ToFloat64(m.DBWriteErrorsTotal); v != 1 {
		t.Errorf("expected DBWriteErrorsTotal=1 after failed write, got %v", v)
	}
	if v := testutil.ToFloat64(m.DBConnectionsInUse); v != 5 {
		t.Errorf("expected DBConnectionsInUse=5, got %v", v)
	}
	if v := testutil.ToFloat64(m.DBConnectionsTotal); v != 40 {
		t.Errorf("expected DBConnectionsTotal=40, got %v", v)
	}
	if v := testutil.ToFloat64(m.DBQueriesTotal.WithLabelValues("entity_graph", "success")); v != 1 {
		t.Errorf("expected DBQueriesTotal entity_graph:success=1, got %v", v)
	}
}

func TestAPIRequestRecord(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.APIRequestRecord("entity_graph", "200", time.Millisecond)
	m.APIRequestRecord("entity_graph", "200", 2*time.Millisecond)
	m.APIRequestRecord("entity_graph", "404", time.Millisecond)

	if v := testutil.ToFloat64(m.APIRequestsTotal.WithLabelValues("entity_graph", "200")); v != 2 {
		t.Errorf("expected APIRequestsTotal entity_graph:200=2, got %v", v)
	}
	if v := testutil.ToFloat64(m.APIRequestsTotal.WithLabelValues("entity_graph", "404")); v != 1 {
		t.Errorf("expected APIRequestsTotal entity_graph:404=1, got %v", v)
	}
}

func TestDBAdaptersPoolStats(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	m.DBPoolStats(3, 20)

	expected := `
		# HELP helix_db_connections_in_use DB connection pool connections currently acquired.
		# TYPE helix_db_connections_in_use gauge
		helix_db_connections_in_use 3
		# HELP helix_db_connections_total DB connection pool total size.
		# TYPE helix_db_connections_total gauge
		helix_db_connections_total 20
	`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"helix_db_connections_in_use", "helix_db_connections_total"); err != nil {
		t.Error(err)
	}
}
