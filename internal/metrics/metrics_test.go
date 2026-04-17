package metrics_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HelixObs/gateway/internal/metrics"
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
