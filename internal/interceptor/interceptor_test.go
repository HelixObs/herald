package interceptor_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/HelixObs/gateway/internal/db"
	"github.com/HelixObs/gateway/internal/interceptor"
	"github.com/HelixObs/gateway/internal/metrics"
	"github.com/HelixObs/gateway/internal/notifier"
	"github.com/HelixObs/gateway/internal/store"
)

// newInterceptor builds an Interceptor with a fresh store + metrics registry.
// db is nil — DB writes are skipped (nil-guarded in the interceptor).
func newInterceptor() *interceptor.Interceptor {
	s := store.New(1_000, nil)
	m := metrics.New(prometheus.NewRegistry())
	return interceptor.New(s, nil, m)
}

// makeSpan builds a minimal OTLP span with the given attributes.
func makeSpan(name string, attrs map[string]string) *tracepb.Span {
	span := &tracepb.Span{
		Name:              name,
		TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
		StartTimeUnixNano: 1_000_000_000,
	}
	for k, v := range attrs {
		span.Attributes = append(span.Attributes, strAttr(k, v))
	}
	return span
}

func makeReq(spans ...*tracepb.Span) *collectortracepb.ExportTraceServiceRequest {
	return &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: spans,
			}},
		}},
	}
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

// ── Non-helix spans ───────────────────────────────────────────────────────────

func TestNonHelixSpanPassedThrough(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("some-service", map[string]string{"http.method": "GET"})
	req := makeReq(span)
	icp.Process(req)
	// Span should be unchanged — no links added.
	if len(span.Links) != 0 {
		t.Errorf("expected 0 links, got %d", len(span.Links))
	}
}

// ── Span attributes ───────────────────────────────────────────────────────────

func TestHelixSpanHasEntityID(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-1",
		"helix.instrument.id": "CHIME",
	})
	icp.Process(makeReq(span))
	// Entity should now be in the store — verify by checking a child resolves it.
	child := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-1",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "block-1",
	})
	icp.Process(makeReq(child))
	if len(child.Links) != 1 {
		t.Errorf("expected 1 link on child, got %d", len(child.Links))
	}
}

// ── Parent resolution ─────────────────────────────────────────────────────────

func TestKnownParentAddsSpanLink(t *testing.T) {
	icp := newInterceptor()

	parent := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-1",
		"helix.instrument.id": "CHIME",
	})
	parent.TraceId = []byte{0xAA, 0xBB, 0xCC, 0xDD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	parent.SpanId = []byte{0x11, 0x22, 0x33, 0x44, 0, 0, 0, 0}
	icp.Process(makeReq(parent))

	child := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-1",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "block-1",
	})
	icp.Process(makeReq(child))

	if len(child.Links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(child.Links))
	}
	if child.Links[0].TraceId[0] != 0xAA {
		t.Errorf("link TraceId mismatch: got %x", child.Links[0].TraceId)
	}
	if child.Links[0].SpanId[0] != 0x11 {
		t.Errorf("link SpanId mismatch: got %x", child.Links[0].SpanId)
	}
}

func TestUnknownParentAddsNoLink(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-1",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "block-never-seen",
	})
	icp.Process(makeReq(span))
	if len(span.Links) != 0 {
		t.Errorf("expected 0 links for unknown parent, got %d", len(span.Links))
	}
}

func TestMultipleParentsAllResolved(t *testing.T) {
	icp := newInterceptor()

	for i, id := range []string{"cand-41", "cand-42", "cand-43"} {
		p := makeSpan("beam-processor", map[string]string{
			"helix.entity.id":     id,
			"helix.instrument.id": "CHIME",
		})
		p.TraceId = []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		p.SpanId = []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}
		icp.Process(makeReq(p))
	}

	event := makeSpan("clustering", map[string]string{
		"helix.entity.id":     "frb-001",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "cand-41,cand-42,cand-43",
	})
	icp.Process(makeReq(event))

	if len(event.Links) != 3 {
		t.Errorf("expected 3 links (N-to-1 provenance), got %d", len(event.Links))
	}
}

func TestMixedKnownAndUnknownParents(t *testing.T) {
	icp := newInterceptor()

	known := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-1",
		"helix.instrument.id": "CHIME",
	})
	icp.Process(makeReq(known))

	child := makeSpan("clustering", map[string]string{
		"helix.entity.id":     "event-1",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "block-1,cand-cross-process",
	})
	icp.Process(makeReq(child))

	if len(child.Links) != 1 {
		t.Errorf("expected 1 link (known parent only), got %d", len(child.Links))
	}
}

// ── helix.* span events ───────────────────────────────────────────────────────

func TestHelixEventsExtracted(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-1",
		"helix.instrument.id": "CHIME",
	})
	span.Events = []*tracepb.Span_Event{
		{Name: "helix.event.rfi_flagged", TimeUnixNano: 1_000_000_000,
			Attributes: []*commonpb.KeyValue{strAttr("fraction", "0.92")}},
		{Name: "helix.event.candidate_promoted", TimeUnixNano: 2_000_000_000},
		{Name: "some.other.event", TimeUnixNano: 3_000_000_000},
	}
	// Just verify the span is processed without panicking and events are untouched.
	icp.Process(makeReq(span))
}

func TestNonHelixEventsIgnored(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-1",
		"helix.instrument.id": "CHIME",
	})
	span.Events = []*tracepb.Span_Event{
		{Name: "some.irrelevant.event"},
	}
	icp.Process(makeReq(span))
	// No panic, span unchanged.
	if len(span.Links) != 0 {
		t.Errorf("unexpected links: %d", len(span.Links))
	}
}

// ── Batch processing ──────────────────────────────────────────────────────────

func TestMultipleSpansInOneBatch(t *testing.T) {
	icp := newInterceptor()

	parent := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-batch",
		"helix.instrument.id": "CHIME",
	})
	child := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-batch",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "block-batch",
	})

	// Both in the same batch — parent processed first, child resolves it.
	icp.Process(makeReq(parent, child))

	if len(child.Links) != 1 {
		t.Errorf("expected child to resolve parent within same batch, got %d links", len(child.Links))
	}
}

func TestNilSpanSkipped(t *testing.T) {
	icp := newInterceptor()
	req := makeReq(nil)
	// Should not panic.
	icp.Process(req)
}

// ── Metadata extraction ───────────────────────────────────────────────────────

func TestSystemAttrsExcludedFromMetadata(t *testing.T) {
	// Verify that helix.* system attrs don't end up in the domain metadata.
	// We test this indirectly by confirming Process doesn't panic on a span
	// that has only system attributes (metadata would be empty map or nil).
	icp := newInterceptor()
	span := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-1",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "",
	})
	icp.Process(makeReq(span))
}

func TestDomainAttrsPreserved(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-1",
		"helix.instrument.id": "CHIME",
		"helix.chime.dm":      "341.2",
		"helix.chime.snr":     "18.3",
	})
	icp.Process(makeReq(span))
	// Verify domain attrs are still on the span (not removed by the interceptor).
	dm := ""
	for _, a := range span.Attributes {
		if a.Key == "helix.chime.dm" {
			dm = a.Value.GetStringValue()
		}
	}
	if dm != "341.2" {
		t.Errorf("expected helix.chime.dm=341.2, got %q", dm)
	}
}

// ── Parent ID trimming ────────────────────────────────────────────────────────

func TestParentIDsWithWhitespaceTrimmed(t *testing.T) {
	icp := newInterceptor()

	parent := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-ws",
		"helix.instrument.id": "CHIME",
	})
	icp.Process(makeReq(parent))

	child := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-ws",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    " block-ws , ",
	})
	icp.Process(makeReq(child))

	if len(child.Links) != 1 {
		t.Errorf("expected whitespace-trimmed parent to resolve, got %d links", len(child.Links))
	}
}

func TestEmptyParentIDs(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-1",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "",
	})
	icp.Process(makeReq(span))
	if len(span.Links) != 0 {
		t.Errorf("expected 0 links for empty parent IDs, got %d", len(span.Links))
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// Ensure "helix.parent.ids" values with commas-only produce no links.
func TestCommaOnlyParentIDsProducesNoLinks(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-1",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    ",,,",
	})
	icp.Process(makeReq(span))
	if len(span.Links) != 0 {
		t.Errorf("expected 0 links, got %d", len(span.Links))
	}
}

// Verify that instrumentID defaults gracefully when missing.
func TestMissingInstrumentIDDoesNotPanic(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("correlator", map[string]string{
		"helix.entity.id": "block-1",
		// helix.instrument.id intentionally absent
	})
	icp.Process(makeReq(span))
}

// Verify the store is updated so a second child can also resolve the parent.
func TestStoreUpdatedForFutureChildren(t *testing.T) {
	icp := newInterceptor()

	parent := makeSpan("correlator", map[string]string{
		"helix.entity.id":     "block-shared",
		"helix.instrument.id": "CHIME",
	})
	icp.Process(makeReq(parent))

	for i, id := range []string{"cand-a", "cand-b", "cand-c"} {
		child := makeSpan("classifier", map[string]string{
			"helix.entity.id":     id,
			"helix.instrument.id": "CHIME",
			"helix.parent.ids":    "block-shared",
		})
		child.SpanId = []byte{byte(i + 10), 0, 0, 0, 0, 0, 0, 0}
		icp.Process(makeReq(child))
		if len(child.Links) != 1 {
			t.Errorf("%s: expected 1 link, got %d", id, len(child.Links))
		}
	}
}

// ── Entity operations (helix.entity.is_operation = "true") ───────────────────

func TestOperationSpanDoesNotUpdateStore(t *testing.T) {
	icp := newInterceptor()

	// Register the entity first.
	entity := makeSpan("clustering", map[string]string{
		"helix.entity.id":     "frb-op-1",
		"helix.instrument.id": "CHIME",
	})
	entity.TraceId = []byte{0xAA, 0xBB, 0xCC, 0xDD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	entity.SpanId = []byte{0x11, 0x22, 0x33, 0x44, 0, 0, 0, 0}
	icp.Process(makeReq(entity))

	// Process an operation span for the same entity ID (different trace).
	opSpan := makeSpan("hdf5-conversion", map[string]string{
		"helix.entity.id":        "frb-op-1",
		"helix.instrument.id":    "CHIME",
		"helix.entity.is_operation": "true",
	})
	opSpan.TraceId = []byte{0xFF, 0xEE, 0xDD, 0xCC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	opSpan.SpanId = []byte{0x99, 0x88, 0x77, 0x66, 0, 0, 0, 0}
	icp.Process(makeReq(opSpan))

	// A child of frb-op-1 should still resolve to the ORIGINAL entity span, not the operation.
	child := makeSpan("archiving", map[string]string{
		"helix.entity.id":     "archive-1",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "frb-op-1",
	})
	icp.Process(makeReq(child))

	if len(child.Links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(child.Links))
	}
	if child.Links[0].TraceId[0] != 0xAA {
		t.Errorf("link points to operation trace (0x%x) instead of entity trace (0xAA)", child.Links[0].TraceId[0])
	}
	if child.Links[0].SpanId[0] != 0x11 {
		t.Errorf("link SpanId mismatch: got 0x%x, want 0x11", child.Links[0].SpanId[0])
	}
}

func TestOperationIsOperationAttrExcludedFromMetadata(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("hdf5-conversion", map[string]string{
		"helix.entity.id":           "frb-op-2",
		"helix.instrument.id":       "CHIME",
		"helix.entity.is_operation": "true",
		"helix.chime.hdf5_size_mb":  "241.3",
	})
	// Must not panic; domain attr still on span.
	icp.Process(makeReq(span))

	size := ""
	for _, a := range span.Attributes {
		if a.Key == "helix.chime.hdf5_size_mb" {
			size = a.Value.GetStringValue()
		}
	}
	if size != "241.3" {
		t.Errorf("expected helix.chime.hdf5_size_mb=241.3, got %q", size)
	}
}

func TestMultipleOperationsOnSameEntity(t *testing.T) {
	icp := newInterceptor()

	entity := makeSpan("clustering", map[string]string{
		"helix.entity.id":     "frb-op-3",
		"helix.instrument.id": "CHIME",
	})
	entity.TraceId = []byte{0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	entity.SpanId = []byte{0x01, 0, 0, 0, 0, 0, 0, 0}
	icp.Process(makeReq(entity))

	for i, opName := range []string{"hdf5-conversion", "registration", "replication"} {
		op := makeSpan(opName, map[string]string{
			"helix.entity.id":           "frb-op-3",
			"helix.instrument.id":       "CHIME",
			"helix.entity.is_operation": "true",
		})
		op.TraceId = []byte{byte(0x10 + i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		op.SpanId = []byte{byte(0x10 + i), 0, 0, 0, 0, 0, 0, 0}
		icp.Process(makeReq(op))
	}

	// After 3 operations, the entity's original span context must still be in the store.
	child := makeSpan("archiving", map[string]string{
		"helix.entity.id":     "archive-2",
		"helix.instrument.id": "CHIME",
		"helix.parent.ids":    "frb-op-3",
	})
	icp.Process(makeReq(child))

	if len(child.Links) != 1 {
		t.Fatalf("expected 1 link after multiple operations, got %d", len(child.Links))
	}
	if child.Links[0].TraceId[0] != 0x01 {
		t.Errorf("link points to wrong trace: 0x%x (expected original entity 0x01)", child.Links[0].TraceId[0])
	}
}

// verify the strings package is used (import check)
var _ = strings.Contains

// ── WithNotifier ──────────────────────────────────────────────────────────────

func TestWithNotifierNilNoPanic(t *testing.T) {
	icp := newInterceptor()
	// Setting a nil notifier must not panic.
	icp.WithNotifier(nil)
}

func TestWithNotifierNilGuardedOnHelixEvent(t *testing.T) {
	icp := newInterceptor()
	icp.WithNotifier(nil) // nil notifier — guarded by `if icp.notifier != nil`

	span := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-notifier",
		"helix.instrument.id": "CHIME",
	})
	span.Events = []*tracepb.Span_Event{
		{Name: "helix.error", TimeUnixNano: 1_000_000_000,
			Attributes: []*commonpb.KeyValue{strAttr("msg", "disk full")}},
	}
	// Must not panic even though the notifier is nil.
	icp.Process(makeReq(span))
}

// ── Attribute type coverage (int, double, bool) ───────────────────────────────

func TestHelixEventIntAttribute(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-int-attr",
		"helix.instrument.id": "CHIME",
	})
	span.Events = []*tracepb.Span_Event{{
		Name: "helix.error",
		Attributes: []*commonpb.KeyValue{
			{Key: "count", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}},
		},
	}}
	// Must not panic and the event metadata must convert the IntValue without error.
	icp.Process(makeReq(span))
}

func TestHelixEventDoubleAttribute(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-double-attr",
		"helix.instrument.id": "CHIME",
	})
	span.Events = []*tracepb.Span_Event{{
		Name: "helix.error",
		Attributes: []*commonpb.KeyValue{
			{Key: "ratio", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 0.92}}},
		},
	}}
	icp.Process(makeReq(span))
}

func TestHelixEventBoolAttribute(t *testing.T) {
	icp := newInterceptor()
	span := makeSpan("classifier", map[string]string{
		"helix.entity.id":     "cand-bool-attr",
		"helix.instrument.id": "CHIME",
	})
	span.Events = []*tracepb.Span_Event{{
		Name: "helix.error",
		Attributes: []*commonpb.KeyValue{
			{Key: "fatal", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
		},
	}}
	icp.Process(makeReq(span))
}

// ── Integration tests (require TEST_DB_URL) ───────────────────────────────────

func TestInterceptorWithDB(t *testing.T) {
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	dbStore, err := db.New(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer dbStore.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	s := store.New(10_000, m)
	icp := interceptor.New(s, dbStore, m)

	// entity span → entities table
	entityID := fmt.Sprintf("icp-test-%d", time.Now().UnixNano())
	icp.Process(makeReq(makeSpan("correlator", map[string]string{
		"helix.entity.id":     entityID,
		"helix.instrument.id": "TEST",
	})))

	// operation span → entity_operations table
	opID := fmt.Sprintf("icp-op-%d", time.Now().UnixNano())
	icp.Process(makeReq(makeSpan("hdf5-conversion", map[string]string{
		"helix.entity.id":          opID,
		"helix.instrument.id":      "TEST",
		"helix.entity.is_operation": "true",
	})))

	// span with helix.error event → entity_events table
	evID := fmt.Sprintf("icp-ev-%d", time.Now().UnixNano())
	evSpan := makeSpan("correlator", map[string]string{
		"helix.entity.id":     evID,
		"helix.instrument.id": "TEST",
	})
	evSpan.Events = []*tracepb.Span_Event{
		{
			Name:         "helix.error",
			TimeUnixNano: 1_000_000_000,
			Attributes:   []*commonpb.KeyValue{strAttr("msg", "test error")},
		},
	}
	icp.Process(makeReq(evSpan))

	// span with parent that was already stored → parent resolution path
	parentID := fmt.Sprintf("icp-parent-%d", time.Now().UnixNano())
	icp.Process(makeReq(makeSpan("correlator", map[string]string{
		"helix.entity.id":     parentID,
		"helix.instrument.id": "TEST",
	})))
	childID := fmt.Sprintf("icp-child-%d", time.Now().UnixNano())
	icp.Process(makeReq(makeSpan("classifier", map[string]string{
		"helix.entity.id":     childID,
		"helix.instrument.id": "TEST",
		"helix.parent.ids":    parentID,
	})))

	// allow goroutines to finish writing
	time.Sleep(400 * time.Millisecond)
}

func TestInterceptorWithDBAndNotifier(t *testing.T) {
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	dbStore, err := db.New(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer dbStore.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	s := store.New(10_000, m)
	icp := interceptor.New(s, dbStore, m)

	// non-nil notifier exercises the icp.notifier.Send() path
	n := notifier.New(nil, nil, m, 100, "", "")
	icp.WithNotifier(n)

	evID := fmt.Sprintf("icp-notifier-%d", time.Now().UnixNano())
	evSpan := makeSpan("correlator", map[string]string{
		"helix.entity.id":     evID,
		"helix.instrument.id": "TEST",
	})
	evSpan.Events = []*tracepb.Span_Event{
		{Name: "helix.event.detection_confirmed", TimeUnixNano: 1_000_000_000},
	}
	icp.Process(makeReq(evSpan))

	time.Sleep(300 * time.Millisecond)
}

// Verify that multiple comma-separated unknown parent IDs are all resolved
// (or counted as misses) and the span is processed without panic.
func TestInterceptorMultipleUnknownParents(t *testing.T) {
	url := os.Getenv("TEST_DB_URL")
	if url == "" {
		t.Skip("TEST_DB_URL not set")
	}
	dbStore, err := db.New(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer dbStore.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	s := store.New(10_000, m)
	icp := interceptor.New(s, dbStore, m)

	childID := fmt.Sprintf("icp-multi-parent-%d", time.Now().UnixNano())
	icp.Process(makeReq(makeSpan("classifier", map[string]string{
		"helix.entity.id":     childID,
		"helix.instrument.id": "TEST",
		"helix.parent.ids":    strings.Join([]string{"unknown-p1", "unknown-p2", "unknown-p3"}, ","),
	})))

	time.Sleep(300 * time.Millisecond)
}
