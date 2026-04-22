// Package interceptor is the core intelligence layer of the HelixObs gateway.
//
// For every OTLP span that carries a helix.entity.id attribute the interceptor:
//
//  1. Reads helix.parent.ids (comma-separated entity IDs set by the client).
//  2. Looks up each parent in the server-side TraceStore.
//     Found parents are added as OTel span links so Tempo can render the DAG.
//     Unknown parents (arrived out of order) are counted via a metric.
//  3. Registers the current span in the TraceStore so future children can link to it.
//  4. Extracts every span event whose name starts with "helix." and schedules
//     a best-effort TimescaleDB write via a background goroutine.
//  5. Writes the entity record itself to TimescaleDB (also best-effort).
//  6. Increments the relevant Prometheus counters.
//
// Spans without helix.entity.id are passed through without modification.
package interceptor

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/HelixObs/gateway/internal/db"
	"github.com/HelixObs/gateway/internal/metrics"
	"github.com/HelixObs/gateway/internal/store"
)

const (
	attrEntityID     = "helix.entity.id"
	attrInstrumentID = "helix.instrument.id"
	attrParentIDs    = "helix.parent.ids"
	attrIsOperation  = "helix.entity.is_operation"
)

// Interceptor processes an ExportTraceServiceRequest in-place before it is
// forwarded to the downstream OTel Collector.
type Interceptor struct {
	store   *store.TraceStore
	db      *db.Store
	metrics *metrics.Metrics
}

func New(s *store.TraceStore, d *db.Store, m *metrics.Metrics) *Interceptor {
	return &Interceptor{store: s, db: d, metrics: m}
}

// Process mutates each helix-annotated span in req — adding span links and
// scheduling DB writes — before the caller forwards req downstream.
func (icp *Interceptor) Process(req *collectortracepb.ExportTraceServiceRequest) {
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				if span != nil {
					icp.processSpan(span)
				}
			}
		}
	}
}

func (icp *Interceptor) processSpan(span *tracepb.Span) {
	icp.metrics.SpansReceivedTotal.Inc()
	start := time.Now()

	entityID := attrStr(span.Attributes, attrEntityID)
	if entityID == "" {
		icp.metrics.SpansPassthroughTotal.Inc()
		return // not a HelixObs span; pass through unchanged
	}
	defer func() {
		instrumentID := attrStr(span.Attributes, attrInstrumentID)
		icp.metrics.SpanProcessingDuration.WithLabelValues(instrumentID).Observe(time.Since(start).Seconds())
	}()

	instrumentID := attrStr(span.Attributes, attrInstrumentID)
	parentIDsStr := attrStr(span.Attributes, attrParentIDs)
	isOperation  := attrStr(span.Attributes, attrIsOperation) == "true"

	// ── Resolve parent entity IDs → span links ────────────────────────
	var parentIDs []string
	if parentIDsStr != "" {
		for _, pid := range strings.Split(parentIDsStr, ",") {
			pid = strings.TrimSpace(pid)
			if pid == "" {
				continue
			}
			parentIDs = append(parentIDs, pid)

			ref := icp.store.Get(pid)
			if ref != nil {
				span.Links = append(span.Links, &tracepb.Span_Link{
					TraceId: ref.TraceID,
					SpanId:  ref.SpanID,
				})
				icp.metrics.ParentResolutionTotal.WithLabelValues(instrumentID, "success").Inc()
			} else {
				icp.metrics.ParentResolutionTotal.WithLabelValues(instrumentID, "failed").Inc()
				icp.metrics.ParentResolutionFailedTotal.WithLabelValues(instrumentID).Inc()
			}
		}
	}

	// ── Register in TraceStore — only for entities, not operations ────
	// Operations must not overwrite the entity's stored SpanContext; doing
	// so would cause future children to link to an operation trace instead
	// of the entity's creation trace.
	if !isOperation {
		icp.store.Put(entityID, &store.SpanRef{
			TraceID: span.TraceId,
			SpanID:  span.SpanId,
		})
	}

	// ── Extract helix.* span events ───────────────────────────────────
	var events []db.EntityEvent
	for _, ev := range span.Events {
		if !strings.HasPrefix(ev.Name, "helix.") {
			continue
		}
		events = append(events, db.EntityEvent{
			InstrumentID: instrumentID,
			EntityID:     entityID,
			EventName:    ev.Name,
			TimestampNs:  int64(ev.TimeUnixNano),
			Metadata:     attrsToMap(ev.Attributes),
		})
	}

	// ── Prometheus ────────────────────────────────────────────────────
	status := "ok"
	for _, ev := range events {
		icp.metrics.EventsTotal.WithLabelValues(instrumentID, ev.EventName).Inc()
		if ev.EventName == "helix.error" {
			icp.metrics.ErrorsTotal.WithLabelValues(instrumentID).Inc()
			status = "error"
		}
	}
	icp.metrics.EntitiesTotal.WithLabelValues(instrumentID, span.Name, status).Inc()

	// ── DB writes — async, best-effort ───────────────────────────────
	if icp.db == nil {
		return // nil DB is used in unit tests
	}

	if isOperation {
		op := db.EntityOperation{
			EntityID:     entityID,
			InstrumentID: instrumentID,
			Operation:    span.Name,
			TraceID:      hex.EncodeToString(span.TraceId),
			TimestampNs:  int64(span.StartTimeUnixNano),
			Metadata:     attrsToMetadata(span.Attributes),
		}
		go func() {
			if err := icp.db.WriteEntityOperation(context.Background(), op); err != nil {
				slog.Warn("entity operation write failed", "entity_id", op.EntityID, "operation", op.Operation, "error", err)
				icp.metrics.DBWriteErrorsTotal.Inc()
			}
		}()
	} else {
		entity := db.Entity{
			ID:           entityID,
			InstrumentID: instrumentID,
			TraceID:      hex.EncodeToString(span.TraceId),
			TimestampNs:  int64(span.StartTimeUnixNano),
			ParentIDs:    parentIDs,
			Metadata:     attrsToMetadata(span.Attributes),
		}
		go func() {
			if err := icp.db.WriteEntity(context.Background(), entity); err != nil {
				slog.Warn("entity write failed", "entity_id", entity.ID, "error", err)
				icp.metrics.DBWriteErrorsTotal.Inc()
			}
		}()
	}

	for _, ev := range events {
		ev := ev
		go func() {
			if err := icp.db.WriteEntityEvent(context.Background(), ev); err != nil {
				slog.Warn("entity event write failed",
					"entity_id", ev.EntityID, "event", ev.EventName, "error", err)
				icp.metrics.DBWriteErrorsTotal.Inc()
			}
		}()
	}
}

// ── Attribute helpers ─────────────────────────────────────────────────────────

func attrStr(attrs []*commonpb.KeyValue, key string) string {
	for _, kv := range attrs {
		if kv.Key == key {
			if sv, ok := kv.Value.Value.(*commonpb.AnyValue_StringValue); ok {
				return sv.StringValue
			}
		}
	}
	return ""
}

// attrsToMap converts a KeyValue slice to map[string]string.
func attrsToMap(attrs []*commonpb.KeyValue) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.Key] = anyValueStr(kv.Value)
	}
	return m
}

// attrsToMetadata converts span attributes to the domain metadata map,
// excluding HelixObs system attributes.
func attrsToMetadata(attrs []*commonpb.KeyValue) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		if kv.Key == attrEntityID || kv.Key == attrInstrumentID || kv.Key == attrParentIDs || kv.Key == attrIsOperation {
			continue
		}
		m[kv.Key] = anyValueStr(kv.Value)
	}
	return m
}

func anyValueStr(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch t := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return t.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(t.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(t.DoubleValue, 'f', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	default:
		return fmt.Sprintf("%v", v.Value)
	}
}
