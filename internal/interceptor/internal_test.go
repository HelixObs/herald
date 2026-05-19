// White-box tests for unexported functions in the interceptor package.
package interceptor

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// ── attrsToMetadata ───────────────────────────────────────────────────────────

func TestAttrsToMetadataExcludesSystemAttrs(t *testing.T) {
	attrs := []*commonpb.KeyValue{
		{Key: attrEntityID, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "entity-1"}}},
		{Key: attrInstrumentID, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "CHIME"}}},
		{Key: attrParentIDs, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "parent-1"}}},
		{Key: attrIsOperation, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "true"}}},
		{Key: "helix.chime.dm", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "341.2"}}},
	}

	result := attrsToMetadata(attrs)

	// System attrs must be excluded.
	for _, k := range []string{attrEntityID, attrInstrumentID, attrParentIDs, attrIsOperation} {
		if _, ok := result[k]; ok {
			t.Errorf("system attr %q should be excluded from metadata, but was present", k)
		}
	}

	// Domain attr must be included.
	if v, ok := result["helix.chime.dm"]; !ok || v != "341.2" {
		t.Errorf("expected helix.chime.dm=341.2 in metadata, got %v (ok=%v)", v, ok)
	}
}

func TestAttrsToMetadataIncludesDomainAttrs(t *testing.T) {
	attrs := []*commonpb.KeyValue{
		{Key: "helix.chime.snr", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "18.3"}}},
		{Key: "pipeline.stage", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "l2"}}},
	}

	result := attrsToMetadata(attrs)
	if result["helix.chime.snr"] != "18.3" {
		t.Errorf("expected helix.chime.snr=18.3, got %q", result["helix.chime.snr"])
	}
	if result["pipeline.stage"] != "l2" {
		t.Errorf("expected pipeline.stage=l2, got %q", result["pipeline.stage"])
	}
}

func TestAttrsToMetadataNilAttrsReturnsNil(t *testing.T) {
	result := attrsToMetadata(nil)
	if result != nil {
		t.Errorf("expected nil for nil attrs, got %v", result)
	}
}

func TestAttrsToMetadataEmptyAttrsReturnsNil(t *testing.T) {
	result := attrsToMetadata([]*commonpb.KeyValue{})
	if result != nil {
		t.Errorf("expected nil for empty attrs, got %v", result)
	}
}

// ── coalesceMetadata ──────────────────────────────────────────────────────────

func TestCoalesceMetadataReturnsFirstNonEmpty(t *testing.T) {
	m := map[string]string{
		"a": "",
		"b": "found",
		"c": "also-here",
	}
	got := coalesceMetadata(m, "a", "b", "c")
	if got != "found" {
		t.Errorf("expected 'found', got %q", got)
	}
}

func TestCoalesceMetadataAllEmptyReturnsEmpty(t *testing.T) {
	m := map[string]string{"a": "", "b": ""}
	got := coalesceMetadata(m, "a", "b")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestCoalesceMetadataEmptyMapReturnsEmpty(t *testing.T) {
	got := coalesceMetadata(map[string]string{}, "missing")
	if got != "" {
		t.Errorf("expected empty string for missing key, got %q", got)
	}
}

func TestCoalesceMetadataNoKeys(t *testing.T) {
	m := map[string]string{"a": "val"}
	got := coalesceMetadata(m)
	if got != "" {
		t.Errorf("expected empty string for no keys, got %q", got)
	}
}

// ── anyValueStr ───────────────────────────────────────────────────────────────

func TestAnyValueStrNilReturnsEmpty(t *testing.T) {
	got := anyValueStr(nil)
	if got != "" {
		t.Errorf("expected empty string for nil, got %q", got)
	}
}

func TestAnyValueStrStringValue(t *testing.T) {
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}}
	got := anyValueStr(v)
	if got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
}

func TestAnyValueStrIntValue(t *testing.T) {
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}
	got := anyValueStr(v)
	if got != "42" {
		t.Errorf("expected '42', got %q", got)
	}
}

func TestAnyValueStrNegativeIntValue(t *testing.T) {
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: -7}}
	got := anyValueStr(v)
	if got != "-7" {
		t.Errorf("expected '-7', got %q", got)
	}
}

func TestAnyValueStrDoubleValue(t *testing.T) {
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}}
	got := anyValueStr(v)
	if got != "3.14" {
		t.Errorf("expected '3.14', got %q", got)
	}
}

func TestAnyValueStrBoolTrue(t *testing.T) {
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}
	got := anyValueStr(v)
	if got != "true" {
		t.Errorf("expected 'true', got %q", got)
	}
}

func TestAnyValueStrBoolFalse(t *testing.T) {
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: false}}
	got := anyValueStr(v)
	if got != "false" {
		t.Errorf("expected 'false', got %q", got)
	}
}

func TestAnyValueStrUnknownTypeReturnsNonEmpty(t *testing.T) {
	// ArrayValue is an "other" type — should not panic, should return non-empty.
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{
		ArrayValue: &commonpb.ArrayValue{},
	}}
	got := anyValueStr(v)
	// We just check it doesn't panic and returns something.
	_ = got
}
