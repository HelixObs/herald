package monitor_test

import (
	"testing"

	"github.com/HelixObs/herald/internal/db"
	"github.com/HelixObs/herald/internal/monitor"
)

// ── ValidateFieldName ─────────────────────────────────────────────────────────

func TestValidateFieldNameValid(t *testing.T) {
	for _, k := range []string{"dm", "helix.chime.dm", "snr_value", "a1.b2.c3"} {
		if !monitor.ValidateFieldName(k) {
			t.Errorf("expected %q to be valid", k)
		}
	}
}

func TestValidateFieldNameInvalid(t *testing.T) {
	for _, k := range []string{"", "has space", "CamelCase", "key;DROP", "k/v"} {
		if monitor.ValidateFieldName(k) {
			t.Errorf("expected %q to be invalid", k)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func row(id string, ts int64, meta map[string]string) db.RawEntityRow {
	return db.RawEntityRow{ID: id, TimestampNs: ts, Metadata: meta}
}

const (
	fromNs = int64(0)
	toNs   = int64(10 * 60 * 1_000_000_000) // 10 minutes
)

// ── Bin — basic ───────────────────────────────────────────────────────────────

func TestBinEmptyRows(t *testing.T) {
	bins, wMax := monitor.Bin(nil, "dm", "", 0, 3000, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) != 0 {
		t.Errorf("expected 0 bins for nil rows, got %d", len(bins))
	}
	if wMax != 0 {
		t.Errorf("expected weightMax=0, got %v", wMax)
	}
}

func TestBinMissingYField(t *testing.T) {
	rows := []db.RawEntityRow{
		row("e1", toNs/2, map[string]string{"other": "42"}),
	}
	bins, _ := monitor.Bin(rows, "dm", "", 0, 3000, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) != 0 {
		t.Errorf("expected 0 bins when y_field absent, got %d", len(bins))
	}
}

func TestBinOutOfYRange(t *testing.T) {
	rows := []db.RawEntityRow{
		row("e1", toNs/2, map[string]string{"dm": "99999"}),
	}
	bins, _ := monitor.Bin(rows, "dm", "", 0, 3000, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) != 0 {
		t.Errorf("expected 0 bins for y outside range, got %d", len(bins))
	}
}

func TestBinMatchingRowsUniformWeight(t *testing.T) {
	rows := []db.RawEntityRow{
		row("e1", toNs/4, map[string]string{"dm": "341.2"}),
		row("e2", toNs/2, map[string]string{"dm": "100.0"}),
	}
	bins, wMax := monitor.Bin(rows, "dm", "", 0, 3000, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) == 0 {
		t.Error("expected non-empty bins")
	}
	// Uniform weight defaults to 1.0
	if wMax != 1.0 {
		t.Errorf("expected weightMax=1.0 for uniform weight, got %v", wMax)
	}
}

func TestBinMatchingRowsWithWeight(t *testing.T) {
	rows := []db.RawEntityRow{
		row("e1", toNs/4, map[string]string{"dm": "341.2", "snr": "18.3"}),
		row("e2", toNs/2, map[string]string{"dm": "100.0", "snr": "12.5"}),
	}
	bins, wMax := monitor.Bin(rows, "dm", "snr", 0, 3000, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) == 0 {
		t.Error("expected non-empty bins")
	}
	if wMax != 18.3 {
		t.Errorf("expected weightMax=18.3, got %v", wMax)
	}
}

func TestBinMissingWeightFieldDefaultsToOne(t *testing.T) {
	// weight_field set but absent from metadata → weight defaults to 1.
	rows := []db.RawEntityRow{
		row("e1", toNs/2, map[string]string{"dm": "500.0"}),
	}
	bins, wMax := monitor.Bin(rows, "dm", "snr", 0, 3000, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) == 0 {
		t.Error("expected non-empty bins even when weight field absent")
	}
	if wMax != 1.0 {
		t.Errorf("expected weightMax=1.0 fallback, got %v", wMax)
	}
}

// ── Bin — best-candidate selection per cell ───────────────────────────────────

func TestBinBestCandidatePerCell(t *testing.T) {
	// Two rows land in the same bin; the higher-weight one should win.
	midTs := toNs / 2
	rows := []db.RawEntityRow{
		row("low", midTs, map[string]string{"dm": "500.0", "snr": "5.0"}),
		row("high", midTs+1, map[string]string{"dm": "500.1", "snr": "20.0"}),
	}
	bins, _ := monitor.Bin(rows, "dm", "snr", 0, 3000, fromNs, toNs, 10, 10, -1, 0)
	for _, b := range bins {
		if b.ID == "low" {
			t.Error("low-weight candidate should have been evicted by high-weight candidate")
		}
	}
}

// ── Bin — y-axis overrides ────────────────────────────────────────────────────

func TestBinYMaxOverride(t *testing.T) {
	rows := []db.RawEntityRow{
		row("e1", toNs/2, map[string]string{"dm": "500.0"}),
	}
	// yMaxOverride=600 → effective [0, 600]; dm=500 should fit
	bins, _ := monitor.Bin(rows, "dm", "", 0, 3000, fromNs, toNs, 100, 100, -1, 600)
	if len(bins) == 0 {
		t.Error("expected non-empty bins with yMaxOverride=600 and dm=500")
	}
}

func TestBinYMinOverride(t *testing.T) {
	rows := []db.RawEntityRow{
		row("e1", toNs/2, map[string]string{"dm": "50.0"}),
	}
	// yMinOverride=100 → effective [100, 3000]; dm=50 is now below range
	bins, _ := monitor.Bin(rows, "dm", "", 0, 3000, fromNs, toNs, 100, 100, 100, 0)
	if len(bins) != 0 {
		t.Errorf("expected 0 bins with yMinOverride=100 and dm=50, got %d", len(bins))
	}
}

// ── Bin — meta payload ────────────────────────────────────────────────────────

func TestBinMetaContainsYAndWeightFields(t *testing.T) {
	rows := []db.RawEntityRow{
		row("e1", toNs/2, map[string]string{"dm": "341.2", "snr": "18.3", "extra": "ignored"}),
	}
	bins, _ := monitor.Bin(rows, "dm", "snr", 0, 3000, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) == 0 {
		t.Fatal("expected non-empty bins")
	}
	b := bins[0]
	if b.Meta["dm"] != "341.2" {
		t.Errorf("expected meta[dm]=341.2, got %q", b.Meta["dm"])
	}
	if b.Meta["snr"] != "18.3" {
		t.Errorf("expected meta[snr]=18.3, got %q", b.Meta["snr"])
	}
	if _, ok := b.Meta["extra"]; ok {
		t.Error("extra field should not appear in meta")
	}
}
