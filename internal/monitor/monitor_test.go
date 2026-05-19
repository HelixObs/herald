package monitor_test

import (
	"strings"
	"testing"

	"github.com/HelixObs/gateway/internal/db"
	"github.com/HelixObs/gateway/internal/monitor"
)

// mockPlugin is a test-local plugin for injection testing.
type mockPlugin struct {
	keys []string
}

func (m *mockPlugin) Config() monitor.PlotConfig { return monitor.PlotConfig{} }
func (m *mockPlugin) RequiredKeys() []string      { return m.keys }
func (m *mockPlugin) Extract(_ db.RawEntityRow) (float64, float64, bool) {
	return 0, 0, false
}

// ── Get ───────────────────────────────────────────────────────────────────────

func TestGetChimeDMTime(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("expected non-nil plugin for chime_dm_time")
	}
}

func TestGetChimeBeamTime(t *testing.T) {
	p := monitor.Get("chime_beam_time")
	if p == nil {
		t.Fatal("expected non-nil plugin for chime_beam_time")
	}
}

func TestGetNonexistent(t *testing.T) {
	p := monitor.Get("nonexistent_plugin")
	if p != nil {
		t.Errorf("expected nil for unknown plugin, got %v", p)
	}
}

// ── AllConfigs ────────────────────────────────────────────────────────────────

func TestAllConfigsAtLeastTwo(t *testing.T) {
	cfgs := monitor.AllConfigs()
	if len(cfgs) < 2 {
		t.Errorf("expected >= 2 registered configs, got %d", len(cfgs))
	}
}

// ── MetadataFilter ────────────────────────────────────────────────────────────

func TestMetadataFilterDMPlot(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	filter, err := monitor.MetadataFilter(p)
	if err != nil {
		t.Fatalf("MetadataFilter error: %v", err)
	}
	if !strings.Contains(filter, "metadata ? 'helix.chime.dm'") {
		t.Errorf("expected filter to contain metadata ? 'helix.chime.dm', got: %q", filter)
	}
	if !strings.Contains(filter, "metadata ? 'helix.chime.snr'") {
		t.Errorf("expected filter to contain metadata ? 'helix.chime.snr', got: %q", filter)
	}
}

func TestMetadataFilterInvalidKeyReturnsError(t *testing.T) {
	// Keys with invalid characters should cause an error.
	p := &mockPlugin{keys: []string{"valid.key", "invalid key; DROP TABLE"}}
	_, err := monitor.MetadataFilter(p)
	if err == nil {
		t.Error("expected error for invalid key name, got nil")
	}
}

func TestMetadataFilterNoKeys(t *testing.T) {
	p := &mockPlugin{keys: []string{}}
	filter, err := monitor.MetadataFilter(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if filter != "" {
		t.Errorf("expected empty filter for no keys, got %q", filter)
	}
}

// ── Bin ───────────────────────────────────────────────────────────────────────

func makeDMRow(id string, ts int64, dm, snr string) db.RawEntityRow {
	return db.RawEntityRow{
		ID:          id,
		TimestampNs: ts,
		Metadata:    map[string]string{"helix.chime.dm": dm, "helix.chime.snr": snr},
	}
}

func TestBinMatchingRows(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	cfg := p.Config()

	fromNs := int64(1_000_000_000)
	toNs := int64(1_000_000_000 + 10*60*1_000_000_000) // 10 minutes later

	rows := []db.RawEntityRow{
		makeDMRow("entity-1", fromNs+int64(1*60*1_000_000_000), "341.2", "18.3"),
		makeDMRow("entity-2", fromNs+int64(5*60*1_000_000_000), "100.0", "12.5"),
	}

	bins, snrMax := monitor.Bin(p, cfg, rows, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) == 0 {
		t.Error("expected non-empty bins for matching rows")
	}
	if snrMax <= 0 {
		t.Errorf("expected SNRMax > 0, got %v", snrMax)
	}
}

func TestBinOutOfYRange(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	cfg := p.Config() // YMin=0, YMax=3000

	fromNs := int64(1_000_000_000)
	toNs := fromNs + int64(10*60*1_000_000_000)

	// DM value way above YMax=3000
	rows := []db.RawEntityRow{
		makeDMRow("entity-oob", fromNs+int64(1*60*1_000_000_000), "99999.0", "18.3"),
	}

	bins, snrMax := monitor.Bin(p, cfg, rows, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) != 0 {
		t.Errorf("expected empty bins for out-of-range y, got %d bins", len(bins))
	}
	if snrMax != 0 {
		t.Errorf("expected SNRMax=0 for empty result, got %v", snrMax)
	}
}

func TestBinEmptyRows(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	cfg := p.Config()

	fromNs := int64(1_000_000_000)
	toNs := fromNs + int64(10*60*1_000_000_000)

	bins, snrMax := monitor.Bin(p, cfg, nil, fromNs, toNs, 100, 100, -1, 0)
	if len(bins) != 0 {
		t.Errorf("expected empty bins for no rows, got %d", len(bins))
	}
	if snrMax != 0 {
		t.Errorf("expected SNRMax=0 for no rows, got %v", snrMax)
	}
}

func TestBinYMaxOverride(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	cfg := p.Config() // default YMax=3000

	fromNs := int64(1_000_000_000)
	toNs := fromNs + int64(10*60*1_000_000_000)

	// DM=500, within override yMax=600 but would be out of default yMax=3000 range still OK
	rows := []db.RawEntityRow{
		makeDMRow("entity-ymax", fromNs+int64(1*60*1_000_000_000), "500.0", "20.0"),
	}

	// yMaxOverride=600 means effective range is [0, 600]
	bins, snrMax := monitor.Bin(p, cfg, rows, fromNs, toNs, 100, 100, -1, 600)
	if len(bins) == 0 {
		t.Error("expected non-empty bins with yMaxOverride=600 and dm=500")
	}
	if snrMax != 20.0 {
		t.Errorf("expected SNRMax=20.0, got %v", snrMax)
	}
}

// ── chimeDMPlot Extract ───────────────────────────────────────────────────────

func TestChimeDMExtractValid(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	row := db.RawEntityRow{
		ID:          "e1",
		TimestampNs: 1000,
		Metadata: map[string]string{
			"helix.chime.dm":  "341.2",
			"helix.chime.snr": "18.3",
		},
	}
	y, snr, ok := p.Extract(row)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if y != 341.2 {
		t.Errorf("expected y=341.2, got %v", y)
	}
	if snr != 18.3 {
		t.Errorf("expected snr=18.3, got %v", snr)
	}
}

func TestChimeDMExtractMissingKeys(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	row := db.RawEntityRow{
		ID:          "e2",
		TimestampNs: 1000,
		Metadata:    map[string]string{"helix.chime.dm": "341.2"}, // missing snr
	}
	_, _, ok := p.Extract(row)
	if ok {
		t.Error("expected ok=false for missing snr key")
	}
}

func TestChimeDMExtractUnparseableFloat(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	row := db.RawEntityRow{
		ID:          "e3",
		TimestampNs: 1000,
		Metadata: map[string]string{
			"helix.chime.dm":  "not-a-float",
			"helix.chime.snr": "18.3",
		},
	}
	_, _, ok := p.Extract(row)
	if ok {
		t.Error("expected ok=false for unparseable dm float")
	}
}

// ── chimeBeamPlot Extract ─────────────────────────────────────────────────────

func TestChimeBeamExtractValid(t *testing.T) {
	p := monitor.Get("chime_beam_time")
	if p == nil {
		t.Fatal("chime_beam_time plugin not registered")
	}
	// beam_no=1024 → 1024 % 1000 = 24
	row := db.RawEntityRow{
		ID:          "e4",
		TimestampNs: 1000,
		Metadata: map[string]string{
			"helix.chime.beam_no": "1024",
			"helix.chime.snr":     "18.3",
		},
	}
	y, snr, ok := p.Extract(row)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if y != 24 {
		t.Errorf("expected y=24 (1024%%1000), got %v", y)
	}
	if snr != 18.3 {
		t.Errorf("expected snr=18.3, got %v", snr)
	}
}

func TestChimeBeamExtractMissingKeys(t *testing.T) {
	p := monitor.Get("chime_beam_time")
	if p == nil {
		t.Fatal("chime_beam_time plugin not registered")
	}
	row := db.RawEntityRow{
		ID:          "e5",
		TimestampNs: 1000,
		Metadata:    map[string]string{"helix.chime.beam_no": "1024"}, // missing snr
	}
	_, _, ok := p.Extract(row)
	if ok {
		t.Error("expected ok=false for missing snr key")
	}
}

func TestChimeBeamExtractUnparseableInt(t *testing.T) {
	p := monitor.Get("chime_beam_time")
	if p == nil {
		t.Fatal("chime_beam_time plugin not registered")
	}
	row := db.RawEntityRow{
		ID:          "e6",
		TimestampNs: 1000,
		Metadata: map[string]string{
			"helix.chime.beam_no": "not-an-int",
			"helix.chime.snr":     "18.3",
		},
	}
	_, _, ok := p.Extract(row)
	if ok {
		t.Error("expected ok=false for unparseable beam_no int")
	}
}

// ── Config and RequiredKeys ───────────────────────────────────────────────────

func TestChimeDMConfig(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	cfg := p.Config()
	if cfg.Name != "chime_dm_time" {
		t.Errorf("expected name=chime_dm_time, got %q", cfg.Name)
	}
	if cfg.YMin != 0 {
		t.Errorf("expected YMin=0, got %v", cfg.YMin)
	}
	if cfg.YMax != 3000 {
		t.Errorf("expected YMax=3000, got %v", cfg.YMax)
	}
}

func TestChimeDMRequiredKeys(t *testing.T) {
	p := monitor.Get("chime_dm_time")
	if p == nil {
		t.Fatal("chime_dm_time plugin not registered")
	}
	keys := p.RequiredKeys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 required keys, got %d", len(keys))
	}
	keySet := map[string]bool{}
	for _, k := range keys {
		keySet[k] = true
	}
	if !keySet["helix.chime.dm"] {
		t.Error("expected helix.chime.dm in required keys")
	}
	if !keySet["helix.chime.snr"] {
		t.Error("expected helix.chime.snr in required keys")
	}
}

func TestChimeBeamConfig(t *testing.T) {
	p := monitor.Get("chime_beam_time")
	if p == nil {
		t.Fatal("chime_beam_time plugin not registered")
	}
	cfg := p.Config()
	if cfg.Name != "chime_beam_time" {
		t.Errorf("expected name=chime_beam_time, got %q", cfg.Name)
	}
	if cfg.YMin != 0 {
		t.Errorf("expected YMin=0, got %v", cfg.YMin)
	}
	if cfg.YMax != 255 {
		t.Errorf("expected YMax=255, got %v", cfg.YMax)
	}
}

func TestChimeBeamRequiredKeys(t *testing.T) {
	p := monitor.Get("chime_beam_time")
	if p == nil {
		t.Fatal("chime_beam_time plugin not registered")
	}
	keys := p.RequiredKeys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 required keys, got %d", len(keys))
	}
	keySet := map[string]bool{}
	for _, k := range keys {
		keySet[k] = true
	}
	if !keySet["helix.chime.beam_no"] {
		t.Error("expected helix.chime.beam_no in required keys")
	}
	if !keySet["helix.chime.snr"] {
		t.Error("expected helix.chime.snr in required keys")
	}
}
