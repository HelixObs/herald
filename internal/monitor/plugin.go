// Package monitor provides the plugin interface and binning logic for the
// entity monitor API (/api/v1/monitor/bins).
package monitor

import (
	"fmt"
	"regexp"

	"github.com/HelixObs/gateway/internal/db"
)

// PlotConfig describes one y-axis interpretation available on the monitor.
type PlotConfig struct {
	Name  string  `json:"name"`
	Label string  `json:"label"`
	YMin  float64 `json:"y_min"`
	YMax  float64 `json:"y_max"`
	YUnit string  `json:"y_unit"`
}

// BinResult is one non-empty cell in the binned output grid.
type BinResult struct {
	T    int               `json:"t"`    // x bin index
	Y    int               `json:"y"`    // y bin index
	SNR  float64           `json:"snr"`  // max SNR in this cell
	ID   string            `json:"id"`   // entity ID of the best candidate
	Ts   int64             `json:"ts"`   // timestamp_ns of the best candidate
	YVal float64           `json:"yv"`   // raw y value of the best candidate
	Meta map[string]string `json:"meta"` // plugin required-key values for the best candidate
}

// BinsResponse is the full response body for GET /api/v1/monitor/bins.
type BinsResponse struct {
	Bins   []BinResult `json:"bins"`
	TBins  int         `json:"t_bins"`
	YBins  int         `json:"y_bins"`
	FromNs int64       `json:"from_ns"`
	ToNs   int64       `json:"to_ns"`
	YMin   float64     `json:"y_min"`
	YMax   float64     `json:"y_max"`
	SNRMax float64     `json:"snr_max"`
}

// Plugin defines one y-axis interpretation for the monitor.
type Plugin interface {
	Config() PlotConfig
	RequiredKeys() []string
	Extract(e db.RawEntityRow) (y, snr float64, ok bool)
}

// MinWindowNs is the minimum query window (5 minutes).
const MinWindowNs int64 = 5 * 60 * 1_000_000_000

// MaxWindowNs is the maximum query window (24 hours).
const MaxWindowNs int64 = 24 * 60 * 60 * 1_000_000_000

var registry []Plugin

// Register adds a plugin. Called from init() in domain files.
func Register(p Plugin) {
	registry = append(registry, p)
}

// Get returns the plugin with the given name, or nil.
func Get(name string) Plugin {
	for _, p := range registry {
		if p.Config().Name == name {
			return p
		}
	}
	return nil
}

// AllConfigs returns the PlotConfig for every registered plugin.
func AllConfigs() []PlotConfig {
	out := make([]PlotConfig, 0, len(registry))
	for _, p := range registry {
		out = append(out, p.Config())
	}
	return out
}

var metaKeyRE = regexp.MustCompile(`^[a-z0-9_.]+$`)

// MetadataFilter builds a SQL WHERE fragment requiring all plugin keys to exist
// in the metadata JSONB column. Keys are validated against ^[a-z0-9_.]+$ to
// prevent injection. Returns empty string if there are no required keys.
func MetadataFilter(p Plugin) (string, error) {
	keys := p.RequiredKeys()
	if len(keys) == 0 {
		return "", nil
	}
	clause := ""
	for i, k := range keys {
		if !metaKeyRE.MatchString(k) {
			return "", fmt.Errorf("invalid metadata key: %q", k)
		}
		if i > 0 {
			clause += " AND "
		}
		clause += fmt.Sprintf("metadata ? '%s'", k)
	}
	return clause, nil
}

// binCell tracks the best candidate seen in one grid cell.
type binCell struct {
	snr  float64
	id   string
	ts   int64
	yVal float64
	meta map[string]string
}

// Bin runs the Go binning loop over rows and returns the populated cells.
// yMinOverride replaces cfg.YMin when >= 0 and < effective yMax.
// yMaxOverride replaces cfg.YMax when > effective yMin.
func Bin(p Plugin, cfg PlotConfig, rows []db.RawEntityRow, fromNs, toNs int64, tBins, yBins int, yMinOverride, yMaxOverride float64) ([]BinResult, float64) {
	yMin := cfg.YMin
	yMax := cfg.YMax
	if yMaxOverride > yMin {
		yMax = yMaxOverride
	}
	if yMinOverride >= 0 && yMinOverride < yMax {
		yMin = yMinOverride
	}

	tRange := float64(toNs - fromNs)
	yRange := yMax - yMin
	grid := make([]binCell, tBins*yBins)
	var globalSNRMax float64

	for _, row := range rows {
		y, snr, ok := p.Extract(row)
		if !ok || y < yMin || y > yMax {
			continue
		}
		tBin := int(float64(row.TimestampNs-fromNs) / tRange * float64(tBins))
		yBin := int((y - yMin) / yRange * float64(yBins))
		if tBin < 0 {
			tBin = 0
		} else if tBin >= tBins {
			tBin = tBins - 1
		}
		if yBin < 0 {
			yBin = 0
		} else if yBin >= yBins {
			yBin = yBins - 1
		}
		idx := tBin*yBins + yBin
		if snr > grid[idx].snr {
			meta := make(map[string]string, len(p.RequiredKeys()))
			for _, k := range p.RequiredKeys() {
				if v, ok := row.Metadata[k]; ok {
					meta[k] = v
				}
			}
			grid[idx] = binCell{snr: snr, id: row.ID, ts: row.TimestampNs, yVal: y, meta: meta}
		}
		if snr > globalSNRMax {
			globalSNRMax = snr
		}
	}

	out := make([]BinResult, 0)
	for i, cell := range grid {
		if cell.id == "" {
			continue
		}
		out = append(out, BinResult{
			T:    i / yBins,
			Y:    i % yBins,
			SNR:  cell.snr,
			ID:   cell.id,
			Ts:   cell.ts,
			YVal: cell.yVal,
			Meta: cell.meta,
		})
	}
	return out, globalSNRMax
}
