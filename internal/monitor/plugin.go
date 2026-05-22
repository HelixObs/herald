// Package monitor provides the binning logic for the entity monitor API (/api/v1/monitor/bins).
package monitor

import (
	"regexp"
	"strconv"

	"github.com/HelixObs/herald/internal/db"
)

// BinResult is one non-empty cell in the binned output grid.
type BinResult struct {
	T      int               `json:"t"`      // x bin index
	Y      int               `json:"y"`      // y bin index
	Weight float64           `json:"weight"` // max weight in this cell
	ID     string            `json:"id"`     // entity ID of the best candidate
	Ts     int64             `json:"ts"`     // timestamp_ns of the best candidate
	YVal   float64           `json:"yv"`     // raw y value of the best candidate
	Meta   map[string]string `json:"meta"`   // y_field and weight_field values for the best candidate
}

// BinsResponse is the full response body for GET /api/v1/monitor/bins.
type BinsResponse struct {
	Bins      []BinResult `json:"bins"`
	TBins     int         `json:"t_bins"`
	YBins     int         `json:"y_bins"`
	FromNs    int64       `json:"from_ns"`
	ToNs      int64       `json:"to_ns"`
	YMin      float64     `json:"y_min"`
	YMax      float64     `json:"y_max"`
	WeightMax float64     `json:"weight_max"`
}

// MinWindowNs is the minimum query window (5 minutes).
const MinWindowNs int64 = 5 * 60 * 1_000_000_000

// MaxWindowNs is the maximum query window (24 hours).
const MaxWindowNs int64 = 24 * 60 * 60 * 1_000_000_000

var fieldNameRE = regexp.MustCompile(`^[a-z0-9_.]+$`)

// ValidateFieldName returns true if s is a safe metadata key (lowercase alphanum, dots, underscores).
func ValidateFieldName(s string) bool {
	return fieldNameRE.MatchString(s)
}

// binCell tracks the best candidate seen in one grid cell.
type binCell struct {
	weight float64
	id     string
	ts     int64
	yVal   float64
	meta   map[string]string
}

// Bin bins rows into a tBins×yBins grid.
//
// yField is the metadata key whose float value determines the row's y position.
// weightField is optional — when non-empty its float value is used as the brightness
// weight for the Inferno colormap; when empty every row contributes weight 1.
// yMinDefault/yMaxDefault are the fallback y-axis bounds used when the overrides are
// out of range (yMinOverride < 0 or yMaxOverride <= yMin are ignored).
func Bin(rows []db.RawEntityRow, yField, weightField string, yMinDefault, yMaxDefault float64,
	fromNs, toNs int64, tBins, yBins int, yMinOverride, yMaxOverride float64) ([]BinResult, float64) {

	yMin := yMinDefault
	yMax := yMaxDefault
	if yMaxOverride > yMin {
		yMax = yMaxOverride
	}
	if yMinOverride >= 0 && yMinOverride < yMax {
		yMin = yMinOverride
	}

	tRange := float64(toNs - fromNs)
	yRange := yMax - yMin
	grid := make([]binCell, tBins*yBins)
	var globalWeightMax float64

	for _, row := range rows {
		yStr, ok := row.Metadata[yField]
		if !ok {
			continue
		}
		y, err := strconv.ParseFloat(yStr, 64)
		if err != nil || y < yMin || y > yMax {
			continue
		}

		weight := 1.0
		if weightField != "" {
			if wStr, wok := row.Metadata[weightField]; wok {
				if w, werr := strconv.ParseFloat(wStr, 64); werr == nil {
					weight = w
				}
			}
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
		if weight > grid[idx].weight {
			// Include all helix.* attributes so the hover tooltip is fully informative.
			meta := make(map[string]string, len(row.Metadata))
			for k, v := range row.Metadata {
				meta[k] = v
			}
			grid[idx] = binCell{weight: weight, id: row.ID, ts: row.TimestampNs, yVal: y, meta: meta}
		}
		if weight > globalWeightMax {
			globalWeightMax = weight
		}
	}

	out := make([]BinResult, 0)
	for i, cell := range grid {
		if cell.id == "" {
			continue
		}
		out = append(out, BinResult{
			T:      i / yBins,
			Y:      i % yBins,
			Weight: cell.weight,
			ID:     cell.id,
			Ts:     cell.ts,
			YVal:   cell.yVal,
			Meta:   cell.meta,
		})
	}
	return out, globalWeightMax
}
