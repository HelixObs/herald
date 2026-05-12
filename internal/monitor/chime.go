package monitor

import (
	"strconv"

	"github.com/HelixObs/gateway/internal/db"
)

func init() {
	Register(&chimeDMPlot{})
	Register(&chimeBeamPlot{})
}

// chimeDMPlot renders DM (pc/cm³) vs time with SNR as pixel brightness.
type chimeDMPlot struct{}

func (c *chimeDMPlot) Config() PlotConfig {
	return PlotConfig{
		Name:  "chime_dm_time",
		Label: "DM (pc/cm³)",
		YMin:  0,
		YMax:  3000,
		YUnit: "pc/cm³",
	}
}

func (c *chimeDMPlot) RequiredKeys() []string {
	return []string{"helix.chime.dm", "helix.chime.snr"}
}

func (c *chimeDMPlot) Extract(e db.RawEntityRow) (y, snr float64, ok bool) {
	dmStr, hasDM := e.Metadata["helix.chime.dm"]
	snrStr, hasSNR := e.Metadata["helix.chime.snr"]
	if !hasDM || !hasSNR {
		return 0, 0, false
	}
	dm, err := strconv.ParseFloat(dmStr, 64)
	if err != nil {
		return 0, 0, false
	}
	snrVal, err := strconv.ParseFloat(snrStr, 64)
	if err != nil {
		return 0, 0, false
	}
	return dm, snrVal, true
}

// chimeBeamPlot renders beam row (beam_no % 1000) vs time with SNR brightness.
// CHIME beam numbers are in bands 0-255, 1000-1255, 2000-2255, 3000-3255;
// modulo 1000 maps all bands onto the 0-255 row axis.
type chimeBeamPlot struct{}

func (c *chimeBeamPlot) Config() PlotConfig {
	return PlotConfig{
		Name:  "chime_beam_time",
		Label: "Beam row (beam_no % 1000)",
		YMin:  0,
		YMax:  255,
		YUnit: "beam row",
	}
}

func (c *chimeBeamPlot) RequiredKeys() []string {
	return []string{"helix.chime.beam_no", "helix.chime.snr"}
}

func (c *chimeBeamPlot) Extract(e db.RawEntityRow) (y, snr float64, ok bool) {
	beamStr, hasBeam := e.Metadata["helix.chime.beam_no"]
	snrStr, hasSNR := e.Metadata["helix.chime.snr"]
	if !hasBeam || !hasSNR {
		return 0, 0, false
	}
	beamNo, err := strconv.ParseInt(beamStr, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	snrVal, err := strconv.ParseFloat(snrStr, 64)
	if err != nil {
		return 0, 0, false
	}
	return float64(beamNo % 1000), snrVal, true
}
