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

// chimeBeamPlot renders beam number vs time with SNR brightness.
type chimeBeamPlot struct{}

func (c *chimeBeamPlot) Config() PlotConfig {
	return PlotConfig{
		Name:  "chime_beam_time",
		Label: "Beam no",
		YMin:  0,
		YMax:  1023,
		YUnit: "beam",
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
	return float64(beamNo), snrVal, true
}
