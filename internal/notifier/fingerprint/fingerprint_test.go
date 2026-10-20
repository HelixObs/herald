package fingerprint_test

import (
	"testing"

	"github.com/HelixObs/herald/internal/notifier/fingerprint"
)

func TestNormalise(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "strips UUID",
			input: "entity 123e4567-e89b-12d3-a456-426614174000 failed",
			want:  "entity ? failed",
		},
		{
			name:  "strips long hex",
			input: "trace id 0af7651916cd43dd8448eb211c80319c failed",
			want:  "trace id ? failed",
		},
		{
			name:  "strips IP address",
			input: "connection to 192.168.1.42 timed out",
			want:  "connection to ? timed out",
		},
		{
			name:  "strips file path",
			input: "failed to open /data/archive/frb-2024-01.hdf5",
			want:  "failed to open ?",
		},
		{
			name:  "strips bare integers",
			input: "buffer overflow at offset 4096 bytes",
			want:  "buffer overflow at offset ? bytes",
		},
		{
			name:  "collapses consecutive question marks",
			input: "error at 192.168.1.1:8080",
			want:  "error at ?:?",
		},
		{
			name:  "empty string unchanged",
			input: "",
			want:  "",
		},
		{
			name:  "plain text unchanged",
			input: "disk full on archive node",
			want:  "disk full on archive node",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := fingerprint.Normalise(tc.input)
			if got != tc.want {
				t.Errorf("Normalise(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestComputeStability(t *testing.T) {
	fp1 := fingerprint.Compute("CHIME", "helix.error", "disk full on node 3", "write-header", "hdf5_archiver")
	fp2 := fingerprint.Compute("CHIME", "helix.error", "disk full on node 7", "write-header", "hdf5_archiver")
	if fp1 != fp2 {
		t.Errorf("expected same fingerprint for same error class, got %q and %q", fp1, fp2)
	}
}

func TestComputeDifferentInstruments(t *testing.T) {
	fp1 := fingerprint.Compute("CHIME", "helix.error", "disk full", "write-header", "")
	fp2 := fingerprint.Compute("VLA", "helix.error", "disk full", "write-header", "")
	if fp1 == fp2 {
		t.Errorf("expected different fingerprints for different instruments")
	}
}

func TestComputeDifferentEvents(t *testing.T) {
	fp1 := fingerprint.Compute("CHIME", "helix.error", "disk full", "write-header", "")
	fp2 := fingerprint.Compute("CHIME", "helix.warning", "disk full", "write-header", "")
	if fp1 == fp2 {
		t.Errorf("expected different fingerprints for different event types")
	}
}

func TestComputeDifferentStages(t *testing.T) {
	fp1 := fingerprint.Compute("CHIME", "helix.error", "disk full", "write-header", "stage_a")
	fp2 := fingerprint.Compute("CHIME", "helix.error", "disk full", "write-header", "stage_b")
	if fp1 == fp2 {
		t.Errorf("expected different fingerprints for different stages")
	}
}

func TestComputeDifferentOperations(t *testing.T) {
	fp1 := fingerprint.Compute("CHIME", "helix.error", "list index out of range", "client-side-registration", "")
	fp2 := fingerprint.Compute("CHIME", "helix.error", "list index out of range", "complete-localization", "")
	if fp1 == fp2 {
		t.Errorf("expected different fingerprints for the same message from different operations")
	}
}

// Operation and stage are separate fields: moving a value from one to the
// other must not produce the same fingerprint.
func TestComputeOperationAndStageNotInterchangeable(t *testing.T) {
	fp1 := fingerprint.Compute("CHIME", "helix.error", "disk full", "write-header", "")
	fp2 := fingerprint.Compute("CHIME", "helix.error", "disk full", "", "write-header")
	if fp1 == fp2 {
		t.Errorf("expected operation and stage to hash differently")
	}
}

func TestComputeLength(t *testing.T) {
	fp := fingerprint.Compute("CHIME", "helix.error", "disk full", "write-header", "hdf5_archiver")
	if len(fp) != 16 {
		t.Errorf("expected 16-char hex fingerprint, got %d chars: %q", len(fp), fp)
	}
}
