// Package fingerprint normalises event messages and computes a stable dedup key.
package fingerprint

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var stripPatterns = []*regexp.Regexp{
	regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`), // UUIDs
	regexp.MustCompile(`\b[0-9a-f]{16,}\b`),                                               // long hex strings (trace/span IDs)
	regexp.MustCompile(`\b\d+\.\d+\.\d+\.\d+\b`),                                         // IP addresses
	regexp.MustCompile(`/[^\s"']+`),                                                       // file paths
	regexp.MustCompile(`\b\d+\b`),                                                         // bare integers
}

// Compute returns an 8-byte hex fingerprint for the given event fields.
// The message is normalised before hashing so that the same error class
// affecting different entities produces the same fingerprint.
func Compute(instrumentID, eventType, message, stage string) string {
	norm := Normalise(message)
	raw := strings.Join([]string{instrumentID, eventType, norm, stage}, "|")
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:8])
}

// Normalise strips volatile tokens (IDs, numbers, paths) from a message so
// that semantically identical errors from different entities hash equally.
func Normalise(message string) string {
	s := message
	for _, p := range stripPatterns {
		s = p.ReplaceAllString(s, "?")
	}
	// Collapse runs of whitespace and question marks.
	s = regexp.MustCompile(`\?(\s*\?)+`).ReplaceAllString(s, "?")
	return strings.TrimSpace(s)
}
