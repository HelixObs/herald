package notifier

import (
	"context"
	"time"
)

// MessagingBackend sends rate-limited notifications to a messaging platform
// (Slack, Discord, Teams, etc.).
// Implementations must be safe for concurrent use.
type MessagingBackend interface {
	// Send delivers msg to destination. Returns true if sent, false if
	// rate-limited within the current window.
	Send(ctx context.Context, destination, fingerprint, msg string, windowSecs, maxPerWindow int) (bool, error)
	// FlushDigests sends end-of-window digest messages for any expired windows.
	FlushDigests(ctx context.Context, destination string, windowSecs int)
}

// SCMBackend creates and updates issues on a source control platform
// (GitHub, GitLab, Bitbucket, etc.).
// Implementations must be safe for concurrent use.
type SCMBackend interface {
	Dispatch(ctx context.Context, p SCMParams) error
}

// SCMParams is the platform-agnostic payload for creating or updating an issue.
type SCMParams struct {
	Token         string
	Repo          string
	Labels        []string
	Title         string
	EntityID      string
	InspectorURL  string
	InspectorBase string
	EventName     string
	Message       string
	Stage         string
	Timestamp     time.Time
	Fingerprint   string
	InstrumentID  string
	OnRecurrence  string // "reopen" or "new_issue"
}
