package notifier

import (
	"context"
	"time"
)

// Message is the platform-agnostic notification payload passed to MessagingBackend.Send.
// Each backend renders it into its own format (Slack Block Kit, Discord embeds, etc.).
type Message struct {
	Text             string            // plain-text fallback used in digest summaries
	InstrumentID     string
	EventName        string
	EntityID         string
	Body             string            // human-readable error/event message
	Stage            string
	InspectorURL     string
	ErrDashURL       string
	NotificationsURL string            // deep-link to the Notifications page for this fingerprint
	IssueURLs        []string          // issue URLs from SCM backends, e.g. "https://github.com/org/repo/issues/1"
	Metadata         map[string]string // event metadata fields (e.g. process name, custom attributes)
}

// MessagingBackend sends rate-limited notifications to a messaging platform
// (Slack, Discord, Teams, etc.).
// Implementations must be safe for concurrent use.
type MessagingBackend interface {
	// Send delivers msg to destination. Returns true if sent, false if
	// rate-limited within the current window.
	Send(ctx context.Context, destination, fingerprint string, msg Message, windowSecs, maxPerWindow int) (bool, error)
	// FlushDigests sends end-of-window digest messages for any expired windows.
	FlushDigests(ctx context.Context, destination string, windowSecs int)
}

// SCMBackend creates and updates issues on a source control platform
// (GitHub, GitLab, Bitbucket, etc.).
// Implementations must be safe for concurrent use.
type SCMBackend interface {
	// Dispatch creates or updates an issue and returns its URL.
	Dispatch(ctx context.Context, p SCMParams) (issueURL string, err error)
}

// SCMParams is the platform-agnostic payload for creating or updating an issue.
type SCMParams struct {
	Token            string
	Repo             string
	Labels           []string
	Title            string
	EntityID         string
	InspectorURL     string
	InspectorBase    string
	EventName        string
	Message          string
	Stage            string
	Timestamp        time.Time
	Fingerprint      string
	InstrumentID     string
	OnRecurrence     string // "reopen" or "new_issue"
	NotificationsURL string // deep-link to the Notifications page for this fingerprint
}
