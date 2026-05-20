// Package github implements notifier.SCMBackend for GitHub Issues.
package github

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	gh "github.com/google/go-github/v62/github"
	"golang.org/x/oauth2"

	"github.com/HelixObs/gateway/internal/db"
	"github.com/HelixObs/gateway/internal/notifier"
)

// IssueDB is the subset of db.Store used by this backend.
type IssueDB interface {
	FindNotificationIssue(ctx context.Context, instrumentID, fingerprint, repo string) (*db.NotificationIssue, error)
	UpsertNotificationIssue(ctx context.Context, instrumentID, fingerprint, repo string, issueNum int, entityID string) error
	DeleteNotificationIssue(ctx context.Context, instrumentID, fingerprint, repo string) error
}

// Client implements notifier.SCMBackend for GitHub.
type Client struct {
	db      IssueDB
	apiBase string // non-empty overrides GitHub API base URL (for testing)
}

// New creates a GitHub Client.
func New(db IssueDB) *Client {
	return &Client{db: db}
}

// NewWithBaseURL creates a GitHub Client with a custom API base URL.
// Intended for testing only — pass the httptest server URL.
func NewWithBaseURL(db IssueDB, apiBase string) *Client {
	return &Client{db: db, apiBase: apiBase}
}

// Dispatch creates a new GitHub issue or updates the body of an existing one.
// Returns the URL of the created or updated issue.
// On recurrence after a closed issue: reopens with a single state-change comment,
// then updates the body. No other comments are ever added.
func (c *Client) Dispatch(ctx context.Context, p notifier.SCMParams) (string, error) {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: p.Token})
	tc := oauth2.NewClient(ctx, ts)
	client := gh.NewClient(tc)
	if c.apiBase != "" {
		if u, err := url.Parse(c.apiBase); err == nil {
			client.BaseURL = u
		}
	}

	parts := strings.SplitN(p.Repo, "/", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid repo format: %s", p.Repo)
	}
	owner, repo := parts[0], parts[1]

	record, err := c.db.FindNotificationIssue(ctx, p.InstrumentID, p.Fingerprint, p.Repo)
	if err != nil {
		return "", fmt.Errorf("look up existing issue: %w", err)
	}

	if record == nil {
		return c.createIssue(ctx, client, owner, repo, p)
	}
	return c.updateIssue(ctx, client, owner, repo, record, p)
}

func (c *Client) createIssue(ctx context.Context, client *gh.Client, owner, repo string, p notifier.SCMParams) (string, error) {
	now := time.Now()
	labels := p.Labels
	req := &gh.IssueRequest{
		Title:  gh.String(p.Title),
		Body:   gh.String(buildBody(p, 1, now, now, nil)),
		Labels: &labels,
	}
	issue, _, err := c.retry(func() (*gh.Issue, *gh.Response, error) {
		return client.Issues.Create(ctx, owner, repo, req)
	})
	if err != nil {
		return "", fmt.Errorf("create issue: %w", err)
	}
	slog.Info("github: created issue", "repo", p.Repo, "number", issue.GetNumber(), "fingerprint", p.Fingerprint)
	issueURL := fmt.Sprintf("https://github.com/%s/%s/issues/%d", owner, repo, issue.GetNumber())
	// Use context.Background() so a gateway shutdown can't cancel this write after the
	// GitHub issue already exists — an orphaned issue with no DB record causes duplicates.
	return issueURL, c.db.UpsertNotificationIssue(context.Background(), p.InstrumentID, p.Fingerprint, p.Repo, issue.GetNumber(), p.EntityID)
}

func (c *Client) updateIssue(ctx context.Context, client *gh.Client, owner, repo string,
	record *db.NotificationIssue, p notifier.SCMParams) (string, error) {

	issue, resp, err := c.retry(func() (*gh.Issue, *gh.Response, error) {
		return client.Issues.Get(ctx, owner, repo, record.GithubIssueNumber)
	})
	if err != nil {
		if resp != nil && resp.StatusCode == 404 {
			slog.Warn("github: issue not found, recreating", "repo", p.Repo, "number", record.GithubIssueNumber)
			if delErr := c.db.DeleteNotificationIssue(ctx, p.InstrumentID, p.Fingerprint, p.Repo); delErr != nil {
				slog.Warn("github: failed to delete stale issue record", "error", delErr)
			}
			return c.createIssue(ctx, client, owner, repo, p)
		}
		return "", fmt.Errorf("get issue: %w", err)
	}

	now := time.Now()
	newCount := record.EntityCount + 1

	if issue.GetState() == "closed" {
		if p.OnRecurrence == "new_issue" {
			slog.Info("github: issue closed, creating new one per config", "repo", p.Repo, "old", record.GithubIssueNumber)
			if delErr := c.db.DeleteNotificationIssue(ctx, p.InstrumentID, p.Fingerprint, p.Repo); delErr != nil {
				slog.Warn("github: failed to delete closed issue record", "error", delErr)
			}
			return c.createIssue(ctx, client, owner, repo, p)
		}
		open := "open"
		if _, _, err := c.retry(func() (*gh.Issue, *gh.Response, error) {
			return client.Issues.Edit(ctx, owner, repo, record.GithubIssueNumber, &gh.IssueRequest{State: &open})
		}); err != nil {
			return "", fmt.Errorf("reopen issue: %w", err)
		}
		ts := now.UTC().Format("2006-01-02 15:04:05 UTC")
		reopenComment := fmt.Sprintf(
			"**Recurred after close** (%s)\n| Entity | Inspector |\n|--------|----------|\n| `%s` | [Inspect](%s) |",
			ts, p.EntityID, p.InspectorURL,
		)
		if _, _, err := c.retryComment(func() (*gh.IssueComment, *gh.Response, error) {
			return client.Issues.CreateComment(ctx, owner, repo, record.GithubIssueNumber, &gh.IssueComment{Body: gh.String(reopenComment)})
		}); err != nil {
			slog.Warn("github: reopen comment failed", "repo", p.Repo, "number", record.GithubIssueNumber, "error", err)
		}
	}

	updatedBody := buildBody(p, newCount, record.FirstSeenAt, now, record.RecentEntityIDs)
	if _, _, err := c.retry(func() (*gh.Issue, *gh.Response, error) {
		return client.Issues.Edit(ctx, owner, repo, record.GithubIssueNumber, &gh.IssueRequest{Body: gh.String(updatedBody)})
	}); err != nil {
		return "", fmt.Errorf("update issue body: %w", err)
	}

	slog.Info("github: updated issue body", "repo", p.Repo, "number", record.GithubIssueNumber, "occurrences", newCount)
	issueURL := fmt.Sprintf("https://github.com/%s/%s/issues/%d", owner, repo, record.GithubIssueNumber)
	return issueURL, c.db.UpsertNotificationIssue(ctx, p.InstrumentID, p.Fingerprint, p.Repo, record.GithubIssueNumber, p.EntityID)
}

// buildBody renders the issue body with a running summary and up to 10 recent entities.
func buildBody(p notifier.SCMParams, occurrences int, firstSeen, lastSeen time.Time, prevEntityIDs []string) string {
	fs := firstSeen.UTC().Format("2006-01-02 15:04:05 UTC")
	ls := lastSeen.UTC().Format("2006-01-02 15:04:05 UTC")

	var sb strings.Builder
	fmt.Fprintf(&sb, "## Error summary\n")
	fmt.Fprintf(&sb, "**Message:** %s\n", p.Message)
	if p.Stage != "" {
		fmt.Fprintf(&sb, "**Stage:** %s\n", p.Stage)
	}
	fmt.Fprintf(&sb, "**Instrument:** %s\n**Event type:** %s\n\n", p.InstrumentID, p.EventName)

	fmt.Fprintf(&sb, "## Occurrence statistics\n| | |\n|---|---|\n")
	fmt.Fprintf(&sb, "| **First seen** | %s |\n", fs)
	fmt.Fprintf(&sb, "| **Last seen** | %s |\n", ls)
	fmt.Fprintf(&sb, "| **Total occurrences** | %d |\n\n", occurrences)

	fmt.Fprintf(&sb, "## Recent affected entities\n")
	fmt.Fprintf(&sb, "| Entity ID | Inspector |\n|-----------|----------|\n")
	fmt.Fprintf(&sb, "| `%s` | [Inspect](%s) |\n", p.EntityID, p.InspectorURL)
	shown := 1
	for _, id := range prevEntityIDs {
		if shown >= 10 || id == p.EntityID {
			continue
		}
		fmt.Fprintf(&sb, "| `%s` | [Inspect](%s/%s) |\n", id, p.InspectorBase, id)
		shown++
	}

	fmt.Fprintf(&sb, "\n---\n*Tracked automatically by HelixObs. Do not close manually — use the silence API to suppress.*")
	return sb.String()
}

func (c *Client) retry(fn func() (*gh.Issue, *gh.Response, error)) (*gh.Issue, *gh.Response, error) {
	var issue *gh.Issue
	var resp *gh.Response
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
		}
		issue, resp, err = fn()
		if err == nil {
			return issue, resp, nil
		}
		if resp != nil && resp.StatusCode < 500 {
			break
		}
	}
	return issue, resp, err
}

func (c *Client) retryComment(fn func() (*gh.IssueComment, *gh.Response, error)) (*gh.IssueComment, *gh.Response, error) {
	var comment *gh.IssueComment
	var resp *gh.Response
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
		}
		comment, resp, err = fn()
		if err == nil {
			return comment, resp, nil
		}
		if resp != nil && resp.StatusCode < 500 {
			break
		}
	}
	return comment, resp, err
}
