package github_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/HelixObs/gateway/internal/db"
	ghbackend "github.com/HelixObs/gateway/internal/notifier/github"
	"github.com/HelixObs/gateway/internal/notifier"
)

// ── fakeDB ────────────────────────────────────────────────────────────────────

type fakeDB struct {
	record  *db.NotificationIssue
	findErr error
	upserts []upsertCall
	deletes int
}

type upsertCall struct {
	issueNum int
	entityID string
}

func (f *fakeDB) FindNotificationIssue(_ context.Context, _, _, _ string) (*db.NotificationIssue, error) {
	return f.record, f.findErr
}
func (f *fakeDB) UpsertNotificationIssue(_ context.Context, _, _, _ string, issueNum int, entityID string) error {
	f.upserts = append(f.upserts, upsertCall{issueNum, entityID})
	return nil
}
func (f *fakeDB) DeleteNotificationIssue(_ context.Context, _, _, _ string) error {
	f.deletes++
	return nil
}

// ── GitHub API mock ───────────────────────────────────────────────────────────

type issueState struct {
	number int
	state  string
	body   string
}

func newGitHubMock(t *testing.T, issue *issueState) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	// POST /repos/owner/repo/issues → create
	mux.HandleFunc("/api/v3/repos/owner/repo/issues", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		issue.number = 42
		issue.state = "open"
		json.NewEncoder(w).Encode(map[string]any{"number": issue.number, "state": "open"}) //nolint:errcheck
	})

	// GET/PATCH /repos/owner/repo/issues/42
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(map[string]any{"number": issue.number, "state": issue.state}) //nolint:errcheck
		case http.MethodPatch:
			var req map[string]any
			json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
			if state, ok := req["state"].(string); ok {
				issue.state = state
			}
			if body, ok := req["body"].(string); ok {
				issue.body = body
			}
			json.NewEncoder(w).Encode(map[string]any{"number": issue.number, "state": issue.state}) //nolint:errcheck
		default:
			http.NotFound(w, r)
		}
	})

	// POST /repos/owner/repo/issues/42/comments
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": 1}) //nolint:errcheck
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func baseParams() notifier.SCMParams {
	return notifier.SCMParams{
		Token:         "ghp_test",
		Repo:          "owner/repo",
		Labels:        []string{"bug"},
		Title:         "[CHIME] disk full",
		EntityID:      "frb-001",
		InspectorURL:  "http://ui/entity/frb-001",
		InspectorBase: "http://ui/entity",
		EventName:     "helix.error",
		Message:       "disk full on archive node",
		Stage:         "hdf5_archiver",
		Timestamp:     time.Now(),
		Fingerprint:   "deadbeef12345678",
		InstrumentID:  "CHIME",
		OnRecurrence:  "reopen",
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestDispatch_CreatesIssue(t *testing.T) {
	issue := &issueState{}
	srv := newGitHubMock(t, issue)
	fdb := &fakeDB{}
	c := ghbackend.NewWithBaseURL(fdb, srv.URL+"/api/v3/")

	issueURL, err := c.Dispatch(context.Background(), baseParams())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(issueURL, "42") {
		t.Errorf("expected issue URL to contain issue number 42, got %q", issueURL)
	}
	if issue.number != 42 {
		t.Errorf("expected issue 42 to be created, got %d", issue.number)
	}
	if len(fdb.upserts) != 1 || fdb.upserts[0].issueNum != 42 {
		t.Errorf("expected upsert with issue 42, got %+v", fdb.upserts)
	}
}

func TestDispatch_UpdatesBodyOnRecurrence(t *testing.T) {
	issue := &issueState{number: 42, state: "open"}
	srv := newGitHubMock(t, issue)
	fdb := &fakeDB{record: &db.NotificationIssue{
		GithubIssueNumber: 42,
		EntityCount:       1,
		FirstSeenAt:       time.Now().Add(-time.Hour),
	}}
	c := ghbackend.NewWithBaseURL(fdb, srv.URL+"/api/v3/")

	if _, err := c.Dispatch(context.Background(), baseParams()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if issue.body == "" {
		t.Error("expected issue body to be updated")
	}
	if !strings.Contains(issue.body, "disk full on archive node") {
		t.Errorf("expected body to contain error message, got: %s", issue.body)
	}
	if len(fdb.upserts) != 1 {
		t.Errorf("expected 1 upsert, got %d", len(fdb.upserts))
	}
}

func TestDispatch_ReopensClosedIssue(t *testing.T) {
	issue := &issueState{number: 42, state: "closed"}
	srv := newGitHubMock(t, issue)
	fdb := &fakeDB{record: &db.NotificationIssue{
		GithubIssueNumber: 42,
		EntityCount:       3,
		FirstSeenAt:       time.Now().Add(-24 * time.Hour),
	}}
	c := ghbackend.NewWithBaseURL(fdb, srv.URL+"/api/v3/")

	p := baseParams()
	p.OnRecurrence = "reopen"
	if _, err := c.Dispatch(context.Background(), p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if issue.state != "open" {
		t.Errorf("expected issue to be reopened, state=%q", issue.state)
	}
}

func TestDispatch_NewIssueOnClosedWhenConfigured(t *testing.T) {
	issue := &issueState{number: 42, state: "closed"}
	srv := newGitHubMock(t, issue)
	fdb := &fakeDB{record: &db.NotificationIssue{
		GithubIssueNumber: 42,
		EntityCount:       1,
		FirstSeenAt:       time.Now().Add(-time.Hour),
	}}
	c := ghbackend.NewWithBaseURL(fdb, srv.URL+"/api/v3/")

	p := baseParams()
	p.OnRecurrence = "new_issue"
	if _, err := c.Dispatch(context.Background(), p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fdb.deletes != 1 {
		t.Errorf("expected old record to be deleted before creating new issue, got %d deletes", fdb.deletes)
	}
}

func TestDispatch_InvalidRepoFormat(t *testing.T) {
	c := ghbackend.New(&fakeDB{})
	p := baseParams()
	p.Repo = "noslash"
	if _, err := c.Dispatch(context.Background(), p); err == nil {
		t.Error("expected error for invalid repo format")
	}
}

// ── buildBody tests ───────────────────────────────────────────────────────────

func TestBuildBody_ContainsSummary(t *testing.T) {
	// We test buildBody indirectly via a full Dispatch that creates an issue.
	issue := &issueState{}
	srv := newGitHubMock(t, issue)
	fdb := &fakeDB{}
	c := ghbackend.NewWithBaseURL(fdb, srv.URL+"/api/v3/")

	c.Dispatch(context.Background(), baseParams()) //nolint:errcheck

	// Subsequent call to check body via update path.
	issue2 := &issueState{number: 42, state: "open"}
	srv2 := newGitHubMock(t, issue2)
	fdb2 := &fakeDB{record: &db.NotificationIssue{
		GithubIssueNumber: 42,
		EntityCount:       1,
		FirstSeenAt:       time.Now().Add(-time.Minute),
		RecentEntityIDs:   []string{"frb-000"},
	}}
	c2 := ghbackend.NewWithBaseURL(fdb2, srv2.URL+"/api/v3/")
	c2.Dispatch(context.Background(), baseParams()) //nolint:errcheck

	if !strings.Contains(issue2.body, "## Error summary") {
		t.Error("expected body to contain Error summary section")
	}
	if !strings.Contains(issue2.body, "## Occurrence statistics") {
		t.Error("expected body to contain Occurrence statistics section")
	}
	if !strings.Contains(issue2.body, "## Recent affected entities") {
		t.Error("expected body to contain Recent affected entities section")
	}
	if !strings.Contains(issue2.body, "frb-001") {
		t.Error("expected body to contain current entity ID")
	}
	if !strings.Contains(issue2.body, "frb-000") {
		t.Error("expected body to contain previous entity ID")
	}
}
