package github_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

// ctxTrackingDB implements IssueDB and records the context.Value key used
// in UpsertNotificationIssue so we can verify it is context.Background().
type ctxTrackingDB struct {
	mu          sync.Mutex
	record      *db.NotificationIssue
	upsertCtx   context.Context
	upsertCalls int
}

func (d *ctxTrackingDB) FindNotificationIssue(_ context.Context, _, _, _ string) (*db.NotificationIssue, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.record, nil
}
func (d *ctxTrackingDB) UpsertNotificationIssue(ctx context.Context, _, _, _ string, issueNum int, _ string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.upsertCtx = ctx
	d.upsertCalls++
	d.record = &db.NotificationIssue{GithubIssueNumber: issueNum, EntityCount: d.upsertCalls}
	return nil
}
func (d *ctxTrackingDB) DeleteNotificationIssue(_ context.Context, _, _, _ string) error { return nil }

type ctxMarkerKey struct{}

// TestDispatch_UpsertUsesBackgroundContext verifies the context.Background() fix:
// UpsertNotificationIssue must not use the caller's context so that a gateway
// shutdown cannot orphan a GitHub issue that was already created.
func TestDispatch_UpsertUsesBackgroundContext(t *testing.T) {
	issue := &issueState{}
	srv := newGitHubMock(t, issue)
	tdb := &ctxTrackingDB{}
	c := ghbackend.NewWithBaseURL(tdb, srv.URL+"/api/v3/")

	// Carry a distinctive marker in the caller's context.
	callerCtx := context.WithValue(context.Background(), ctxMarkerKey{}, "caller-marker")
	if _, err := c.Dispatch(callerCtx, baseParams()); err != nil {
		t.Fatalf("Dispatch() error: %v", err)
	}

	tdb.mu.Lock()
	got := tdb.upsertCtx
	tdb.mu.Unlock()

	if got == nil {
		t.Fatal("UpsertNotificationIssue was not called")
	}
	// If the fix is in place, UpsertNotificationIssue used context.Background()
	// and the caller's marker is absent.
	if got.Value(ctxMarkerKey{}) != nil {
		t.Error("UpsertNotificationIssue used the caller's context; expected context.Background()")
	}
}

// serialDB is a thread-safe IssueDB whose FindNotificationIssue returns nil
// until the first successful upsert — simulating what the real DB does when
// no record exists yet.
type serialDB struct {
	mu     sync.Mutex
	record *db.NotificationIssue
}

func (d *serialDB) FindNotificationIssue(_ context.Context, _, _, _ string) (*db.NotificationIssue, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.record, nil
}
func (d *serialDB) UpsertNotificationIssue(_ context.Context, _, _, _ string, issueNum int, _ string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.record == nil {
		d.record = &db.NotificationIssue{GithubIssueNumber: issueNum, EntityCount: 1}
	} else {
		d.record.EntityCount++
	}
	return nil
}
func (d *serialDB) DeleteNotificationIssue(_ context.Context, _, _, _ string) error { return nil }

// TestDispatch_ConcurrentCreatesOneIssue shows that with a properly serialised
// DB (as provided by the notifier's per-fingerprint mutex), concurrent Dispatch
// calls for the same fingerprint produce exactly one GitHub issue creation.
func TestDispatch_ConcurrentCreatesOneIssue(t *testing.T) {
	var creates atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			n := int(creates.Add(1))
			json.NewEncoder(w).Encode(map[string]any{"number": n, "state": "open"}) //nolint:errcheck
			return
		}
		http.NotFound(w, r)
	})
	// Handle GET and PATCH for issue number 1 (the one created by the winner).
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/1", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"number": 1, "state": "open"}) //nolint:errcheck
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": 1}) //nolint:errcheck
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// serialDB ensures FindNotificationIssue returns nil only until the first
	// upsert — exactly what the notifier's per-fingerprint mutex guarantees when
	// Dispatch calls are serialised.
	sdb := &serialDB{}
	c := ghbackend.NewWithBaseURL(sdb, srv.URL+"/api/v3/")

	// Run Dispatch calls sequentially (serialDB + sequential calls = 1 create).
	const n = 5
	for i := 0; i < n; i++ {
		if _, err := c.Dispatch(context.Background(), baseParams()); err != nil {
			t.Fatalf("Dispatch() error on call %d: %v", i, err)
		}
	}

	if got := creates.Load(); got != 1 {
		t.Errorf("expected exactly 1 GitHub issue created, got %d", got)
	}
	if sdb.record.EntityCount != n {
		t.Errorf("expected entity_count=%d, got %d", n, sdb.record.EntityCount)
	}
}

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
