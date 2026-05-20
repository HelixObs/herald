package auth_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/HelixObs/gateway/internal/auth"
)

func newTestConfigStore(t *testing.T, yaml string) *auth.ConfigStore {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/inst.yml", []byte(yaml), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	return auth.NewConfigStore(dir)
}

func TestTokenHandler_IssuesJWT(t *testing.T) {
	cs := newTestConfigStore(t, `
instrument_id: TESTINST
auth:
  type: secret
  api_key_hash: "`+hashOf("correct-secret")+`"
`)
	issuer := auth.NewIssuer([]string{"jwt-signing-key"})
	h := auth.NewTokenHandler(cs, issuer)

	body, _ := json.Marshal(map[string]string{
		"instrument_id": "TESTINST",
		"credential":    "correct-secret",
	})
	req := httptest.NewRequest(http.MethodPost, "/auth/token", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Token     string `json:"token"`
		ExpiresIn int    `json:"expires_in"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Token == "" {
		t.Error("expected non-empty token in response")
	}
	if resp.ExpiresIn != 86400 {
		t.Errorf("expected expires_in=86400, got %d", resp.ExpiresIn)
	}
	// Token should be verifiable with the same issuer.
	subject, err := issuer.Validate(resp.Token)
	if err != nil {
		t.Fatalf("token not valid: %v", err)
	}
	if subject != "TESTINST" {
		t.Errorf("expected subject=TESTINST, got %q", subject)
	}
}

func TestTokenHandler_RejectsInvalidCredential(t *testing.T) {
	cs := newTestConfigStore(t, `
instrument_id: TESTINST
auth:
  type: secret
  api_key_hash: "`+hashOf("correct-secret")+`"
`)
	h := auth.NewTokenHandler(cs, auth.NewIssuer([]string{"key"}))

	body, _ := json.Marshal(map[string]string{
		"instrument_id": "TESTINST",
		"credential":    "wrong-secret",
	})
	req := httptest.NewRequest(http.MethodPost, "/auth/token", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestTokenHandler_RejectsUnknownInstrument(t *testing.T) {
	cs := newTestConfigStore(t, `instrument_id: OTHER`)
	h := auth.NewTokenHandler(cs, auth.NewIssuer([]string{"key"}))

	body, _ := json.Marshal(map[string]string{
		"instrument_id": "UNKNOWN",
		"credential":    "secret",
	})
	req := httptest.NewRequest(http.MethodPost, "/auth/token", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestTokenHandler_RejectsWrongMethod(t *testing.T) {
	cs := newTestConfigStore(t, `instrument_id: X`)
	h := auth.NewTokenHandler(cs, auth.NewIssuer([]string{"key"}))
	req := httptest.NewRequest(http.MethodGet, "/auth/token", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestTokenHandler_RejectsBadJSON(t *testing.T) {
	cs := newTestConfigStore(t, `instrument_id: X`)
	h := auth.NewTokenHandler(cs, auth.NewIssuer([]string{"key"}))
	req := httptest.NewRequest(http.MethodPost, "/auth/token", bytes.NewReader([]byte("not-json")))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestTokenHandler_RejectsMissingFields(t *testing.T) {
	cs := newTestConfigStore(t, `instrument_id: X`)
	h := auth.NewTokenHandler(cs, auth.NewIssuer([]string{"key"}))

	body, _ := json.Marshal(map[string]string{"instrument_id": "X"}) // missing credential
	req := httptest.NewRequest(http.MethodPost, "/auth/token", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}
