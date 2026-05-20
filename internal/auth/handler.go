package auth

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// TokenHandler serves POST /auth/token.
// It validates the caller's credential against the instrument's configured backend,
// then issues a short-lived HelixObs JWT.
type TokenHandler struct {
	configs *ConfigStore
	issuer  *Issuer
}

func NewTokenHandler(configs *ConfigStore, issuer *Issuer) *TokenHandler {
	return &TokenHandler{configs: configs, issuer: issuer}
}

func (h *TokenHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		InstrumentID string `json:"instrument_id"`
		Credential   string `json:"credential"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.InstrumentID == "" || req.Credential == "" {
		http.Error(w, "instrument_id and credential are required", http.StatusBadRequest)
		return
	}

	authCfg, ok := h.configs.Get(req.InstrumentID)
	if !ok || authCfg.Type == "" {
		slog.Warn("auth/token: no auth configured", "instrument_id", req.InstrumentID)
		http.Error(w, "auth not configured for instrument", http.StatusUnauthorized)
		return
	}

	backend, err := BackendFor(authCfg)
	if err != nil {
		slog.Error("auth/token: build backend failed", "instrument_id", req.InstrumentID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := backend.Authenticate(r.Context(), req.Credential); err != nil {
		// Don't log the credential, just the instrument and error type.
		slog.Warn("auth/token: credential rejected", "instrument_id", req.InstrumentID)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	token, err := h.issuer.Issue(req.InstrumentID)
	if err != nil {
		slog.Error("auth/token: issue failed", "instrument_id", req.InstrumentID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct { //nolint:errcheck
		Token     string `json:"token"`
		ExpiresIn int    `json:"expires_in"`
	}{
		Token:     token,
		ExpiresIn: int((24 * time.Hour).Seconds()),
	})
}
