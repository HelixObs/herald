package auth

import (
	"context"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// BearerAuth wraps an HTTP handler requiring a valid HelixObs JWT on every request.
// When the issuer has no keys configured (empty JWT_SECRET), all requests pass through
// so dev mode and safe rollout work without changes to clients.
func BearerAuth(issuer *Issuer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !issuer.Enabled() {
			next.ServeHTTP(w, r)
			return
		}
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "authorization required", http.StatusUnauthorized)
			return
		}
		if _, err := issuer.Validate(token); err != nil {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// UnaryInterceptor returns a gRPC unary server interceptor that validates Bearer tokens.
// When the issuer has no keys configured, all requests pass through (dev mode).
func UnaryInterceptor(issuer *Issuer) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if !issuer.Enabled() {
			return handler(ctx, req)
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		values := md.Get("authorization")
		if len(values) == 0 {
			return nil, status.Error(codes.Unauthenticated, "authorization required")
		}
		token, ok := bearerToken(values[0])
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "bearer token required")
		}
		if _, err := issuer.Validate(token); err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(ctx, req)
	}
}

func bearerToken(authHeader string) (string, bool) {
	token, ok := strings.CutPrefix(authHeader, "Bearer ")
	return token, ok && token != ""
}
