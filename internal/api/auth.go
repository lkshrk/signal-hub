package api

import (
	"context"
	"net/http"
	"strings"
)

type consumerKey struct{}

func consumerFrom(ctx context.Context) string {
	name, _ := ctx.Value(consumerKey{}).(string)
	return name
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r.Header.Get("Authorization"))
		name := ""
		if ok {
			name, ok = s.Router.ByToken(token)
		}
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), consumerKey{}, name)))
	})
}

func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}
