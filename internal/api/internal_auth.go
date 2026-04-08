package api

import "net/http"

func RequireInternalToken(token string, next http.HandlerFunc) http.HandlerFunc {
	if token == "" {
		return next
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Token") != token {
			sendError(w, http.StatusUnauthorized, "invalid internal token")
			return
		}

		next.ServeHTTP(w, r)
	}
}
