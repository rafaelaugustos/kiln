package dashboard

import (
	"mime"
	"net/http"
	"net/url"
)

const csp = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; " +
	"form-action 'self'; frame-ancestors 'none'; base-uri 'none'"

func secure(h http.Header) {
	h.Set("Content-Security-Policy", csp)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "same-origin")
	h.Set("Cache-Control", "no-store")
}

func guard(r *http.Request, a Access, api bool) int {
	if a != ReadWrite {
		return http.StatusForbidden
	}
	if !sameOrigin(r) {
		return http.StatusForbidden
	}
	if api {
		t, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || t != "application/json" {
			return http.StatusUnsupportedMediaType
		}
	}
	return 0
}

func sameOrigin(r *http.Request) bool {
	switch r.Header.Get("Sec-Fetch-Site") {
	case "", "same-origin", "none":
	default:
		return false
	}
	o := r.Header.Get("Origin")
	if o == "" {
		return true
	}
	u, err := url.Parse(o)
	return err == nil && u.Host != "" && u.Host == r.Host
}
