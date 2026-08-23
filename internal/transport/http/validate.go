package httptransport

import (
	"net/http"
	"regexp"
	"strings"
)

var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func validID(s string) bool { return idPattern.MatchString(s) }
func tenantFrom(r *http.Request) string {
	v := strings.TrimSpace(r.Header.Get("X-Tenant-ID"))
	if v == "" {
		v = r.URL.Query().Get("tenant_id")
	}
	return v
}
func requestID(r *http.Request) string {
	v := r.Header.Get("X-Request-ID")
	if v == "" {
		v = "generated"
	}
	return v
}
