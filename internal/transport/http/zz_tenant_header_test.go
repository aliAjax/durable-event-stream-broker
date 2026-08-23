package httptransport

import (
	"net/http/httptest"
	"testing"
)

func TestTenantHeaderTakesPrecedence(t *testing.T) {
	r := httptest.NewRequest("GET", "/?tenant_id=query", nil)
	r.Header.Set("X-Tenant-ID", "header")
	if got := tenantFrom(r); got != "header" {
		t.Fatalf("tenant=%q", got)
	}
}
