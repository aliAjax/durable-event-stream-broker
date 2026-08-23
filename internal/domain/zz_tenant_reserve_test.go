package domain

import "testing"

func TestTenantRejectsNegativeBudget(t *testing.T) {
	tenant := NewTenant("t", "tenant", 100)
	if err := tenant.Reserve(-5); err == nil { t.Fatal("negative reservation accepted") }
	used, _ := tenant.Snapshot()
	if used != 0 { t.Fatalf("used bytes changed: %d", used) }
}
