package domain

import "testing"

func TestTenantIgnoresNegativeRelease(t *testing.T) {
	tenant := NewTenant("t", "tenant", 100)
	if err := tenant.Reserve(10); err != nil {
		t.Fatal(err)
	}
	tenant.Release(-5)
	used, _ := tenant.Snapshot()
	if used != 10 {
		t.Fatalf("negative release changed used bytes: %d", used)
	}
}
