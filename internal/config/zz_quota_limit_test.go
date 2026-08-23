package config

import "testing"

func TestConfigCapsQuotaSize(t *testing.T) {
	c := Default()
	c.TenantQuota = 1<<40 + 1
	if err := c.Validate(); err == nil {
		t.Fatal("oversized tenant quota accepted")
	}
}
