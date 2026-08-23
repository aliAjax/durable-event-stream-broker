package config

import "testing"

func TestConfigRejectsNegativeQuota(t *testing.T) {
	c := Default(); c.TenantQuota = -1
	if err := c.Validate(); err == nil { t.Fatal("negative tenant quota accepted") }
}
