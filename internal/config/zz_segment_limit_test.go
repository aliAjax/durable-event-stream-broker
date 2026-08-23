package config

import "testing"

func TestConfigCapsSegmentSize(t *testing.T) {
	c := Default(); c.SegmentBytes = 2 << 30
	if err := c.Validate(); err == nil { t.Fatal("oversized segment accepted") }
}
