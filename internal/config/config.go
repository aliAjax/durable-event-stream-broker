package config

import (
	"os"

	"github.com/example/persistent-event-stream-broker/internal/domain"
)

const MaxSegmentBytes int64 = 1 << 30

var MaxTenantQuota = domain.MaxQuotaBytes

type Config struct {
	HTTPAddr, DataDir string
	SegmentBytes      int64
	TenantQuota       int64
}

func Default() Config {
	addr := os.Getenv("BROKER_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	dir := os.Getenv("BROKER_DATA_DIR")
	if dir == "" {
		dir = "./data"
	}
	return Config{HTTPAddr: addr, DataDir: dir, SegmentBytes: 4 << 20, TenantQuota: 64 << 20}
}

func (c Config) Validate() error {
	if c.HTTPAddr == "" || c.DataDir == "" {
		return ErrInvalid
	}
	if c.SegmentBytes < 1024 || c.SegmentBytes > MaxSegmentBytes {
		return ErrInvalid
	}
	if c.TenantQuota < 0 || c.TenantQuota > MaxTenantQuota {
		return ErrInvalid
	}
	return nil
}

var ErrInvalid = &configError{"invalid configuration"}

type configError struct{ s string }

func (e *configError) Error() string { return e.s }
