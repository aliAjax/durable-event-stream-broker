package domain

import "sync"

const MaxQuotaBytes int64 = 1 << 40

type Tenant struct {
	ID, Name              string
	QuotaBytes, UsedBytes int64
	RatePerSecond         int
	Connections           int
	mu                    sync.Mutex
}

func NewTenant(id, name string, quota int64) *Tenant {
	return &Tenant{ID: id, Name: name, QuotaBytes: quota, RatePerSecond: 1000}
}
func (t *Tenant) Reserve(n int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.QuotaBytes > 0 && t.UsedBytes+n > t.QuotaBytes {
		return E(ErrQuota, "tenant byte quota exceeded")
	}
	t.UsedBytes += n
	return nil
}
func (t *Tenant) Release(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.UsedBytes -= n
	if t.UsedBytes < 0 {
		t.UsedBytes = 0
	}
}
func (t *Tenant) Snapshot() (int64, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.UsedBytes, t.QuotaBytes
}
