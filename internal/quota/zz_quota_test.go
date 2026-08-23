package quota

import (
	"sync"
	"testing"
)

func TestQuotaAndMetricsConcurrentAccess(t *testing.T) {
	b := New(100, 100)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 2; i++ { wg.Add(1); go func() { defer wg.Done(); <-start; for j := 0; j < 100; j++ { b.Allow(1); _ = b.RetryAfter(2) } }() }
	close(start)
	wg.Wait()
	if _, last := b.Snapshot(); last.IsZero() { t.Fatal("bucket snapshot lost timestamp") }
}
