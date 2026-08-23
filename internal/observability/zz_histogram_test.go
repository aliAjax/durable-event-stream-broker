package observability

import (
	"sync"
	"testing"
	"time"
)

func TestHistogramConcurrentSnapshot(t *testing.T) {
	h := &Histogram{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				h.Observe(time.Millisecond)
				_ = h.Snapshot()
			}
		}()
	}
	close(start)
	wg.Wait()
	snapshot := h.Snapshot()
	if len(snapshot) == 0 {
		t.Fatal("histogram snapshot empty")
	}
	snapshot[0] = 0
	if h.Snapshot()[0] == 0 {
		t.Fatal("histogram snapshot aliases internal values")
	}
}
