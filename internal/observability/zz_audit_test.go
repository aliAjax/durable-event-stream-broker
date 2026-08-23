package observability

import (
	"sync"
	"testing"
)

func TestAuditSnapshotCopy(t *testing.T) {
	c := &Chain{}
	start := make(chan struct{}); var wg sync.WaitGroup; wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done(); <-start
			fields := map[string]string{"source": "original"}
			e := c.Append("append", "actor", "resource", fields)
			e.Fields["source"] = "changed"
		}(i)
	}
	close(start); wg.Wait()
	if c.events[0].Fields["source"] != "original" { t.Fatal("audit fields alias caller map") }
	if !c.Verify() { t.Fatal("audit chain invalid") }
}
