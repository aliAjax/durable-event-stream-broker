package observability

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct{ n atomic.Int64 }

func (c *Counter) Inc()        { c.n.Add(1) }
func (c *Counter) Add(n int64) { c.n.Add(n) }
func (c *Counter) Get() int64  { return c.n.Load() }

type Histogram struct {
	mu     sync.Mutex
	values []time.Duration
}

func (h *Histogram) Observe(v time.Duration) {
	h.mu.Lock()
	h.values = append(h.values, v)
	h.mu.Unlock()
}
func (h *Histogram) Count() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.values) }

type Registry struct {
	Appends, Fetches, Failures Counter
	Latency                    Histogram
}

func (r *Registry) Prometheus() string {
	return fmt.Sprintf("broker_appends_total %d\nbroker_fetches_total %d\nbroker_failures_total %d\n", r.Appends.Get(), r.Fetches.Get(), r.Failures.Get())
}
