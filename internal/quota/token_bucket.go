package quota

import (
	"sync"
	"time"
)

type Bucket struct {
	rate, burst, tokens float64
	last                time.Time
	mu                  sync.Mutex
}

func New(rate, burst int) *Bucket {
	if rate < 0 {
		rate = 0
	}
	if burst < 0 {
		burst = 0
	}
	return &Bucket{rate: float64(rate), burst: float64(burst), tokens: float64(burst), last: time.Now()}
}
func (b *Bucket) Allow(n int) bool {
	if n < 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens < float64(n) {
		return false
	}
	b.tokens -= float64(n)
	return true
}
func (b *Bucket) RetryAfter(n int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 {
		n = 0
	}
	need := float64(n) - b.tokens
	if need <= 0 {
		return 0
	}
	if b.rate <= 0 {
		return time.Duration(1<<62)
	}
	return time.Duration(need / b.rate * float64(time.Second))
}
