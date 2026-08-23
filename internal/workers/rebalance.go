package workers

import (
	"context"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
	"sync"
	"time"
)

type RebalanceWorker struct {
	Repo *repository.Repository
	mu   sync.Mutex
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func (w *RebalanceWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.stop != nil { w.mu.Unlock(); return }
	w.stop, w.done = make(chan struct{}), make(chan struct{})
	w.mu.Unlock()
	go func() {
		t := time.NewTicker(5 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				w.Run()
			case <-w.stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (w *RebalanceWorker) Stop(ctx context.Context) error {
	w.mu.Lock()
	stop, done := w.stop, w.done
	w.mu.Unlock()
	if stop == nil { return nil }
	w.once.Do(func() { close(stop) })
	select { case <-done: return nil; case <-ctx.Done(): return ctx.Err() }
}

func (w *RebalanceWorker) Run() {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	for _, g := range w.Repo.Groups {
		g.Expire(now)
	}
}
func Assign(g *domain.ConsumerGroup, partitions []int) { g.Assign(partitions) }
