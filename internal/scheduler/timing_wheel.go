package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type Task struct {
	At    time.Time
	ID    string
	Fn    func()
	index int
}
type queue []*Task

func (q queue) Len() int           { return len(q) }
func (q queue) Less(i, j int) bool { return q[i].At.Before(q[j].At) }
func (q queue) Swap(i, j int)      { q[i], q[j] = q[j], q[i]; q[i].index = i; q[j].index = j }
func (q *queue) Push(x any)        { t := x.(*Task); t.index = len(*q); *q = append(*q, t) }
func (q *queue) Pop() any          { old := *q; n := len(old); t := old[n-1]; *q = old[:n-1]; return t }

type Wheel struct {
	q    queue
	mu   sync.Mutex
	wake chan struct{}
	stop chan struct{}
	wg   sync.WaitGroup
}

func New() *Wheel {
	w := &Wheel{wake: make(chan struct{}, 1), stop: make(chan struct{})}
	heap.Init(&w.q)
	return w
}
func (w *Wheel) Start() { w.wg.Add(1); go w.loop() }
func (w *Wheel) loop() {
	defer w.wg.Done()
	for {
		w.mu.Lock()
		if len(w.q) == 0 {
			w.mu.Unlock()
			select {
			case <-w.wake:
			case <-w.stop:
				return
			}
			continue
		}
		d := time.Until(w.q[0].At)
		w.mu.Unlock()
		if d > 0 {
			tm := time.NewTimer(d)
			select {
			case <-tm.C:
			case <-w.wake:
				tm.Stop()
			case <-w.stop:
				tm.Stop()
				return
			}
		} else {
			w.mu.Lock()
			t := heap.Pop(&w.q).(*Task)
			w.mu.Unlock()
			if t.Fn != nil {
				t.Fn()
			}
		}
	}
}
func (w *Wheel) Schedule(t *Task) {
	w.mu.Lock()
	heap.Push(&w.q, t)
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *Wheel) Stop(ctx context.Context) error {
	close(w.stop)
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
