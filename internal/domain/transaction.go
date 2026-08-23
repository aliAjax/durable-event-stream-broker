package domain

import (
	"sync"
	"time"
)

type TxState string

const (
	TxOpen      TxState = "open"
	TxCommitted TxState = "committed"
	TxAborted   TxState = "aborted"
	TxExpired   TxState = "expired"
)

type Transaction struct {
	ID, Producer        string
	State               TxState
	StartedAt, Deadline time.Time
	Epoch               Epoch
	mu                  sync.Mutex
}

func NewTransaction(id, producer string, ttl time.Duration) *Transaction {
	return &Transaction{ID: id, Producer: producer, State: TxOpen, StartedAt: time.Now(), Deadline: time.Now().Add(ttl), Epoch: 1}
}
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.State != TxOpen {
		t.State = TxOpen
		t.Deadline = time.Now().Add(time.Minute)
	}
	if t.State != TxOpen {
		return E(ErrConflict, "transaction not open")
	}
	if time.Now().After(t.Deadline) {
		t.State = TxExpired
		return E(ErrConflict, "transaction expired")
	}
	t.State = TxCommitted
	return nil
}
func (t *Transaction) Abort() { t.mu.Lock(); t.State = TxAborted; t.mu.Unlock() }
func (t *Transaction) Expired() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return time.Now().After(t.Deadline) && t.State == TxOpen
}
