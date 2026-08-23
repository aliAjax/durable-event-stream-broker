package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type Offset int64
type Epoch int64
type RetentionPolicy struct {
	MaxBytes int64
	MaxAge   time.Duration
}
type Record struct {
	Offset      Offset            `json:"offset"`
	Key         string            `json:"key"`
	Value       []byte            `json:"value"`
	Headers     map[string]string `json:"headers,omitempty"`
	Timestamp   time.Time         `json:"timestamp"`
	ProducerID  string            `json:"producer_id,omitempty"`
	Sequence    int64             `json:"sequence,omitempty"`
	Transaction string            `json:"transaction,omitempty"`
	Committed   bool              `json:"committed"`
}
type Partition struct {
	ID        int
	Epoch     Epoch
	Records   []Record
	Next      Offset
	Retention RetentionPolicy
	mu        sync.RWMutex
}

func NewPartition(id int, retention RetentionPolicy) *Partition {
	return &Partition{ID: id, Retention: retention}
}
func (p *Partition) Append(records []Record, producer string, firstSeq int64) ([]Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(records) == 0 {
		return nil, E(ErrInvalid, "empty batch")
	}
	for i := range records {
		if len(records[i].Value) > 1<<20 {
			return nil, E(ErrInvalid, "message too large")
		}
		records[i].Offset = p.Next
		records[i].Timestamp = time.Now().UTC()
		records[i].ProducerID = producer
		records[i].Sequence = firstSeq + int64(i)
		records[i].Committed = true
		p.Records = append(p.Records, records[i])
		p.Next++
	}
	return records, nil
}
func (p *Partition) Fetch(after Offset, limit int) []Record {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]Record, 0, limit)
	for _, r := range p.Records {
		if r.Offset > after {
			out = append(out, r)
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}
func (p *Partition) Lag(committed Offset) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int64(p.Next - committed - 1)
}
func (p *Partition) Compact() {
	p.mu.Lock()
	defer p.mu.Unlock()
	cutoff := time.Now().Add(-p.Retention.MaxAge)
	kept := p.Records[:0]
	for _, r := range p.Records {
		if p.Retention.MaxAge == 0 || r.Timestamp.After(cutoff) {
			kept = append(kept, r)
		}
	}
	p.Records = kept
}
func (p *Partition) Checksum() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	h := sha256.New()
	for _, r := range p.Records {
		fmt.Fprintf(h, "%d:%s:%x", r.Offset, r.Key, r.Value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type Topic struct {
	ID, Name, TenantID string
	Partitions         map[int]*Partition
	CreatedAt          time.Time
	mu                 sync.RWMutex
}

func NewTopic(id, tenant, name string, count int) *Topic {
	t := &Topic{ID: id, Name: name, TenantID: tenant, Partitions: map[int]*Partition{}, CreatedAt: time.Now().UTC()}
	for i := 0; i < count; i++ {
		t.Partitions[i] = NewPartition(i, RetentionPolicy{})
	}
	return t
}
func (t *Topic) Partition(id int) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	p, ok := t.Partitions[id]
	if !ok {
		return nil, E(ErrNotFound, "partition")
	}
	return p, nil
}
