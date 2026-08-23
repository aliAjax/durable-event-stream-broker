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

func (r Record) Clone() Record {
	c := r
	if len(r.Value) > 0 {
		c.Value = make([]byte, len(r.Value))
		copy(c.Value, r.Value)
	}
	if len(r.Headers) > 0 {
		c.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			c.Headers[k] = v
		}
	}
	return c
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
func (p *Partition) Clone() *Partition {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c := &Partition{
		ID:        p.ID,
		Epoch:     p.Epoch,
		Next:      p.Next,
		Retention: p.Retention,
		Records:   make([]Record, len(p.Records)),
	}
	for i, r := range p.Records {
		c.Records[i] = r.Clone()
	}
	return c
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
	}
	stored := make([]Record, len(records))
	out := make([]Record, len(records))
	for i := range records {
		rec := records[i].Clone()
		rec.Offset = p.Next
		rec.Timestamp = time.Now().UTC()
		rec.ProducerID = producer
		rec.Sequence = firstSeq + int64(i)
		rec.Committed = true
		stored[i] = rec
		out[i] = rec.Clone()
		p.Records = append(p.Records, stored[i])
		p.Next++
	}
	return out, nil
}
func (p *Partition) Fetch(after Offset, limit int) []Record {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]Record, 0, limit)
	for _, r := range p.Records {
		if r.Offset > after {
			out = append(out, r.Clone())
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
func (t *Topic) Clone() *Topic {
	t.mu.RLock()
	defer t.mu.RUnlock()
	c := &Topic{
		ID:         t.ID,
		Name:       t.Name,
		TenantID:   t.TenantID,
		Partitions: make(map[int]*Partition, len(t.Partitions)),
		CreatedAt:  t.CreatedAt,
	}
	for id, p := range t.Partitions {
		c.Partitions[id] = p.Clone()
	}
	return c
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
