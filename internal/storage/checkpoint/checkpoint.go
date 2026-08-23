package checkpoint

import (
	"encoding/json"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	Path    string
	Offsets map[string]map[int]domain.Offset
	mu      sync.Mutex
}

func Open(dir string) (*Store, error) {
	if e := os.MkdirAll(dir, 0755); e != nil {
		return nil, e
	}
	s := &Store{Path: filepath.Join(dir, "offsets.json"), Offsets: map[string]map[int]domain.Offset{}}
	if b, e := os.ReadFile(s.Path); e == nil {
		_ = json.Unmarshal(b, &s.Offsets)
	}
	return s, nil
}
func (s *Store) Commit(group string, part int, off domain.Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Offsets[group] == nil {
		s.Offsets[group] = map[int]domain.Offset{}
	}
	if old, ok := s.Offsets[group][part]; ok && off < old {
		return domain.E(domain.ErrConflict, "offset regression")
	}
	s.Offsets[group][part] = off
	b, _ := json.MarshalIndent(s.Offsets, "", "  ")
	tmp := s.Path + ".tmp"
	if e := os.WriteFile(tmp, b, 0644); e != nil {
		return e
	}
	return os.Rename(tmp, s.Path)
}
func (s *Store) Get(group string, part int) domain.Offset {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Offsets[group][part]
}
