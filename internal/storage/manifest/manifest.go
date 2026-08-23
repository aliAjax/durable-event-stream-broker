package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Entry struct {
	Segment  string `json:"segment"`
	Base     int64  `json:"base"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}
type Manifest struct {
	Path    string
	Entries []Entry
	mu      sync.Mutex
}

func Open(dir string) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	m := &Manifest{Path: filepath.Join(dir, "MANIFEST.json")}
	b, e := os.ReadFile(m.Path)
	if e == nil {
		_ = json.Unmarshal(b, m)
	}
	return m, nil
}
func (m *Manifest) Add(e Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Entries = append(m.Entries, e)
	return m.persist()
}
func (m *Manifest) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.Entries[:0]
	for _, v := range m.Entries {
		if v.Segment != name {
			out = append(out, v)
		}
	}
	m.Entries = out
	return m.persist()
}
func (m *Manifest) persist() error {
	b, e := json.MarshalIndent(m, "", "  ")
	if e != nil {
		return e
	}
	tmp := m.Path + ".tmp"
	if e = os.WriteFile(tmp, b, 0644); e != nil {
		return e
	}
	return os.Rename(tmp, m.Path)
}
