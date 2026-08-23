package segment

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/example/persistent-event-stream-broker/internal/domain"
)

const magic uint32 = 0x42524b31

type Segment struct {
	Path  string
	File  *os.File
	Index map[domain.Offset]int64
	mu    sync.Mutex
}

func Open(dir string, base int64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "segment-"+itoa(base)+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	s := &Segment{Path: path, File: f, Index: map[domain.Offset]int64{}}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}
func (s *Segment) Append(rs []domain.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range rs {
		payload, err := json.Marshal(r)
		if err != nil {
			return err
		}
		head := make([]byte, 12)
		binary.BigEndian.PutUint32(head, magic)
		binary.BigEndian.PutUint32(head[4:], uint32(len(payload)))
		binary.BigEndian.PutUint32(head[8:], crc32.ChecksumIEEE(payload))
		pos, _ := s.File.Seek(0, io.SeekEnd)
		if _, err = s.File.Write(head); err != nil {
			return err
		}
		if _, err = s.File.Write(payload); err != nil {
			return err
		}
		s.Index[r.Offset] = pos
	}
	return s.File.Sync()
}
func (s *Segment) Read(after domain.Offset, limit int) ([]domain.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.File.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	br := bufio.NewReader(s.File)
	out := []domain.Record{}
	for len(out) < limit {
		h := make([]byte, 12)
		if _, err := io.ReadFull(br, h); err != nil {
			if err == io.EOF {
				return out, nil
			}
			return out, err
		}
		if binary.BigEndian.Uint32(h) != magic {
			return out, io.ErrUnexpectedEOF
		}
		n := binary.BigEndian.Uint32(h[4:])
		if n > 64<<20 {
			return out, io.ErrShortBuffer
		}
		p := make([]byte, n)
		if _, err := io.ReadFull(br, p); err != nil {
			return out, err
		}
		if crc32.ChecksumIEEE(p) != binary.BigEndian.Uint32(h[8:]) {
			return out, io.ErrUnexpectedEOF
		}
		var r domain.Record
		if err := json.Unmarshal(p, &r); err != nil {
			return out, err
		}
		if r.Offset > after {
			out = append(out, r)
		}
	}
	return out, nil
}
func (s *Segment) recover() error {
	if _, err := s.File.Seek(0, io.SeekStart); err != nil {
		return err
	}
	br := bufio.NewReader(s.File)
	var valid int64
	for {
		h := make([]byte, 12)
		_, err := io.ReadFull(br, h)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		if binary.BigEndian.Uint32(h) != magic {
			break
		}
		n := binary.BigEndian.Uint32(h[4:])
		if n > 64<<20 {
			break
		}
		p := make([]byte, n)
		if _, err = io.ReadFull(br, p); err != nil {
			break
		}
		if crc32.ChecksumIEEE(p) != binary.BigEndian.Uint32(h[8:]) {
			break
		}
		var r domain.Record
		if json.Unmarshal(p, &r) != nil {
			break
		}
		s.Index[r.Offset] = valid
		valid += 12 + int64(n)
	}
	st, _ := s.File.Stat()
	if valid < st.Size() {
		if err := s.File.Truncate(valid); err != nil {
			return err
		}
	}
	_, err := s.File.Seek(0, io.SeekEnd)
	return err
}
func (s *Segment) Close() error { return s.File.Close() }
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	b := make([]byte, 0, 20)
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
