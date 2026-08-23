package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type Entry struct {
	Type    byte
	Payload []byte
}
type WAL struct {
	File *os.File
	mu   sync.Mutex
}

func Open(path string) (*WAL, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if e != nil {
		return nil, e
	}
	return &WAL{File: f}, nil
}
func (w *WAL) Append(e Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	h := make([]byte, 10)
	h[0] = e.Type
	binary.BigEndian.PutUint32(h[1:], uint32(len(e.Payload)))
	binary.BigEndian.PutUint32(h[5:], crc32.ChecksumIEEE(e.Payload))
	if _, x := w.File.Write(h); x != nil {
		return x
	}
	if _, x := w.File.Write(e.Payload); x != nil {
		return x
	}
	return w.File.Sync()
}
func (w *WAL) Replay(fn func(Entry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, e := w.File.Seek(0, io.SeekStart); e != nil {
		return e
	}
	r := bufio.NewReader(w.File)
	for {
		h := make([]byte, 10)
		if _, e := io.ReadFull(r, h); e != nil {
			if e == io.EOF {
				return nil
			}
			return e
		}
		n := binary.BigEndian.Uint32(h[1:])
		if n > 64<<20 {
			return io.ErrShortBuffer
		}
		p := make([]byte, n)
		if _, e := io.ReadFull(r, p); e != nil {
			return e
		}
		if crc32.ChecksumIEEE(p) != binary.BigEndian.Uint32(h[5:]) {
			return io.ErrUnexpectedEOF
		}
		if e := fn(Entry{Type: h[0], Payload: p}); e != nil {
			return e
		}
	}
}
func (w *WAL) Close() error { return w.File.Close() }
