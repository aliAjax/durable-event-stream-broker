package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"io"
)

var ErrFrame = errors.New("invalid frame")

func EncodeBatch(rs []domain.Record) ([]byte, error) {
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, uint32(len(rs)))
	for _, r := range rs {
		if len(r.Value) > 16<<20 {
			return nil, ErrFrame
		}
		binary.Write(&b, binary.BigEndian, uint32(len(r.Key)))
		b.WriteString(r.Key)
		binary.Write(&b, binary.BigEndian, uint32(len(r.Value)))
		b.Write(r.Value)
		binary.Write(&b, binary.BigEndian, uint64(r.Offset))
	}
	return b.Bytes(), nil
}
func DecodeBatch(data []byte) ([]domain.Record, error) {
	r := bytes.NewReader(data)
	var n uint32
	if binary.Read(r, binary.BigEndian, &n) != nil || n > 100000 {
		return nil, ErrFrame
	}
	out := make([]domain.Record, 0, n)
	for i := uint32(0); i < n; i++ {
		var kl, vl uint32
		if binary.Read(r, binary.BigEndian, &kl) != nil || kl > 1<<20 {
			return nil, ErrFrame
		}
		k := make([]byte, kl)
		if _, e := io.ReadFull(r, k); e != nil {
			return nil, e
		}
		if binary.Read(r, binary.BigEndian, &vl) != nil || vl > 16<<20 {
			return nil, ErrFrame
		}
		v := make([]byte, vl)
		if _, e := io.ReadFull(r, v); e != nil {
			return nil, e
		}
		var off uint64
		if binary.Read(r, binary.BigEndian, &off) != nil {
			return nil, ErrFrame
		}
		out = append(out, domain.Record{Key: string(k), Value: v, Offset: domain.Offset(off)})
	}
	if r.Len() != 0 {
		return nil, ErrFrame
	}
	return out, nil
}
