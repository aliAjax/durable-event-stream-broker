package domain

import (
	"encoding/json"
	"sort"
	"strings"
)

type SchemaReference struct {
	ID, Subject, Version, Format string
	Fields                       map[string]string
}

func (s SchemaReference) Validate() error {
	if s.ID == "" || s.Subject == "" || s.Version == "" {
		return E(ErrInvalid, "schema identity required")
	}
	return nil
}
func (s SchemaReference) Fingerprint() string {
	keys := make([]string, 0, len(s.Fields))
	for k := range s.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(s.Fields[k])
		b.WriteByte(';')
	}
	return b.String()
}
func (s SchemaReference) JSON() []byte { b, _ := json.Marshal(s); return b }
