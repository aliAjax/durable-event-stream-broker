package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

var ErrCursor = errors.New("invalid cursor")

func SignCursor(topic string, part int, off int64, secret []byte) string {
	p := topic + "|" + strconv.Itoa(part) + "|" + strconv.FormatInt(off, 10)
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(p))
	return base64.RawURLEncoding.EncodeToString([]byte(p + "|" + base64.RawURLEncoding.EncodeToString(h.Sum(nil))))
}
func ParseCursor(cur string, secret []byte) (string, int, int64, error) {
	b, e := base64.RawURLEncoding.DecodeString(cur)
	if e != nil {
		return "", 0, 0, ErrCursor
	}
	p := strings.Split(string(b), "|")
	if len(p) != 4 {
		return "", 0, 0, ErrCursor
	}
	want := SignCursor(p[0], atoi(p[1]), atoi64(p[2]), secret)
	if !hmac.Equal([]byte(cur), []byte(want)) {
		return "", 0, 0, ErrCursor
	}
	return p[0], atoi(p[1]), atoi64(p[2]), nil
}
func atoi(s string) int     { v, _ := strconv.Atoi(s); return v }
func atoi64(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
