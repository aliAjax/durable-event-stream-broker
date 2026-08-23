package domain

import "testing"

func TestHeaderSizeIgnoresEmptyKey(t *testing.T) {
	if (Headers{"": "ignored", "x": "1"}).Size() != 2 { t.Fatal("empty header key counted") }
}
