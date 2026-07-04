package storage

import (
	"bytes"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{Op: OpPut, Key: "k", Value: []byte("v")},
		{Op: OpPut, Key: "empty-value", Value: nil},
		{Op: OpPut, Key: "binary", Value: []byte{0, 255, 10, 0}},
		{Op: OpDelete, Key: "gone"},
	}
	for _, want := range cases {
		got, err := decodeCommand(encodeCommand(nil, want))
		if err != nil {
			t.Fatalf("%+v: %v", want, err)
		}
		if got.Op != want.Op || got.Key != want.Key || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("round trip %+v -> %+v", want, got)
		}
	}
}

// TestDecodeRejectsMalformedInput feeds every prefix of a valid encoding
// plus targeted corruptions; none may panic or be silently accepted.
func TestDecodeRejectsMalformedInput(t *testing.T) {
	valid := encodeCommand(nil, Command{Op: OpPut, Key: "key", Value: []byte("value")})

	for cut := 0; cut < len(valid); cut++ {
		if _, err := decodeCommand(valid[:cut]); err == nil {
			t.Fatalf("truncation to %d bytes accepted", cut)
		}
	}

	badOp := append([]byte{}, valid...)
	badOp[0] = 99
	if _, err := decodeCommand(badOp); err == nil {
		t.Fatal("unknown op accepted")
	}

	// A huge keyLen must fail the bounds check, not allocate or overflow.
	overflow := append([]byte{}, valid...)
	overflow[1], overflow[2], overflow[3], overflow[4] = 0xFF, 0xFF, 0xFF, 0xFF
	if _, err := decodeCommand(overflow); err == nil {
		t.Fatal("absurd keyLen accepted")
	}
}
