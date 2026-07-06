package record

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFrameParseRoundTrip(t *testing.T) {
	var buf []byte
	payloads := [][]byte{[]byte("first"), {}, []byte("third-longer-payload")}
	for _, p := range payloads {
		buf = Frame(buf, p)
	}

	off := int64(0)
	for i, want := range payloads {
		payload, recLen, ok := Parse(buf[off:], 1<<20)
		if !ok {
			t.Fatalf("record %d failed to parse", i)
		}
		if !bytes.Equal(payload, want) {
			t.Fatalf("record %d = %q, want %q", i, payload, want)
		}
		off += recLen
	}
	if off != int64(len(buf)) {
		t.Fatalf("parsed %d bytes of %d", off, len(buf))
	}
}

func TestParseRejectsTruncationAndCorruption(t *testing.T) {
	full := Frame(nil, []byte("payload"))

	for cut := 0; cut < len(full); cut++ {
		if _, _, ok := Parse(full[:cut], 1<<20); ok {
			t.Fatalf("truncation to %d bytes parsed as valid", cut)
		}
	}

	for i := range full {
		bad := append([]byte(nil), full...)
		bad[i] ^= 0xFF
		if payload, _, ok := Parse(bad, 1<<20); ok && bytes.Equal(payload, []byte("payload")) {
			t.Fatalf("flipping byte %d went undetected", i)
		}
	}

	if _, _, ok := Parse(full, 3); ok {
		t.Fatal("payload above maxPayload parsed as valid")
	}
}

func TestWriteFileAtomicReplaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")

	if err := WriteFileAtomic(path, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(path, []byte("v2")); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v2" {
		t.Fatalf("read %q, want %q", got, "v2")
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
}
