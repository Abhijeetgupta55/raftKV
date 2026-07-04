package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// buildWAL writes records seq 1..n into dir and returns the byte offset
// at which each record ends, so tests can cut and corrupt at exact
// boundaries.
func buildWAL(t *testing.T, dir string, n int) (recordEnds []int64) {
	t.Helper()
	w, err := createWAL(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for seq := 1; seq <= n; seq++ {
		if err := w.Append(uint64(seq), []byte(fmt.Sprintf("command-%d", seq))); err != nil {
			t.Fatal(err)
		}
		recordEnds = append(recordEnds, w.size)
	}
	return recordEnds
}

func replayAll(t *testing.T, dir string, fromSeq uint64) (seqs []uint64, lastSeq uint64, validSize int64, err error) {
	t.Helper()
	lastSeq, validSize, err = replayWAL(dir, fromSeq, func(seq uint64, command []byte) error {
		seqs = append(seqs, seq)
		want := fmt.Sprintf("command-%d", seq)
		if string(command) != want {
			t.Fatalf("record %d replayed %q, want %q", seq, command, want)
		}
		return nil
	})
	return seqs, lastSeq, validSize, err
}

func TestAppendReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ends := buildWAL(t, dir, 5)

	seqs, lastSeq, validSize, err := replayAll(t, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 5 || len(seqs) != 5 {
		t.Fatalf("replayed %v (lastSeq %d), want 1..5", seqs, lastSeq)
	}
	if validSize != ends[4] {
		t.Fatalf("validSize %d, want full file %d", validSize, ends[4])
	}
}

func TestReplaySkipsSnapshotCoveredRecords(t *testing.T) {
	dir := t.TempDir()
	buildWAL(t, dir, 5)

	seqs, lastSeq, _, err := replayAll(t, dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 5 || len(seqs) != 2 || seqs[0] != 4 || seqs[1] != 5 {
		t.Fatalf("replayed %v from seq 3, want [4 5]", seqs)
	}
}

// TestTornTailTruncatedAtEveryOffset is the torn-write test: for every
// possible byte length of a half-written final record — as if the process
// died at that exact moment mid-append — replay must recover exactly the
// first two records and report the cut point for truncation.
func TestTornTailTruncatedAtEveryOffset(t *testing.T) {
	src := t.TempDir()
	ends := buildWAL(t, src, 3)
	data, err := os.ReadFile(segmentPath(src, 1))
	if err != nil {
		t.Fatal(err)
	}

	for cut := ends[1]; cut < ends[2]; cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(segmentPath(dir, 1), data[:cut], 0o644); err != nil {
			t.Fatal(err)
		}

		seqs, lastSeq, validSize, err := replayAll(t, dir, 0)
		if err != nil {
			t.Fatalf("cut at %d: replay failed: %v", cut, err)
		}
		if lastSeq != 2 || len(seqs) != 2 {
			t.Fatalf("cut at %d: replayed %v, want [1 2]", cut, seqs)
		}
		if validSize != ends[1] {
			t.Fatalf("cut at %d: validSize %d, want %d", cut, validSize, ends[1])
		}
	}
}

func TestCorruptionInFinishedSegmentIsFatal(t *testing.T) {
	dir := t.TempDir()
	w, err := createWAL(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if err := w.Append(seq, []byte(fmt.Sprintf("command-%d", seq))); err != nil {
			t.Fatal(err)
		}
	}
	// Rotate so segment 1 is "finished"; write one more record after.
	if err := w.rotate(4); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(4, []byte("command-4")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	flipByte(t, segmentPath(dir, 1), walHeaderSize+walSeqSize) // first command byte of record 1

	if _, _, _, err := replayAll(t, dir, 0); err == nil {
		t.Fatal("replay accepted a corrupt record in a finished segment")
	}
}

// TestCorruptionMidNewestSegmentEndsLogThere pins the documented
// limitation: within the newest segment, corruption before the tail is
// indistinguishable from a torn write, so replay stops at the first
// invalid record even if intact records follow.
func TestCorruptionMidNewestSegmentEndsLogThere(t *testing.T) {
	dir := t.TempDir()
	ends := buildWAL(t, dir, 3)

	flipByte(t, segmentPath(dir, 1), ends[0]+walHeaderSize+walSeqSize) // inside record 2

	seqs, lastSeq, validSize, err := replayAll(t, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 1 || len(seqs) != 1 {
		t.Fatalf("replayed %v, want just [1]", seqs)
	}
	if validSize != ends[0] {
		t.Fatalf("validSize %d, want %d", validSize, ends[0])
	}
}

func TestSequenceGapIsFatal(t *testing.T) {
	dir := t.TempDir()
	w, err := createWAL(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(1, []byte("command-1")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(3, []byte("command-3")); err != nil { // hole: no seq 2
		t.Fatal(err)
	}
	w.Close()

	if _, _, _, err := replayAll(t, dir, 0); err == nil {
		t.Fatal("replay accepted a sequence gap")
	}
}

func TestAppendErrorIsSticky(t *testing.T) {
	dir := t.TempDir()
	w, err := createWAL(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	w.f.Close() // simulate the file handle dying mid-flight

	if err := w.Append(1, []byte("x")); err == nil {
		t.Fatal("append to closed file succeeded")
	}
	if err := w.Append(2, []byte("y")); err != errWALFailed {
		t.Fatalf("second append after failure = %v, want errWALFailed", err)
	}
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[off] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestListSegmentsRejectsStrangers(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "not-a-number.wal"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := listSegments(dir); err == nil {
		t.Fatal("listSegments accepted a non-numeric segment name")
	}
}
