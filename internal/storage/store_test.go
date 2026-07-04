package storage

import (
	"bytes"
	"fmt"
	"testing"
)

func reopen(t *testing.T, dir string, opts Options) *DurableStore {
	t.Helper()
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestReopenRestoresState(t *testing.T) {
	dir := t.TempDir()

	s := reopen(t, dir, Options{})
	mustPut(t, s, "k1", "v1")
	mustPut(t, s, "k2", "v2")
	if existed, err := s.Delete("k1"); err != nil || !existed {
		t.Fatalf("Delete(k1) = %v, %v", existed, err)
	}
	mustPut(t, s, "k3", "") // empty value must survive as present-but-empty
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := reopen(t, dir, Options{})
	if _, found := s2.Get("k1"); found {
		t.Fatal("deleted key k1 resurrected by recovery")
	}
	mustGet(t, s2, "k2", "v2")
	if v, found := s2.Get("k3"); !found || len(v) != 0 {
		t.Fatalf("Get(k3) = %q, %v; want empty, true", v, found)
	}
}

func TestRecoveryIsIdempotent(t *testing.T) {
	dir := t.TempDir()

	s := reopen(t, dir, Options{})
	mustPut(t, s, "k", "v")
	s.Close()

	// Recovering repeatedly from the same files — including reopens that
	// write nothing — must always produce the same state.
	for i := 0; i < 3; i++ {
		s := reopen(t, dir, Options{})
		mustGet(t, s, "k", "v")
		s.Close()
	}
}

func TestWritesAfterRecoveryContinueTheLog(t *testing.T) {
	dir := t.TempDir()

	s := reopen(t, dir, Options{})
	mustPut(t, s, "a", "1")
	s.Close()

	s2 := reopen(t, dir, Options{})
	mustPut(t, s2, "b", "2") // appends into the recovered segment
	s2.Close()

	s3 := reopen(t, dir, Options{})
	mustGet(t, s3, "a", "1")
	mustGet(t, s3, "b", "2")
}

func TestSnapshotTriggerTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir, Options{SnapshotThresholdBytes: 256})

	for i := 0; i < 100; i++ {
		mustPut(t, s, fmt.Sprintf("key-%03d", i), fmt.Sprintf("value-%03d", i))
	}

	// The threshold is far smaller than 100 records, so snapshots must
	// have happened and dead segments must be gone: exactly one (active)
	// segment remains, well under the total bytes written.
	segs, err := listSegments(s.wal.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("%d wal segments after snapshots, want 1", len(segs))
	}
	if s.wal.size >= 256+512 { // active segment holds only post-snapshot writes
		t.Fatalf("active segment is %d bytes; snapshotting isn't truncating", s.wal.size)
	}
	s.Close()

	s2 := reopen(t, dir, Options{})
	for i := 0; i < 100; i++ {
		mustGet(t, s2, fmt.Sprintf("key-%03d", i), fmt.Sprintf("value-%03d", i))
	}
}

func TestForcedSnapshotPlusTailReplay(t *testing.T) {
	dir := t.TempDir()

	s := reopen(t, dir, Options{})
	for i := 0; i < 10; i++ {
		mustPut(t, s, fmt.Sprintf("pre-%d", i), "x")
	}
	if err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		mustPut(t, s, fmt.Sprintf("post-%d", i), "y") // lives only in the WAL tail
	}
	s.Close()

	// Recovery must combine both sources: snapshot + WAL tail.
	s2 := reopen(t, dir, Options{})
	for i := 0; i < 10; i++ {
		mustGet(t, s2, fmt.Sprintf("pre-%d", i), "x")
	}
	for i := 0; i < 5; i++ {
		mustGet(t, s2, fmt.Sprintf("post-%d", i), "y")
	}
}

func TestDeleteOfAbsentKeyWritesNoLogEntry(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir, Options{})

	existed, err := s.Delete("never-existed")
	if err != nil || existed {
		t.Fatalf("Delete(absent) = %v, %v; want false, nil", existed, err)
	}
	if s.wal.size != 0 {
		t.Fatalf("no-op delete wrote %d bytes to the WAL", s.wal.size)
	}
}

func mustPut(t *testing.T, s *DurableStore, key, value string) {
	t.Helper()
	if err := s.Put(key, []byte(value)); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

func mustGet(t *testing.T, s *DurableStore, key, want string) {
	t.Helper()
	v, found := s.Get(key)
	if !found || !bytes.Equal(v, []byte(want)) {
		t.Fatalf("Get(%q) = %q, %v; want %q", key, v, found, want)
	}
}
