package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mem := NewMemStore()
	mem.Put("alpha", []byte("one"))
	mem.Put("empty", nil)
	mem.Put("binary", []byte{0x00, 0xFF, 0x0A, 0x00})

	if err := writeSnapshot(dir, 42, mem); err != nil {
		t.Fatal(err)
	}

	got, lastSeq, err := loadNewestSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 42 {
		t.Fatalf("lastSeq = %d, want 42", lastSeq)
	}
	if got.Len() != mem.Len() {
		t.Fatalf("restored %d keys, want %d", got.Len(), mem.Len())
	}
	mem.Range(func(key string, want []byte) bool {
		v, found := got.Get(key)
		if !found || !bytes.Equal(v, want) {
			t.Fatalf("restored %q = %q, %v; want %q", key, v, found, want)
		}
		return true
	})
}

func TestSnapshotChecksumDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	mem := NewMemStore()
	mem.Put("k", []byte("value"))
	if err := writeSnapshot(dir, 7, mem); err != nil {
		t.Fatal(err)
	}

	flipByte(t, snapshotPath(dir, 7), int64(len(snapMagic))+8+8+2) // inside the entry data

	if _, _, err := loadNewestSnapshot(dir); err == nil {
		t.Fatal("corrupt snapshot loaded without error")
	}
}

func TestLoadPicksNewestAndCleansTmp(t *testing.T) {
	dir := t.TempDir()

	old := NewMemStore()
	old.Put("k", []byte("old"))
	if err := writeSnapshot(dir, 10, old); err != nil {
		t.Fatal(err)
	}
	current := NewMemStore()
	current.Put("k", []byte("new"))
	if err := writeSnapshot(dir, 20, current); err != nil {
		t.Fatal(err)
	}
	// A stray .tmp is what a crash mid-snapshot leaves behind.
	stray := filepath.Join(dir, "00000000000000000099.snap.tmp")
	if err := os.WriteFile(stray, []byte("half-written"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, lastSeq, err := loadNewestSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 20 {
		t.Fatalf("loaded snapshot %d, want 20", lastSeq)
	}
	if v, _ := got.Get("k"); string(v) != "new" {
		t.Fatalf("loaded value %q, want %q", v, "new")
	}
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatal("stray .tmp file was not cleaned up")
	}
}

func TestNoSnapshotMeansEmptyStore(t *testing.T) {
	mem, lastSeq, err := loadNewestSnapshot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 0 || mem.Len() != 0 {
		t.Fatalf("empty dir loaded seq %d with %d keys", lastSeq, mem.Len())
	}
}

func TestPruneKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	mem := NewMemStore()
	for _, seq := range []uint64{10, 20, 30} {
		if err := writeSnapshot(dir, seq, mem); err != nil {
			t.Fatal(err)
		}
	}
	if err := pruneSnapshots(dir, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(snapshotPath(dir, 10)); !os.IsNotExist(err) {
		t.Fatal("prune kept the oldest snapshot")
	}
	for _, seq := range []uint64{20, 30} {
		if _, err := os.Stat(snapshotPath(dir, seq)); err != nil {
			t.Fatalf("prune removed snapshot %d: %v", seq, err)
		}
	}
}
