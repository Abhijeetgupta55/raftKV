package raft

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Abhijeetgupta55/raftkv/internal/record"
	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

func entry(term, index uint64, command string) Entry {
	return Entry{
		Term: term, Index: index,
		Type:    raftv1.EntryType_ENTRY_TYPE_NORMAL,
		Command: []byte(command),
	}
}

func mustOpenLog(t *testing.T, dir string) (*logStore, []Entry) {
	t.Helper()
	ls, entries, err := openLog(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ls.close() })
	return ls, entries
}

func assertEntries(t *testing.T, got, want []Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Term != w.Term || g.Index != w.Index || g.Type != w.Type || !bytes.Equal(g.Command, w.Command) {
			t.Fatalf("entry %d = %+v, want %+v", i, g, w)
		}
	}
}

func TestLogAppendReopenRoundTrip(t *testing.T) {
	dir := t.TempDir()

	ls, entries := mustOpenLog(t, dir)
	if len(entries) != 0 {
		t.Fatalf("fresh log replayed %d entries", len(entries))
	}
	want := []Entry{
		entry(1, 1, "one"),
		entry(1, 2, ""), // empty command (the no-op entry a new leader appends)
		entry(2, 3, "three"),
	}
	if err := ls.append(want[0], want[1]); err != nil { // batched
		t.Fatal(err)
	}
	if err := ls.append(want[2]); err != nil {
		t.Fatal(err)
	}
	ls.close()

	_, got := mustOpenLog(t, dir)
	assertEntries(t, got, want)
}

// TestLogicalTruncation is the property that separates the Raft log from
// the KV WAL: appending at an existing index supersedes the old suffix.
func TestLogicalTruncation(t *testing.T) {
	dir := t.TempDir()

	ls, _ := mustOpenLog(t, dir)
	if err := ls.append(entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")); err != nil {
		t.Fatal(err)
	}
	// A new leader's conflicting entries overwrite indices 2 and 3.
	if err := ls.append(entry(2, 2, "B"), entry(2, 3, "C")); err != nil {
		t.Fatal(err)
	}
	ls.close()

	_, got := mustOpenLog(t, dir)
	assertEntries(t, got, []Entry{entry(1, 1, "a"), entry(2, 2, "B"), entry(2, 3, "C")})
}

func TestLogicalTruncationToBase(t *testing.T) {
	dir := t.TempDir()

	ls, _ := mustOpenLog(t, dir)
	if err := ls.append(entry(1, 1, "a"), entry(1, 2, "b")); err != nil {
		t.Fatal(err)
	}
	if err := ls.append(entry(3, 1, "A")); err != nil { // supersede everything
		t.Fatal(err)
	}
	ls.close()

	_, got := mustOpenLog(t, dir)
	assertEntries(t, got, []Entry{entry(3, 1, "A")})
}

func TestTornTailTruncatedAtEveryOffset(t *testing.T) {
	src := t.TempDir()
	ls, _ := mustOpenLog(t, src)
	if err := ls.append(entry(1, 1, "aaaa"), entry(1, 2, "bbbb")); err != nil {
		t.Fatal(err)
	}
	sizeAfter2, err := ls.f.Seek(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := ls.append(entry(1, 3, "cccc")); err != nil {
		t.Fatal(err)
	}
	ls.close()
	data, err := os.ReadFile(filepath.Join(src, logFileName))
	if err != nil {
		t.Fatal(err)
	}

	for cut := sizeAfter2; cut < int64(len(data)); cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, logFileName), data[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		_, got := mustOpenLog(t, dir)
		assertEntries(t, got, []Entry{entry(1, 1, "aaaa"), entry(1, 2, "bbbb")})

		// The torn bytes must be physically gone so appends can resume.
		info, err := os.Stat(filepath.Join(dir, logFileName))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != sizeAfter2 {
			t.Fatalf("cut at %d: file is %d bytes after recovery, want %d", cut, info.Size(), sizeAfter2)
		}
	}
}

func TestAppendResumesAfterRecovery(t *testing.T) {
	dir := t.TempDir()

	ls, _ := mustOpenLog(t, dir)
	if err := ls.append(entry(1, 1, "a")); err != nil {
		t.Fatal(err)
	}
	ls.close()

	ls2, _ := mustOpenLog(t, dir)
	if err := ls2.append(entry(1, 2, "b")); err != nil {
		t.Fatal(err)
	}
	ls2.close()

	_, got := mustOpenLog(t, dir)
	assertEntries(t, got, []Entry{entry(1, 1, "a"), entry(1, 2, "b")})
}

func TestIndexGapInFileIsFatal(t *testing.T) {
	dir := t.TempDir()
	writeRawEntries(t, dir, entry(1, 1, "a"), entry(1, 3, "gap"))

	if _, _, err := openLog(dir, 0); err == nil {
		t.Fatal("openLog accepted a log with an index gap")
	}
}

func TestLogNotStartingAtOneIsFatal(t *testing.T) {
	dir := t.TempDir()
	writeRawEntries(t, dir, entry(1, 2, "headless"))

	if _, _, err := openLog(dir, 0); err == nil {
		t.Fatal("openLog accepted a log that lost its head")
	}
}

func TestNonContiguousAppendRejected(t *testing.T) {
	ls, _ := mustOpenLog(t, t.TempDir())
	if err := ls.append(entry(1, 1, "a"), entry(1, 3, "gap")); err == nil {
		t.Fatal("append accepted a non-contiguous batch")
	}
}

func TestAppendErrorIsSticky(t *testing.T) {
	ls, _ := mustOpenLog(t, t.TempDir())
	ls.f.Close() // simulate the handle dying

	if err := ls.append(entry(1, 1, "a")); err == nil {
		t.Fatal("append to closed file succeeded")
	}
	if err := ls.append(entry(1, 2, "b")); err != errLogFailed {
		t.Fatalf("append after failure = %v, want errLogFailed", err)
	}
}

// writeRawEntries builds a log file directly on disk, bypassing append's
// validation, to simulate damage append could never produce.
func writeRawEntries(t *testing.T, dir string, entries ...Entry) {
	t.Helper()
	var buf []byte
	for _, e := range entries {
		p := make([]byte, 0, entryHeaderLen+len(e.Command))
		p = appendUint64(p, e.Term)
		p = appendUint64(p, e.Index)
		p = append(p, byte(e.Type))
		p = append(p, e.Command...)
		buf = record.Frame(buf, p)
	}
	if err := os.WriteFile(filepath.Join(dir, logFileName), buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendUint64(b []byte, v uint64) []byte {
	for i := 0; i < 8; i++ {
		b = append(b, byte(v>>(8*i)))
	}
	return b
}

func TestManyEntriesSurvive(t *testing.T) {
	dir := t.TempDir()
	ls, _ := mustOpenLog(t, dir)

	var want []Entry
	for i := uint64(1); i <= 200; i++ {
		e := entry(i/50+1, i, fmt.Sprintf("cmd-%d", i))
		want = append(want, e)
	}
	if err := ls.append(want...); err != nil {
		t.Fatal(err)
	}
	ls.close()

	_, got := mustOpenLog(t, dir)
	assertEntries(t, got, want)
}
