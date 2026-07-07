// Package raft implements the Raft consensus protocol from the paper.
// It replicates opaque command bytes and knows nothing about what they
// mean; the state machine lives above it, the disk primitives below it.
package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Abhijeetgupta55/raftkv/internal/record"
	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// Entry is one slot in the replicated log.
type Entry struct {
	Term    uint64
	Index   uint64
	Type    raftv1.EntryType
	Command []byte
}

func (e Entry) toProto() *raftv1.LogEntry {
	return &raftv1.LogEntry{Term: e.Term, Index: e.Index, Type: e.Type, Command: e.Command}
}

func entryFromProto(p *raftv1.LogEntry) Entry {
	return Entry{Term: p.GetTerm(), Index: p.GetIndex(), Type: p.GetType(), Command: p.GetCommand()}
}

// The Raft log is a single append-only file of CRC-framed records
// (payload: uint64 term | uint64 index | command). Unlike the KV WAL,
// indices in the file may move *backwards*: Raft followers must discard
// conflicting log suffixes, and rather than physically truncating the
// file (a crash mid-truncate is hard to reason about), truncation is
// logical — appending an entry at index i supersedes everything at ≥ i,
// and replay applies that rule. The dead bytes cost is bounded: Milestone
// 3's compaction discards the whole file periodically.
//
// The torn-tail policy is inherited from the KV WAL: the first invalid
// record ends the log, which is safe because appends are fsynced before
// any RPC response or vote is sent, so whatever follows was never
// promised to anyone.

const (
	logFileName    = "log"
	entryHeaderLen = 17 // uint64 term + uint64 index + uint8 type before the command
	// Sanity bound for replay: far above the service-layer 1 MiB value
	// limit, small enough to reject absurd lengths from a corrupt header.
	maxEntryPayload = entryHeaderLen + (64 << 20)
)

// errLogFailed mirrors the KV WAL's sticky failure: after any write or
// sync error the file's on-disk state is unknown, so the log refuses all
// further appends rather than risk writing records after a torn one.
var errLogFailed = errors.New("raft: log failed by an earlier write error")

type logStore struct {
	f          *os.File
	dir        string
	payloadBuf []byte
	frameBuf   []byte
	failed     bool
}

// openLog opens (creating if absent) the Raft log in dir and replays it,
// returning the surviving entries — contiguous, ascending, conflicts
// already resolved by the supersede rule. base is the index the log
// starts after (the snapshot's last included index; 0 for a fresh node):
// the first entry must be base+1 or the log lost its head.
func openLog(dir string, base uint64) (*logStore, []Entry, error) {
	path := filepath.Join(dir, logFileName)

	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}

	var entries []Entry
	off := int64(0)
	for off < int64(len(data)) {
		payload, recLen, ok := record.Parse(data[off:], maxEntryPayload)
		if !ok || len(payload) < entryHeaderLen {
			break // torn tail: never acknowledged, truncated below
		}
		e := Entry{
			Term:    binary.LittleEndian.Uint64(payload),
			Index:   binary.LittleEndian.Uint64(payload[8:]),
			Type:    raftv1.EntryType(payload[16]),
			Command: append([]byte(nil), payload[entryHeaderLen:]...),
		}

		switch {
		case len(entries) == 0:
			if e.Index != base+1 {
				return nil, nil, fmt.Errorf("raft: log starts at index %d, want %d (snapshot covers through %d)",
					e.Index, base+1, base)
			}
		case e.Index == entries[len(entries)-1].Index+1:
			// The common case: the log grows.
		case e.Index >= entries[0].Index && e.Index <= entries[len(entries)-1].Index:
			// Logical truncation: a later append superseded this suffix.
			entries = entries[:e.Index-entries[0].Index]
		default:
			return nil, nil, fmt.Errorf("raft: log index jumps from %d to %d",
				entries[len(entries)-1].Index, e.Index)
		}
		entries = append(entries, e)
		off += recLen
	}

	// Truncate the torn tail (no-op when the file ended cleanly), then
	// reopen for appending.
	if err := os.Truncate(path, off); err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("raft: truncate torn log tail: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if err := record.SyncDir(dir); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &logStore{f: f, dir: dir}, entries, nil
}

// append durably writes entries — framed together, one write, one fsync —
// and does not return until they are on disk. This is the persistence
// half of Raft's contract: an entry must be durable before the node
// acknowledges it to the leader or counts it toward its own majority.
//
// Entries must be contiguous; the first may rewind to supersede a
// conflicting suffix (but never below the log's base). The in-memory
// log in raft.go enforces the same rule, so a violation here is a bug,
// not bad input.
func (l *logStore) append(entries ...Entry) error {
	if l.failed {
		return errLogFailed
	}
	if len(entries) == 0 {
		return nil
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("raft: non-contiguous append: %d after %d",
				entries[i].Index, entries[i-1].Index)
		}
	}

	b := l.frameBuf[:0]
	for _, e := range entries {
		p := l.payloadBuf[:0]
		p = binary.LittleEndian.AppendUint64(p, e.Term)
		p = binary.LittleEndian.AppendUint64(p, e.Index)
		p = append(p, byte(e.Type))
		p = append(p, e.Command...)
		l.payloadBuf = p
		b = record.Frame(b, p)
	}
	l.frameBuf = b

	if _, err := l.f.Write(b); err != nil {
		l.failed = true
		return err
	}
	if err := l.f.Sync(); err != nil {
		l.failed = true
		return err
	}
	return nil
}

func (l *logStore) close() error {
	return l.f.Close()
}

// rewrite atomically replaces the whole log file with exactly the given
// entries — the compaction path (entries covered by a snapshot are
// dropped) and the snapshot-installation path (everything is dropped).
// The file is closed first because Windows cannot rename over an open
// file; a crash between close and rename leaves the old log intact.
func (l *logStore) rewrite(entries []Entry) error {
	if l.failed {
		return errLogFailed
	}
	if err := l.f.Close(); err != nil {
		l.failed = true
		return err
	}

	var buf []byte
	for _, e := range entries {
		p := make([]byte, 0, entryHeaderLen+len(e.Command))
		p = binary.LittleEndian.AppendUint64(p, e.Term)
		p = binary.LittleEndian.AppendUint64(p, e.Index)
		p = append(p, byte(e.Type))
		p = append(p, e.Command...)
		buf = record.Frame(buf, p)
	}

	path := filepath.Join(l.dir, logFileName)
	if err := record.WriteFileAtomic(path, buf); err != nil {
		l.failed = true
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		l.failed = true
		return err
	}
	l.f = f
	return nil
}
