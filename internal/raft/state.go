package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Abhijeetgupta55/raftkv/internal/record"
)

// currentTerm and votedFor are the other two thirds of Raft's persistent
// state (the log is the first). They live in a tiny file replaced
// atomically on every change:
//
//	magic "RKVVOTE1" | uint64 term | uint64 votedFor | uint32 crc32c
//
// The ordering contract callers must honor is the whole point: the file
// is rewritten and fsynced BEFORE the action it records becomes visible —
// before a vote response leaves the node, before a candidate requests
// votes for a new term. A node that forgot its vote after a crash could
// vote twice in the same term, electing two leaders and breaking the
// election-safety property, so this write is never deferred or batched.

const (
	stateFileName = "state"
	stateMagic    = "RKVVOTE1"
	stateFileSize = len(stateMagic) + 8 + 8 + 4
)

// noVote is the votedFor value meaning "no vote cast this term".
// Node IDs are 1-based, so 0 is safely out of band.
const noVote uint64 = 0

// saveTermAndVote durably records term and votedFor, returning only
// after both are on disk.
func saveTermAndVote(dir string, term, votedFor uint64) error {
	b := make([]byte, 0, stateFileSize)
	b = append(b, stateMagic...)
	b = binary.LittleEndian.AppendUint64(b, term)
	b = binary.LittleEndian.AppendUint64(b, votedFor)
	b = binary.LittleEndian.AppendUint32(b, record.Checksum(b))
	return record.WriteFileAtomic(filepath.Join(dir, stateFileName), b)
}

// loadTermAndVote restores the persisted term and vote, returning zeros
// for a node that has never voted. Corruption is fatal: a node that
// cannot trust its own vote record must not participate in elections.
func loadTermAndVote(dir string) (term, votedFor uint64, err error) {
	data, err := os.ReadFile(filepath.Join(dir, stateFileName))
	if os.IsNotExist(err) {
		return 0, noVote, nil
	}
	if err != nil {
		return 0, 0, err
	}

	if len(data) != stateFileSize {
		return 0, 0, fmt.Errorf("raft: state file is %d bytes, want %d", len(data), stateFileSize)
	}
	body, tail := data[:len(data)-4], data[len(data)-4:]
	if record.Checksum(body) != binary.LittleEndian.Uint32(tail) {
		return 0, 0, fmt.Errorf("raft: state file checksum mismatch — disk corruption")
	}
	if string(body[:len(stateMagic)]) != stateMagic {
		return 0, 0, fmt.Errorf("raft: state file has bad magic")
	}

	term = binary.LittleEndian.Uint64(body[len(stateMagic):])
	votedFor = binary.LittleEndian.Uint64(body[len(stateMagic)+8:])
	return term, votedFor, nil
}
