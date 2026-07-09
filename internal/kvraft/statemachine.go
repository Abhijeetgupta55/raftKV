// Package kvraft wires the replicated KV state machine and the
// client-facing gRPC service onto a raft.Node. It is the seam where the
// consensus layer (which knows only opaque bytes) meets the KV semantics
// (which know nothing of elections): the Raft log carries encoded
// commands, and this state machine is the only thing that interprets them.
package kvraft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/Abhijeetgupta55/raftkv/internal/storage"
)

// command is the envelope replicated through Raft: a client identity and a
// monotonic per-client serial wrapping an opaque storage mutation. The
// (clientID, serial) pair is what makes a retried write exactly-once — the
// state machine applies a given serial at most once (M4 session dedup).
// clientID 0 means "no session" (fire-and-forget, no dedup).
//
// Wire form (little-endian): uint64 clientID | uint64 serial | inner bytes,
// where inner is a storage.Command produced by storage.EncodeCommand.
type command struct {
	clientID uint64
	serial   uint64
	inner    []byte
}

func encodeCommand(c command) []byte {
	b := make([]byte, 0, 16+len(c.inner))
	b = binary.LittleEndian.AppendUint64(b, c.clientID)
	b = binary.LittleEndian.AppendUint64(b, c.serial)
	return append(b, c.inner...)
}

func decodeCommand(b []byte) (command, error) {
	if len(b) < 16 {
		return command{}, fmt.Errorf("kvraft: command envelope truncated: %d bytes", len(b))
	}
	return command{
		clientID: binary.LittleEndian.Uint64(b),
		serial:   binary.LittleEndian.Uint64(b[8:]),
		inner:    b[16:],
	}, nil
}

// stateMachine is the replicated KV map plus the session table that makes
// writes idempotent. It implements raft.StateMachine. All access is under
// mu because Apply runs on the Raft loop goroutine while Get reads happen
// on gRPC handler goroutines (after a ReadBarrier).
type stateMachine struct {
	mu       sync.RWMutex
	data     *storage.MemStore
	sessions map[uint64]uint64 // clientID -> highest serial applied
}

func newStateMachine() *stateMachine {
	return &stateMachine{data: storage.NewMemStore(), sessions: map[uint64]uint64{}}
}

// Apply interprets one committed command. The dedup check is the heart of
// exactly-once: a duplicate delivery of an already-applied serial is a
// no-op, so a client that retries a timed-out write never double-applies.
func (sm *stateMachine) Apply(index uint64, data []byte) {
	c, err := decodeCommand(data)
	if err != nil {
		// A committed entry that won't decode is a programming error, not a
		// runtime condition; skipping it keeps every replica identical
		// (they all skip the same bytes) rather than diverging on a panic.
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if c.clientID != 0 && c.serial <= sm.sessions[c.clientID] {
		return // already applied this serial: exactly-once dedup
	}
	cmd, err := storage.DecodeCommand(c.inner)
	if err != nil {
		return
	}
	switch cmd.Op {
	case storage.OpPut:
		sm.data.Put(cmd.Key, cmd.Value)
	case storage.OpDelete:
		sm.data.Delete(cmd.Key)
	}
	if c.clientID != 0 {
		sm.sessions[c.clientID] = c.serial
	}
}

func (sm *stateMachine) get(key string) ([]byte, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.data.Get(key)
}

// snapshot image: both the KV data and the session table must be captured,
// or a restore would forget which serials were applied and dedup would
// break across a snapshot/InstallSnapshot boundary.
type snapshotImage struct {
	Data     map[string][]byte
	Sessions map[uint64]uint64
}

func (sm *stateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	img := snapshotImage{Data: map[string][]byte{}, Sessions: map[uint64]uint64{}}
	sm.data.Range(func(k string, v []byte) bool {
		img.Data[k] = append([]byte(nil), v...)
		return true
	})
	for id, s := range sm.sessions {
		img.Sessions[id] = s
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sm *stateMachine) Restore(data []byte) error {
	var img snapshotImage
	if len(data) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&img); err != nil {
			return err
		}
	}
	mem := storage.NewMemStore()
	for k, v := range img.Data {
		mem.Put(k, v)
	}
	sm.mu.Lock()
	sm.data = mem
	sm.sessions = img.Sessions
	if sm.sessions == nil {
		sm.sessions = map[uint64]uint64{}
	}
	sm.mu.Unlock()
	return nil
}
