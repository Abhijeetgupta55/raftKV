package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Abhijeetgupta55/raftkv/internal/record"
	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// A Raft snapshot bundles the state machine's serialized state with the
// log metadata needed to splice it back into the protocol: the index and
// term it covers through, and the membership as of that index (so a
// brand-new node learns the cluster from the snapshot alone).
//
//	magic "RKVRSNP1" | uint64 lastIndex | uint64 lastTerm
//	| uint32 configLen | config | data | uint32 crc32c
//
// One file per node, atomically replaced. Everything at or below
// lastIndex is committed by definition — a snapshot is only ever taken
// of applied state — so recovery treats it as the commit floor.

const (
	raftSnapFileName = "snapshot"
	raftSnapMagic    = "RKVRSNP1"
)

type raftSnapshot struct {
	lastIndex uint64
	lastTerm  uint64
	config    map[uint64]string
	data      []byte
}

func persistRaftSnapshot(dir string, s raftSnapshot) error {
	cfg := encodeMembers(s.config)
	b := make([]byte, 0, len(raftSnapMagic)+16+4+len(cfg)+len(s.data)+4)
	b = append(b, raftSnapMagic...)
	b = binary.LittleEndian.AppendUint64(b, s.lastIndex)
	b = binary.LittleEndian.AppendUint64(b, s.lastTerm)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(cfg)))
	b = append(b, cfg...)
	b = append(b, s.data...)
	b = binary.LittleEndian.AppendUint32(b, record.Checksum(b))
	return record.WriteFileAtomic(filepath.Join(dir, raftSnapFileName), b)
}

// loadRaftSnapshot returns the node's snapshot, or a zero snapshot (and
// no error) when none exists. Corruption is fatal: the snapshot is the
// log's replacement, and a node that lost it cannot rejoin safely on its
// own.
func loadRaftSnapshot(dir string) (raftSnapshot, error) {
	path := filepath.Join(dir, raftSnapFileName)
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return raftSnapshot{}, nil
	}
	if err != nil {
		return raftSnapshot{}, err
	}

	if len(b) < len(raftSnapMagic)+16+4+4 {
		return raftSnapshot{}, fmt.Errorf("raft: snapshot file too short (%d bytes)", len(b))
	}
	body, tail := b[:len(b)-4], b[len(b)-4:]
	if record.Checksum(body) != binary.LittleEndian.Uint32(tail) {
		return raftSnapshot{}, fmt.Errorf("raft: snapshot checksum mismatch — disk corruption")
	}
	if string(body[:len(raftSnapMagic)]) != raftSnapMagic {
		return raftSnapshot{}, fmt.Errorf("raft: snapshot has bad magic")
	}
	body = body[len(raftSnapMagic):]

	s := raftSnapshot{
		lastIndex: binary.LittleEndian.Uint64(body),
		lastTerm:  binary.LittleEndian.Uint64(body[8:]),
	}
	cfgLen := binary.LittleEndian.Uint32(body[16:])
	body = body[20:]
	if uint64(len(body)) < uint64(cfgLen) {
		return raftSnapshot{}, fmt.Errorf("raft: snapshot config truncated")
	}
	if s.config, err = decodeMembers(body[:cfgLen]); err != nil {
		return raftSnapshot{}, err
	}
	s.data = append([]byte(nil), body[cfgLen:]...)
	return s, nil
}

// compact snapshots the log through appliedIndex: persist the state
// machine image, drop the covered entries, move the log's base. Called
// from the node's event loop with the state machine's serialized state,
// so it sees a consistent point-in-time image.
func (c *core) compact(appliedIndex uint64, smData []byte) error {
	if appliedIndex <= c.snapIndex {
		return nil
	}
	if appliedIndex > c.lastDelivered {
		return fmt.Errorf("raft: cannot compact through %d: only applied through %d", appliedIndex, c.lastDelivered)
	}

	snap := raftSnapshot{
		lastIndex: appliedIndex,
		lastTerm:  c.termAt(appliedIndex),
		config:    c.configAt(appliedIndex),
		data:      smData,
	}
	if err := persistRaftSnapshot(c.cfg.DataDir, snap); err != nil {
		return fmt.Errorf("raft: persisting snapshot: %w", err)
	}

	// The snapshot is durable; everything it covers is now dead weight.
	// Order matters for crash safety: with the snapshot on disk first, a
	// crash before the log rewrite just means some entries exist twice
	// (in the snapshot and the log) — recovery replays from the base and
	// gets the same state.
	base := c.entries[0].Index
	keep := append([]Entry(nil), c.entries[appliedIndex-base+1:]...)
	if err := c.store.rewrite(keep); err != nil {
		return fmt.Errorf("raft: rewriting compacted log: %w", err)
	}
	c.entries = keep
	c.snapIndex, c.snapTerm, c.snapConfig = snap.lastIndex, snap.lastTerm, snap.config
	c.logger.Info("compacted log", "through", appliedIndex, "remaining_entries", len(keep))
	return nil
}

// handleInstallSnapshot is the follower's side of catching up from a
// snapshot: the leader compacted away the entries this node needs, so it
// receives the whole state instead.
func (c *core) handleInstallSnapshot(req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error) {
	if req.GetTerm() > c.term {
		if err := c.stepDown(req.GetTerm()); err != nil {
			return nil, err
		}
	}
	resp := &raftv1.InstallSnapshotResponse{Term: c.term}
	if req.GetTerm() < c.term {
		return resp, nil
	}
	if c.role != follower {
		c.role = follower
		c.votes = nil
	}
	c.leaderID = req.GetLeaderId()
	c.resetElectionTimer()
	c.ticksSinceLeader = 0 // genuine contact from our leader

	idx, term := req.GetLastIncludedIndex(), req.GetLastIncludedTerm()
	if idx <= c.snapIndex || idx <= c.commitIndex {
		return resp, nil // stale: we already have everything it covers
	}

	cfg, err := decodeMembers(req.GetConfig())
	if err != nil {
		return nil, fmt.Errorf("raft: snapshot config: %w", err)
	}
	if err := persistRaftSnapshot(c.cfg.DataDir, raftSnapshot{
		lastIndex: idx, lastTerm: term, config: cfg, data: req.GetData(),
	}); err != nil {
		return nil, fmt.Errorf("raft: persisting installed snapshot: %w", err)
	}

	// If our log continues coherently past the snapshot, keep the suffix
	// (paper §7); otherwise the snapshot replaces everything we had.
	var keep []Entry
	if idx <= c.lastLogIndex() && c.termAt(idx) == term {
		base := c.entries[0].Index
		keep = append([]Entry(nil), c.entries[idx-base+1:]...)
	}
	if err := c.store.rewrite(keep); err != nil {
		return nil, fmt.Errorf("raft: rewriting log for snapshot: %w", err)
	}
	c.entries = keep
	c.snapIndex, c.snapTerm, c.snapConfig = idx, term, cfg
	c.commitIndex = max(c.commitIndex, idx)
	c.lastDelivered = max(c.lastDelivered, idx)
	c.recalcConfig()

	// The node layer must reset the state machine to this image before
	// applying anything after it.
	c.pendingRestore = append([]byte(nil), req.GetData()...)
	c.logger.Info("installed snapshot", "through", idx, "term", term)
	return resp, nil
}

// handleInstallSnapshotResponse advances the follower's frontier past
// the snapshot on the leader.
func (c *core) handleInstallSnapshotResponse(from uint64, req *raftv1.InstallSnapshotRequest, resp *raftv1.InstallSnapshotResponse) error {
	if resp.GetTerm() > c.term {
		return c.stepDown(resp.GetTerm())
	}
	if c.role != leader || req.GetTerm() != c.term {
		return nil
	}
	idx := req.GetLastIncludedIndex()
	if idx > c.matchIndex[from] {
		c.matchIndex[from] = idx
		c.nextIndex[from] = idx + 1
	}
	if c.nextIndex[from] <= c.lastLogIndex() {
		c.sendAppend(from)
	}
	return nil
}

// takeRestore drains a pending state-machine restore (set by snapshot
// installation), mirroring takeOutbox/takeCommitted.
func (c *core) takeRestore() []byte {
	r := c.pendingRestore
	c.pendingRestore = nil
	return r
}
