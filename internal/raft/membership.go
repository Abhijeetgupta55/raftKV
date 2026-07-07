package raft

import (
	"encoding/binary"
	"fmt"
	"sort"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// Cluster membership changes one server at a time, the dissertation's
// single-server approach (§4.1). Why that is safe where an arbitrary
// change is not: adding or removing ONE server means any majority of the
// old configuration and any majority of the new one overlap in at least
// one node, so two disjoint quorums — two leaders — cannot form during
// the transition. Changing several servers at once breaks that overlap,
// which is exactly the failure joint consensus exists to prevent; with
// one-at-a-time changes the overlap is structural and no joint phase is
// needed. The one-at-a-time rule is enforced here: a new change is
// refused while a previous one is still uncommitted.
//
// A config entry takes effect when it is APPENDED, not when it commits
// (§4.1): nodes always operate on the latest config in their log. The
// consequence handled in truncateAndAppend: if a conflicting suffix
// containing a config entry is truncated away, the active config must be
// recomputed from what remains.

const (
	confChangeAdd    byte = 1
	confChangeRemove byte = 2
)

type confChange struct {
	op   byte
	id   uint64
	addr string
}

func encodeConfChange(cc confChange) []byte {
	b := make([]byte, 0, 1+8+4+len(cc.addr))
	b = append(b, cc.op)
	b = binary.LittleEndian.AppendUint64(b, cc.id)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(cc.addr)))
	return append(b, cc.addr...)
}

func decodeConfChange(b []byte) (confChange, error) {
	if len(b) < 1+8+4 {
		return confChange{}, fmt.Errorf("raft: config change truncated: %d bytes", len(b))
	}
	cc := confChange{op: b[0], id: binary.LittleEndian.Uint64(b[1:])}
	n := binary.LittleEndian.Uint32(b[9:])
	if uint64(len(b)-13) != uint64(n) {
		return confChange{}, fmt.Errorf("raft: config change address length mismatch")
	}
	cc.addr = string(b[13:])
	if cc.op != confChangeAdd && cc.op != confChangeRemove {
		return confChange{}, fmt.Errorf("raft: unknown config change op %d", cc.op)
	}
	return cc, nil
}

// encodeMembers serializes a full membership table (for snapshots), in
// sorted order so the bytes are deterministic.
func encodeMembers(m map[uint64]string) []byte {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	b := binary.LittleEndian.AppendUint32(nil, uint32(len(ids)))
	for _, id := range ids {
		b = binary.LittleEndian.AppendUint64(b, id)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(m[id])))
		b = append(b, m[id]...)
	}
	return b
}

func decodeMembers(b []byte) (map[uint64]string, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("raft: members table truncated")
	}
	count := binary.LittleEndian.Uint32(b)
	b = b[4:]
	m := make(map[uint64]string, count)
	for i := uint32(0); i < count; i++ {
		if len(b) < 12 {
			return nil, fmt.Errorf("raft: members table truncated at entry %d", i)
		}
		id := binary.LittleEndian.Uint64(b)
		alen := binary.LittleEndian.Uint32(b[8:])
		b = b[12:]
		if uint64(len(b)) < uint64(alen) {
			return nil, fmt.Errorf("raft: members table truncated in address %d", i)
		}
		m[id] = string(b[:alen])
		b = b[alen:]
	}
	return m, nil
}

// ProposeConfChange starts a single-server membership change. It returns
// the log position the change was appended at; commitment is observed
// like any proposal.
func (c *core) proposeConfChange(cc confChange) (index, term uint64, err error) {
	if c.role != leader {
		return 0, 0, &NotLeaderError{LeaderID: c.leaderID}
	}
	// The one-at-a-time rule: a second change while the first is
	// uncommitted could remove the quorum overlap that makes
	// single-server changes safe.
	if c.lastConfigIndex > c.commitIndex {
		return 0, 0, fmt.Errorf("raft: a membership change is already in flight (index %d, commit %d)",
			c.lastConfigIndex, c.commitIndex)
	}
	if cc.op == confChangeAdd {
		if addr, exists := c.members[cc.id]; exists && addr == cc.addr {
			return 0, 0, fmt.Errorf("raft: node %d is already a member", cc.id)
		}
	} else if _, exists := c.members[cc.id]; !exists {
		return 0, 0, fmt.Errorf("raft: node %d is not a member", cc.id)
	}

	index, term, err = c.propose(encodeConfChange(cc), raftv1.EntryType_ENTRY_TYPE_CONFIG)
	if err != nil {
		return 0, 0, err
	}
	// Effective on append: this node (the leader) uses the new config
	// for its very next quorum computation.
	c.applyConfChange(cc, index)
	return index, term, nil
}

func (c *core) applyConfChange(cc confChange, index uint64) {
	switch cc.op {
	case confChangeAdd:
		c.members[cc.id] = cc.addr
		if c.role == leader {
			if _, tracked := c.nextIndex[cc.id]; !tracked {
				c.nextIndex[cc.id] = c.lastLogIndex() + 1
				c.matchIndex[cc.id] = 0
			}
		}
	case confChangeRemove:
		delete(c.members, cc.id)
		delete(c.nextIndex, cc.id)
		delete(c.matchIndex, cc.id)
	}
	c.lastConfigIndex = index
	c.configVersion++
	c.logger.Info("config change applied", "op", cc.op, "member", cc.id, "members", len(c.members))
}

// noteAppendedConfigs applies any config entries in a freshly appended
// slice (the follower path; the leader applies in proposeConfChange).
func (c *core) noteAppendedConfigs(entries []Entry) {
	for _, e := range entries {
		if e.Type != raftv1.EntryType_ENTRY_TYPE_CONFIG {
			continue
		}
		cc, err := decodeConfChange(e.Command)
		if err != nil {
			c.logger.Error("undecodable config entry", "index", e.Index, "err", err)
			continue
		}
		c.applyConfChange(cc, e.Index)
	}
}

// recalcConfig rebuilds the active config from the snapshot's membership
// plus every config entry still in the log — the recovery path after a
// truncation may have discarded config entries the current table
// reflected.
func (c *core) recalcConfig() {
	m := make(map[uint64]string, len(c.snapConfig))
	for id, addr := range c.snapConfig {
		m[id] = addr
	}
	c.lastConfigIndex = 0
	for _, e := range c.entries {
		if e.Type != raftv1.EntryType_ENTRY_TYPE_CONFIG {
			continue
		}
		cc, err := decodeConfChange(e.Command)
		if err != nil {
			continue
		}
		switch cc.op {
		case confChangeAdd:
			m[cc.id] = cc.addr
		case confChangeRemove:
			delete(m, cc.id)
		}
		c.lastConfigIndex = e.Index
	}
	c.members = m
	c.configVersion++
}

// configAt reconstructs the membership as of a specific log index, for
// stamping into a snapshot taken at that index.
func (c *core) configAt(index uint64) map[uint64]string {
	m := make(map[uint64]string, len(c.snapConfig))
	for id, addr := range c.snapConfig {
		m[id] = addr
	}
	for _, e := range c.entries {
		if e.Index > index {
			break
		}
		if e.Type != raftv1.EntryType_ENTRY_TYPE_CONFIG {
			continue
		}
		if cc, err := decodeConfChange(e.Command); err == nil {
			switch cc.op {
			case confChangeAdd:
				m[cc.id] = cc.addr
			case confChangeRemove:
				delete(m, cc.id)
			}
		}
	}
	return m
}
