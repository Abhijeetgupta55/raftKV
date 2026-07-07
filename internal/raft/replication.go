package raft

import (
	"errors"
	"fmt"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// errPersistFailure marks errors after which the node cannot safely
// continue (unlike ordinary rejections, which are answers). The event
// loop halts the node when it sees one.
var errPersistFailure = errors.New("raft: persist failure")

// NotLeaderError rejects an operation that only the leader may perform.
// LeaderID is a routing hint (0 when unknown, e.g. mid-election).
type NotLeaderError struct {
	LeaderID uint64
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not the leader (leader hint: node %d)", e.LeaderID)
}

// maxAppendBytes caps the command payload of one AppendEntries so a
// catching-up follower is fed in gRPC-sized bites; at least one entry is
// always sent regardless.
const maxAppendBytes = 1 << 20

// propose appends a command to the leader's log and starts replicating
// it. It returns the entry's (index, term) — the caller learns the
// outcome by watching for that index to commit with that term; if a
// different term's entry commits there instead, leadership was lost and
// the command was discarded, never applied.
func (c *core) propose(command []byte, typ raftv1.EntryType) (index, term uint64, err error) {
	if c.role != leader {
		return 0, 0, &NotLeaderError{LeaderID: c.leaderID}
	}
	if c.transferTarget != 0 {
		// Mid-transfer the leadership is already promised away; taking
		// new proposals would just have them truncated by the successor.
		return 0, 0, &NotLeaderError{LeaderID: c.transferTarget}
	}
	e := Entry{Term: c.term, Index: c.lastLogIndex() + 1, Type: typ, Command: command}
	// The leader's own log write is fsynced before any replication
	// message leaves (outbox drains after return), so the leader never
	// counts itself toward a majority for an entry it could forget.
	if err := c.store.append(e); err != nil {
		return 0, 0, fmt.Errorf("%w: persisting proposal: %v", errPersistFailure, err)
	}
	c.entries = append(c.entries, e)
	c.broadcastAppends()
	c.maybeAdvanceCommit() // a single-node cluster commits immediately
	return e.Index, e.Term, nil
}

// broadcastAppends sends every peer its next slice of the log — a pure
// heartbeat when the peer is caught up.
func (c *core) broadcastAppends() {
	for _, id := range c.peerIDs() {
		c.sendAppend(id)
	}
}

func (c *core) sendAppend(peer uint64) {
	next := c.nextIndex[peer]
	if next == 0 {
		next = 1
	}

	// The entries this peer needs are gone — compacted into the
	// snapshot — so it gets the whole state instead.
	if next <= c.snapIndex {
		snap, err := loadRaftSnapshot(c.cfg.DataDir)
		if err != nil {
			c.logger.Error("cannot load snapshot for lagging peer", "peer", peer, "err", err)
			return
		}
		c.outbox = append(c.outbox, outbound{to: peer, installSnapshot: &raftv1.InstallSnapshotRequest{
			Term:              c.term,
			LeaderId:          c.cfg.ID,
			LastIncludedIndex: snap.lastIndex,
			LastIncludedTerm:  snap.lastTerm,
			Data:              snap.data,
			Config:            encodeMembers(snap.config),
		}})
		return
	}

	prev := next - 1
	c.appendSeq++
	req := &raftv1.AppendEntriesRequest{
		Term:         c.term,
		LeaderId:     c.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  c.termAt(prev),
		LeaderCommit: c.commitIndex,
		Seq:          c.appendSeq,
	}
	if last := c.lastLogIndex(); next <= last && len(c.entries) > 0 {
		base := c.entries[0].Index
		payload := 0
		for i := next; i <= last; i++ {
			e := c.entries[i-base]
			req.Entries = append(req.Entries, e.toProto())
			payload += len(e.Command)
			if payload >= maxAppendBytes {
				break
			}
		}
	}
	c.outbox = append(c.outbox, outbound{to: peer, appendEntries: req})
}

// handleAppendEntries is the follower's side of replication: the
// log-matching check, conflict hints for fast repair, idempotent entry
// append, and commit advancement.
func (c *core) handleAppendEntries(req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	if req.GetTerm() > c.term {
		if err := c.stepDown(req.GetTerm()); err != nil {
			return nil, err
		}
	}

	resp := &raftv1.AppendEntriesResponse{Term: c.term, Seq: req.GetSeq()}
	if req.GetTerm() < c.term {
		return resp, nil
	}

	// An AppendEntries at our own term is proof of a legitimate current
	// leader: election safety guarantees at most one per term, so a
	// candidate concedes and a follower refreshes its patience — even if
	// the log check below rejects this particular request.
	if c.role != follower {
		c.role = follower
		c.votes = nil
		c.logger.Info("recognized leader", "term", c.term, "leader", req.GetLeaderId())
	}
	c.leaderID = req.GetLeaderId()
	c.resetElectionTimer()
	c.ticksSinceLeader = 0 // genuine contact from our leader

	// Log-matching check (the induction step that keeps logs identical up
	// to any shared point). On failure, tell the leader where to retry:
	// a too-short log points just past its own end; a term mismatch names
	// the intruding term and where it starts, so the leader can skip the
	// whole term instead of probing entry by entry.
	prevIndex, prevTerm := req.GetPrevLogIndex(), req.GetPrevLogTerm()
	if prevIndex > 0 {
		if prevIndex > c.lastLogIndex() {
			resp.ConflictIndex = c.lastLogIndex() + 1
			return resp, nil
		}
		if t := c.termAt(prevIndex); t != prevTerm {
			resp.ConflictTerm = t
			resp.ConflictIndex = c.firstIndexOfTerm(t, prevIndex)
			return resp, nil
		}
	}

	// Find the first genuinely new entry. Everything before it is a
	// retransmission we already hold (same index and term ⇒ same entry,
	// by the log-matching property), so a duplicated or reordered RPC
	// never rewrites — let alone re-fsyncs — anything.
	newFrom := -1
	for i, pe := range req.GetEntries() {
		if pe.GetIndex() > c.lastLogIndex() || c.termAt(pe.GetIndex()) != pe.GetTerm() {
			newFrom = i
			break
		}
	}
	if newFrom >= 0 {
		toAppend := make([]Entry, 0, len(req.GetEntries())-newFrom)
		for _, pe := range req.GetEntries()[newFrom:] {
			toAppend = append(toAppend, entryFromProto(pe))
		}
		if err := c.truncateAndAppend(toAppend); err != nil {
			return nil, err
		}
	}

	// Advance commit to what the leader says, bounded by the last entry
	// this request lets us verify we share with it. Using our own log
	// length instead would be wrong: a stale heartbeat's leaderCommit may
	// cover entries this exchange said nothing about.
	if lastNew := prevIndex + uint64(len(req.GetEntries())); req.GetLeaderCommit() > c.commitIndex {
		c.commitIndex = min(req.GetLeaderCommit(), lastNew)
		c.deliverCommitted()
	}

	resp.Success = true
	return resp, nil
}

// truncateAndAppend replaces any conflicting suffix with the leader's
// entries. The on-disk log applies the same supersede rule during
// replay, so memory and disk cannot diverge across a crash.
func (c *core) truncateAndAppend(entries []Entry) error {
	first := entries[0].Index
	if first <= c.commitIndex {
		// The leader is contradicting an entry we know is committed.
		// Log matching makes this impossible in a correct cluster, so
		// refuse loudly instead of corrupting state.
		return fmt.Errorf("raft: leader tried to truncate committed entry %d (commit %d)", first, c.commitIndex)
	}
	truncated := false
	if len(c.entries) > 0 && first <= c.lastLogIndex() {
		cut := c.entries[first-c.entries[0].Index:]
		for _, e := range cut {
			if e.Type == raftv1.EntryType_ENTRY_TYPE_CONFIG {
				truncated = true // a membership change is being undone
			}
		}
		c.entries = c.entries[:first-c.entries[0].Index]
	}
	if err := c.store.append(entries...); err != nil {
		return fmt.Errorf("raft: persisting entries: %w", err)
	}
	c.entries = append(c.entries, entries...)

	// Config entries take effect on append (membership.go); a truncation
	// that discarded one forces a full rebuild from the snapshot's
	// config forward.
	if truncated {
		c.recalcConfig()
	} else {
		c.noteAppendedConfigs(entries)
	}
	return nil
}

// handleAppendResponse processes a follower's answer on the leader:
// advance the replication frontier on success, back up and retry on
// rejection.
func (c *core) handleAppendResponse(from uint64, req *raftv1.AppendEntriesRequest, resp *raftv1.AppendEntriesResponse) error {
	if resp.GetTerm() > c.term {
		return c.stepDown(resp.GetTerm())
	}
	if c.role != leader || req.GetTerm() != c.term {
		return nil // an answer to a leadership we no longer hold
	}

	if resp.GetSuccess() {
		c.noteFreshAck(from, resp.GetSeq()) // ReadIndex leadership proof
		m := req.GetPrevLogIndex() + uint64(len(req.GetEntries()))
		if m > c.matchIndex[from] {
			c.matchIndex[from] = m
			c.nextIndex[from] = m + 1
			c.maybeAdvanceCommit()
		}
		// A pending leadership transfer fires the moment the target's
		// log is complete.
		if from == c.transferTarget && c.matchIndex[from] == c.lastLogIndex() {
			c.sendTimeoutNow(from)
		}
		if c.nextIndex[from] <= c.lastLogIndex() {
			c.sendAppend(from) // more backlog: keep feeding
		}
		return nil
	}

	// Rejection: jump nextIndex using the follower's conflict hints. If
	// we hold the conflicting term ourselves, our entries for it are
	// necessarily compatible (log matching), so retry from just past our
	// last entry of that term; otherwise skip the follower's entire run
	// of that term.
	next := resp.GetConflictIndex()
	if t := resp.GetConflictTerm(); t != 0 {
		if last := c.lastIndexOfTerm(t); last != 0 {
			next = last + 1
		}
	}
	if next == 0 {
		next = 1
	}
	if next < c.nextIndex[from] {
		c.nextIndex[from] = next
	} else {
		// A stale or reordered rejection must never move the frontier
		// forward past entries the follower hasn't confirmed.
		c.nextIndex[from] = max(1, c.nextIndex[from]-1)
	}
	c.sendAppend(from)
	return nil
}

// maybeAdvanceCommit advances commitIndex to the highest index that (a)
// a majority stores and (b) belongs to the CURRENT term. Condition (b)
// is the Figure-8 rule: a prior-term entry on a majority can still be
// overwritten by an even newer leader, so such entries commit only
// transitively, shielded by a current-term entry above them — which is
// why every new leader immediately appends a no-op.
func (c *core) maybeAdvanceCommit() {
	for n := c.lastLogIndex(); n > c.commitIndex && c.termAt(n) == c.term; n-- {
		votes := 0
		if c.isMember() {
			votes++ // the leader itself, unless it's being removed
		}
		for _, id := range c.peerIDs() {
			if c.matchIndex[id] >= n {
				votes++
			}
		}
		if votes >= c.majority() {
			c.commitIndex = n
			c.deliverCommitted()
			return
		}
	}
}

// deliverCommitted queues newly committed entries for the node layer to
// apply, in order, exactly once per core lifetime.
func (c *core) deliverCommitted() {
	if len(c.entries) == 0 {
		return
	}
	base := c.entries[0].Index
	for i := c.lastDelivered + 1; i <= c.commitIndex; i++ {
		e := c.entries[i-base]
		c.committed = append(c.committed, e)

		// A committed removal of this node while it leads: keep serving
		// until the change is safe (committed — that's now), then get
		// out of the way (§4.2.2 of the dissertation).
		if e.Type == raftv1.EntryType_ENTRY_TYPE_CONFIG && c.role == leader && !c.isMember() {
			c.role = follower
			c.leaderID = 0
			c.transferTarget = 0
			c.abortPendingReads()
			c.logger.Info("stepped down: removed from the cluster", "term", c.term)
		}
	}
	if c.commitIndex > c.lastDelivered {
		c.lastDelivered = c.commitIndex
	}
}

// takeCommitted drains the queue of committed-but-unapplied entries,
// mirroring takeOutbox.
func (c *core) takeCommitted() []Entry {
	out := c.committed
	c.committed = nil
	return out
}

func (c *core) firstIndexOfTerm(term, from uint64) uint64 {
	i := from
	for i > 1 && c.termAt(i-1) == term {
		i--
	}
	return i
}

func (c *core) lastIndexOfTerm(term uint64) uint64 {
	for i := c.lastLogIndex(); i > 0; i-- {
		switch t := c.termAt(i); {
		case t == term:
			return i
		case t < term || t == 0:
			return 0 // terms only decrease going back; it's not here
		}
	}
	return 0
}
