package raft

import (
	"fmt"
)

// ReadIndex (dissertation §6.4): linearizable reads without writing to
// the log.
//
// Why the naive read — just answering from the leader's state machine —
// is wrong: a leader partitioned away from the majority doesn't know it
// has been deposed. A new leader on the other side may have committed
// newer writes, so the old leader would serve stale data as if it were
// current. That is a linearizability violation: a read returning a value
// older than a write that completed before the read began.
//
// The fix has three steps:
//  1. Record the current commitIndex as the read's index — but only once
//     this leader has committed an entry in ITS OWN term (the term-start
//     no-op guarantees one exists); before that, commitIndex might not
//     yet cover a previous leader's committed entries.
//  2. Prove we are STILL the leader by hearing from a majority — counting
//     only acks to heartbeats sent after the read arrived (the seq field
//     separates fresh acks from stale in-flight ones).
//  3. Wait until the state machine has applied through the read index,
//     then serve locally.
//
// ErrLeaderNotReady and dropped reads on step-down are retryable errors;
// the client (or service layer) retries against the current leader.

// ErrLeaderNotReady means the new leader's term-start no-op hasn't
// committed yet, so it cannot vouch for the completeness of its
// commitIndex. Momentary; retry.
var ErrLeaderNotReady = fmt.Errorf("raft: leader has not committed an entry in its term yet")

type pendingRead struct {
	id     uint64
	index  uint64          // commitIndex at registration
	minSeq uint64          // acks must answer sends at or after this
	acks   map[uint64]bool // peers whose fresh acks we've counted
}

// readOutcome is delivered to the node layer once a read barrier
// resolves: confirmed (serve once applied ≥ index) or aborted (lost
// leadership mid-confirmation; the client must retry).
type readOutcome struct {
	id        uint64
	index     uint64
	confirmed bool
}

// requestRead registers a linearizable read barrier. Caller must be the
// leader; confirmation arrives asynchronously via takeReadOutcomes.
func (c *core) requestRead() (id uint64, err error) {
	if c.role != leader {
		return 0, &NotLeaderError{LeaderID: c.leaderID}
	}
	if c.termAt(c.commitIndex) != c.term {
		return 0, ErrLeaderNotReady
	}

	c.readCounter++
	r := pendingRead{
		id:     c.readCounter,
		index:  c.commitIndex,
		minSeq: c.appendSeq + 1, // only sends from this moment on count
		acks:   map[uint64]bool{c.cfg.ID: true},
	}
	c.pendingReads = append(c.pendingReads, r)

	// A single-node cluster is its own majority; otherwise round-trip
	// heartbeats to collect fresh acks.
	if !c.confirmReads() {
		c.broadcastAppends()
	}
	return r.id, nil
}

// noteFreshAck records a majority-proving ack and resolves any reads it
// completes. Called from handleAppendResponse for successful responses
// whose seq is fresh enough.
func (c *core) noteFreshAck(from, seq uint64) {
	touched := false
	for i := range c.pendingReads {
		if seq >= c.pendingReads[i].minSeq {
			c.pendingReads[i].acks[from] = true
			touched = true
		}
	}
	if touched {
		c.confirmReads()
	}
}

// confirmReads moves every pending read that reached a majority into the
// outcome queue. Returns true if none remain pending.
func (c *core) confirmReads() bool {
	remaining := c.pendingReads[:0]
	for _, r := range c.pendingReads {
		if len(r.acks) >= c.majority() {
			c.readOutcomes = append(c.readOutcomes, readOutcome{id: r.id, index: r.index, confirmed: true})
		} else {
			remaining = append(remaining, r)
		}
	}
	c.pendingReads = remaining
	return len(c.pendingReads) == 0
}

// abortPendingReads fails every in-flight read barrier; called on
// step-down, where confirmation can no longer be honestly given.
func (c *core) abortPendingReads() {
	for _, r := range c.pendingReads {
		c.readOutcomes = append(c.readOutcomes, readOutcome{id: r.id, confirmed: false})
	}
	c.pendingReads = nil
}

// takeReadOutcomes drains resolved read barriers for the node layer.
func (c *core) takeReadOutcomes() []readOutcome {
	out := c.readOutcomes
	c.readOutcomes = nil
	return out
}
