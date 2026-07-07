package raft

import (
	"fmt"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// Pre-vote (dissertation §9.6) and leadership transfer (§3.10).
//
// Pre-vote fixes a real availability problem: a node that was
// partitioned away keeps timing out and incrementing its term; on
// rejoining, its inflated term forces the healthy leader to step down
// even though the rejoiner can't win (its log is stale). With pre-vote,
// a would-be candidate first asks "would you vote for me?" without
// changing any state — its own term included — and only campaigns for
// real after a majority says yes. The rejoiner's probes fail (peers have
// a live leader) and the cluster never notices it was gone.

// startPreCampaign probes for electability without disturbing anything.
func (c *core) startPreCampaign() error {
	c.preVotes = map[uint64]bool{c.cfg.ID: true}
	c.resetElectionTimer()

	askTerm := c.term + 1
	for _, id := range c.peerIDs() {
		c.outbox = append(c.outbox, outbound{to: id, requestVote: &raftv1.RequestVoteRequest{
			Term:         askTerm,
			CandidateId:  c.cfg.ID,
			LastLogIndex: c.lastLogIndex(),
			LastLogTerm:  c.lastLogTerm(),
			PreVote:      true,
		}})
	}
	return c.maybeWinPreVote() // single-node cluster proceeds immediately
}

// handlePreVote answers a pre-vote probe. Nothing here mutates state:
// no term adoption, no persisted vote, no timer reset.
func (c *core) handlePreVote(req *raftv1.RequestVoteRequest) *raftv1.RequestVoteResponse {
	resp := &raftv1.RequestVoteResponse{Term: c.term}

	// Leader stickiness (CheckQuorum-style): if we've genuinely heard from
	// a live leader within a minimum election timeout, a probing candidate
	// is disrupting a healthy cluster — refuse regardless of its log.
	//
	// The recency test is ticksSinceLeader, NOT electionElapsed: the latter
	// is also reset when this node starts its own pre-campaign or grants a
	// vote, so two survivors of a leader partition would keep resetting it
	// and perpetually veto each other's probes — a liveness bug that stalls
	// failover. ticksSinceLeader is reset only by real leader contact, so
	// once the leader vanishes it climbs on every survivor and the veto
	// correctly lifts.
	if c.leaderID != 0 && c.ticksSinceLeader < c.cfg.ElectionTicksMin {
		return resp
	}
	resp.VoteGranted = req.GetTerm() > c.term &&
		c.logUpToDate(req.GetLastLogIndex(), req.GetLastLogTerm())
	return resp
}

func (c *core) handlePreVoteResponse(from, askedTerm uint64, resp *raftv1.RequestVoteResponse) error {
	if resp.GetTerm() > c.term {
		return c.stepDown(resp.GetTerm())
	}
	if c.role != follower || askedTerm != c.term+1 || c.preVotes == nil {
		return nil // the probe is stale or we've moved on
	}
	if resp.GetVoteGranted() {
		c.preVotes[from] = true
		return c.maybeWinPreVote()
	}
	return nil
}

func (c *core) maybeWinPreVote() error {
	if c.preVotes == nil || len(c.preVotes) < c.majority() {
		return nil
	}
	c.preVotes = nil
	return c.startCampaign()
}

// transferLeadership hands the lead to a specific peer: stop taking
// proposals, make sure the target's log is complete, then tell it to
// campaign immediately (TimeoutNow skips pre-vote and fires before
// anyone else's timeout, so the target wins the race).
func (c *core) transferLeadership(to uint64) error {
	if c.role != leader {
		return &NotLeaderError{LeaderID: c.leaderID}
	}
	if to == c.cfg.ID {
		return nil // already there
	}
	if _, isMember := c.members[to]; !isMember {
		return fmt.Errorf("raft: transfer target %d is not a member", to)
	}
	c.transferTarget = to
	// Abort if the transfer doesn't complete within an election timeout —
	// the target may be down, and the cluster shouldn't stay writeless.
	c.transferTicks = c.cfg.ElectionTicksMax

	if c.matchIndex[to] == c.lastLogIndex() {
		c.sendTimeoutNow(to)
	} else {
		c.sendAppend(to) // finish catching it up first
	}
	return nil
}

func (c *core) sendTimeoutNow(to uint64) {
	c.outbox = append(c.outbox, outbound{to: to, timeoutNow: &raftv1.TimeoutNowRequest{
		Term:     c.term,
		LeaderId: c.cfg.ID,
	}})
	c.logger.Info("asked peer to take over leadership", "target", to, "term", c.term)
}

// handleTimeoutNow: the current leader sanctioned this takeover, so
// campaign immediately — no pre-vote, no waiting for a timeout.
func (c *core) handleTimeoutNow(req *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error) {
	if req.GetTerm() > c.term {
		if err := c.stepDown(req.GetTerm()); err != nil {
			return nil, err
		}
	}
	resp := &raftv1.TimeoutNowResponse{}
	if req.GetTerm() < c.term {
		return resp, nil
	}
	return resp, c.startCampaign()
}
