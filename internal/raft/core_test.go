package raft

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// Every test here is deterministic: the core is a plain object, time is
// tick() called by hand, and RPCs are direct method calls. No goroutines,
// no sleeps, no network.

func newTestCore(t *testing.T) *core {
	t.Helper()
	c, err := newCore(Config{
		ID:      1,
		PeerIDs: []uint64{2, 3},
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, 42)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.close() })
	return c
}

// tickToCampaign advances time until the node starts a new election,
// detected by the term incrementing (a single-node cluster wins its
// campaign instantly, so checking for the candidate role would miss it).
func tickToCampaign(t *testing.T, c *core) {
	t.Helper()
	start := c.term
	for i := 0; i < c.cfg.ElectionTicksMax+1; i++ {
		if err := c.tick(); err != nil {
			t.Fatal(err)
		}
		if c.term > start {
			return
		}
	}
	t.Fatal("node never started a campaign")
}

func voteReq(term, candidateID, lastLogIndex, lastLogTerm uint64) *raftv1.RequestVoteRequest {
	return &raftv1.RequestVoteRequest{
		Term: term, CandidateId: candidateID,
		LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm,
	}
}

func TestFollowerGrantsVoteAndPersistsIt(t *testing.T) {
	c := newTestCore(t)

	resp, err := c.handleRequestVote(voteReq(1, 2, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetVoteGranted() || resp.GetTerm() != 1 {
		t.Fatalf("vote: granted=%v term=%d, want true, 1", resp.GetVoteGranted(), resp.GetTerm())
	}

	// The grant must already be durable: reload from disk.
	term, votedFor, err := loadTermAndVote(c.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if term != 1 || votedFor != 2 {
		t.Fatalf("persisted term=%d votedFor=%d, want 1, 2", term, votedFor)
	}
}

func TestOneVotePerTerm(t *testing.T) {
	c := newTestCore(t)

	if resp, _ := c.handleRequestVote(voteReq(1, 2, 0, 0)); !resp.GetVoteGranted() {
		t.Fatal("first candidate denied")
	}
	if resp, _ := c.handleRequestVote(voteReq(1, 3, 0, 0)); resp.GetVoteGranted() {
		t.Fatal("second candidate granted a vote in the same term — two leaders possible")
	}
	// Re-asking by the same candidate (retried RPC) stays granted.
	if resp, _ := c.handleRequestVote(voteReq(1, 2, 0, 0)); !resp.GetVoteGranted() {
		t.Fatal("vote retry by the original candidate denied")
	}
}

func TestVoteDeniedToLowerTerm(t *testing.T) {
	c := newTestCore(t)
	if err := c.stepDown(5); err != nil {
		t.Fatal(err)
	}

	resp, _ := c.handleRequestVote(voteReq(3, 2, 0, 0))
	if resp.GetVoteGranted() {
		t.Fatal("granted a vote to a candidate from the past")
	}
	if resp.GetTerm() != 5 {
		t.Fatalf("response term %d, want 5 so the candidate steps down", resp.GetTerm())
	}
}

// TestElectionRestriction is §5.4.1: a candidate whose log is behind ours
// must not win our vote, or a leader could be elected without every
// committed entry.
func TestElectionRestriction(t *testing.T) {
	c := newTestCore(t)
	// Our log: two entries, last term 3.
	if err := c.store.append(Entry{Term: 2, Index: 1}, Entry{Term: 3, Index: 2}); err != nil {
		t.Fatal(err)
	}
	c.entries = []Entry{{Term: 2, Index: 1}, {Term: 3, Index: 2}}

	cases := []struct {
		name                      string
		lastLogIndex, lastLogTerm uint64
		want                      bool
	}{
		{"older last term", 5, 2, false},
		{"same term, shorter log", 1, 3, false},
		{"same term, same length", 2, 3, true},
		{"same term, longer log", 3, 3, true},
		{"newer last term, shorter log", 1, 4, true},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Fresh term each case so votedFor never interferes.
			resp, err := c.handleRequestVote(voteReq(uint64(10+i), 2, tc.lastLogIndex, tc.lastLogTerm))
			if err != nil {
				t.Fatal(err)
			}
			if resp.GetVoteGranted() != tc.want {
				t.Fatalf("granted=%v, want %v", resp.GetVoteGranted(), tc.want)
			}
		})
	}
}

func TestHigherTermAdoptedEvenWhenVoteDenied(t *testing.T) {
	c := newTestCore(t)
	c.entries = []Entry{{Term: 3, Index: 1}}

	resp, err := c.handleRequestVote(voteReq(7, 2, 1, 1)) // stale log, new term
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetVoteGranted() {
		t.Fatal("stale-logged candidate granted a vote")
	}
	if c.term != 7 {
		t.Fatalf("term %d after seeing term 7, want 7", c.term)
	}
	if term, _, _ := loadTermAndVote(c.cfg.DataDir); term != 7 {
		t.Fatalf("persisted term %d, want 7", term)
	}
}

func TestDeniedVoteDoesNotResetElectionTimer(t *testing.T) {
	c := newTestCore(t)
	c.entries = []Entry{{Term: 3, Index: 1}}

	for i := 0; i < 5; i++ {
		if err := c.tick(); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := c.electionElapsed

	c.handleRequestVote(voteReq(7, 2, 1, 1)) // denied: stale log
	if c.electionElapsed != elapsed {
		t.Fatal("a denied candidate reset our election timer — stale nodes could suppress elections forever")
	}
}

func TestElectionTimeoutStartsCampaign(t *testing.T) {
	c := newTestCore(t)
	tickToCampaign(t, c)

	if c.term != 1 || c.votedFor != 1 {
		t.Fatalf("campaign state: term=%d votedFor=%d, want 1, 1", c.term, c.votedFor)
	}
	if term, votedFor, _ := loadTermAndVote(c.cfg.DataDir); term != 1 || votedFor != 1 {
		t.Fatalf("candidacy not durable: persisted term=%d votedFor=%d", term, votedFor)
	}

	out := c.takeOutbox()
	if len(out) != 2 {
		t.Fatalf("sent %d messages, want RequestVote to both peers", len(out))
	}
	for _, o := range out {
		if o.requestVote == nil {
			t.Fatalf("expected a RequestVote, got %+v", o)
		}
		if o.requestVote.GetTerm() != 1 || o.requestVote.GetCandidateId() != 1 {
			t.Fatalf("bad RequestVote: %+v", o.requestVote)
		}
	}
}

func TestCandidateWinsWithMajority(t *testing.T) {
	c := newTestCore(t)
	tickToCampaign(t, c)
	c.takeOutbox()

	// One grant + our own vote = 2 of 3.
	err := c.handleVoteResponse(2, 1, &raftv1.RequestVoteResponse{Term: 1, VoteGranted: true})
	if err != nil {
		t.Fatal(err)
	}
	if c.role != leader {
		t.Fatalf("role %v after majority, want leader", c.role)
	}

	out := c.takeOutbox()
	if len(out) != 2 || out[0].appendEntries == nil {
		t.Fatalf("new leader must heartbeat immediately, sent %+v", out)
	}
}

func TestDeniedVotesDoNotElect(t *testing.T) {
	c := newTestCore(t)
	tickToCampaign(t, c)

	c.handleVoteResponse(2, 1, &raftv1.RequestVoteResponse{Term: 1, VoteGranted: false})
	c.handleVoteResponse(3, 1, &raftv1.RequestVoteResponse{Term: 1, VoteGranted: false})
	if c.role != follower && c.role != candidate {
		t.Fatalf("role %v, never leader without a majority", c.role)
	}
	if c.role == leader {
		t.Fatal("elected without a majority")
	}
}

func TestStaleCampaignResponsesIgnored(t *testing.T) {
	c := newTestCore(t)
	tickToCampaign(t, c) // campaign at term 1
	tickToCampaign(t, c) // split-vote retry: term 2
	if c.term != 2 {
		t.Fatalf("term %d after two campaigns, want 2", c.term)
	}

	// A straggling grant from the term-1 campaign arrives now.
	c.handleVoteResponse(2, 1, &raftv1.RequestVoteResponse{Term: 1, VoteGranted: true})
	if c.role == leader {
		t.Fatal("won term-2 election with a term-1 vote")
	}
}

func TestCandidateStepsDownOnHigherTermResponse(t *testing.T) {
	c := newTestCore(t)
	tickToCampaign(t, c)

	c.handleVoteResponse(2, 1, &raftv1.RequestVoteResponse{Term: 9, VoteGranted: false})
	if c.role != follower || c.term != 9 {
		t.Fatalf("role=%v term=%d, want follower at term 9", c.role, c.term)
	}
}

func TestHeartbeatResetsElectionTimer(t *testing.T) {
	c := newTestCore(t)

	// Keep the node just shy of its timeout with periodic heartbeats.
	for cycle := 0; cycle < 10; cycle++ {
		for i := 0; i < c.cfg.ElectionTicksMin-1; i++ {
			if err := c.tick(); err != nil {
				t.Fatal(err)
			}
		}
		resp, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{Term: 1, LeaderId: 2})
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetSuccess() {
			t.Fatal("heartbeat rejected")
		}
	}
	if c.role != follower {
		t.Fatalf("role %v despite live leader, want follower", c.role)
	}
	if c.leaderID != 2 {
		t.Fatalf("leaderID %d, want 2", c.leaderID)
	}
}

func TestStaleLeaderHeartbeatRejected(t *testing.T) {
	c := newTestCore(t)
	c.stepDown(5)

	resp, _ := c.handleAppendEntries(&raftv1.AppendEntriesRequest{Term: 3, LeaderId: 2})
	if resp.GetSuccess() {
		t.Fatal("accepted a heartbeat from a deposed leader")
	}
	if resp.GetTerm() != 5 {
		t.Fatalf("response term %d, want 5", resp.GetTerm())
	}
}

func TestCandidateConcedesToLeaderInSameTerm(t *testing.T) {
	c := newTestCore(t)
	tickToCampaign(t, c) // candidate at term 1

	resp, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{Term: 1, LeaderId: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetSuccess() || c.role != follower || c.leaderID != 3 {
		t.Fatalf("candidate did not concede: role=%v leader=%d", c.role, c.leaderID)
	}
}

func TestLeaderHeartbeatsPeriodically(t *testing.T) {
	c := newTestCore(t)
	tickToCampaign(t, c)
	c.handleVoteResponse(2, 1, &raftv1.RequestVoteResponse{Term: 1, VoteGranted: true})
	c.takeOutbox() // discard election traffic + victory heartbeat

	for i := 0; i < c.cfg.HeartbeatTicks; i++ {
		if err := c.tick(); err != nil {
			t.Fatal(err)
		}
	}
	out := c.takeOutbox()
	if len(out) != 2 || out[0].appendEntries == nil || out[1].appendEntries == nil {
		t.Fatalf("expected heartbeats to both peers, got %+v", out)
	}
}

func TestElectionTimeoutStaysWithinBounds(t *testing.T) {
	c := newTestCore(t)
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		c.resetElectionTimer()
		if c.electionTimeout < c.cfg.ElectionTicksMin || c.electionTimeout >= c.cfg.ElectionTicksMax {
			t.Fatalf("timeout %d outside [%d, %d)", c.electionTimeout, c.cfg.ElectionTicksMin, c.cfg.ElectionTicksMax)
		}
		seen[c.electionTimeout] = true
	}
	if len(seen) < 5 {
		t.Fatalf("only %d distinct timeouts in 200 resets — not randomized enough to break split votes", len(seen))
	}
}

func TestSingleNodeClusterElectsItself(t *testing.T) {
	c, err := newCore(Config{
		ID:      1,
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, 7)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.close() })

	tickToCampaign(t, c)
	if c.role != leader {
		t.Fatalf("single-node cluster: role %v, want leader", c.role)
	}
}

func TestTermAndVoteSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{ID: 1, PeerIDs: []uint64{2, 3}, DataDir: dir, Logger: logger}

	c, err := newCore(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.handleRequestVote(voteReq(4, 3, 0, 0)); err != nil {
		t.Fatal(err)
	}
	c.close()

	c2, err := newCore(cfg, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c2.close() })
	if c2.term != 4 || c2.votedFor != 3 {
		t.Fatalf("restarted with term=%d votedFor=%d, want 4, 3", c2.term, c2.votedFor)
	}
	// The revived node must refuse a different candidate in that term.
	if resp, _ := c2.handleRequestVote(voteReq(4, 2, 0, 0)); resp.GetVoteGranted() {
		t.Fatal("voted twice in term 4 across a restart — election safety broken")
	}
}

func TestPersistFailureSurfacesAsError(t *testing.T) {
	c := newTestCore(t)
	c.saveState = func(term, votedFor uint64) error {
		return errors.New("disk on fire")
	}

	if _, err := c.handleRequestVote(voteReq(1, 2, 0, 0)); err == nil {
		t.Fatal("vote granted without durable state")
	}
	for i := 0; i < c.cfg.ElectionTicksMax+1; i++ {
		if err := c.tick(); err != nil {
			return // campaign persistence failed loudly, as it must
		}
	}
	t.Fatal("campaign started without durable candidacy")
}
