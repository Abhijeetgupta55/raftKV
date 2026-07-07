package raft

import (
	"bytes"
	"errors"
	"testing"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// makeLeader elects the test core (node 1) leader of its 3-node cluster
// and drains the election traffic.
func makeLeader(t *testing.T, c *core) {
	t.Helper()
	tickToCampaign(t, c)
	if err := c.handleVoteResponse(2, c.term, &raftv1.RequestVoteResponse{Term: c.term, VoteGranted: true}); err != nil {
		t.Fatal(err)
	}
	if c.role != leader {
		t.Fatalf("role %v after majority vote, want leader", c.role)
	}
	c.takeOutbox()
}

// appendReqTo digs the most recent AppendEntries for a peer out of the
// outbox, so tests can echo it back with a response like a real follower.
func appendReqTo(t *testing.T, out []outbound, peer uint64) *raftv1.AppendEntriesRequest {
	t.Helper()
	var req *raftv1.AppendEntriesRequest
	for _, o := range out {
		if o.to == peer && o.appendEntries != nil {
			req = o.appendEntries
		}
	}
	if req == nil {
		t.Fatalf("no AppendEntries to node %d in outbox", peer)
	}
	return req
}

func ok(term uint64) *raftv1.AppendEntriesResponse {
	return &raftv1.AppendEntriesResponse{Term: term, Success: true}
}

func TestLeaderAppendsNoopOnElection(t *testing.T) {
	c := newTestCore(t)
	makeLeader(t, c)

	if len(c.entries) != 1 || c.entries[0].Type != raftv1.EntryType_ENTRY_TYPE_NOOP {
		t.Fatalf("new leader's log = %+v, want exactly the term-start no-op", c.entries)
	}
	if c.entries[0].Term != c.term || c.entries[0].Index != 1 {
		t.Fatalf("no-op = %+v, want term %d index 1", c.entries[0], c.term)
	}
}

func TestProposeReplicatesAndCommitsOnMajority(t *testing.T) {
	c := newTestCore(t)
	makeLeader(t, c)

	index, term, err := c.propose([]byte("cmd"), raftv1.EntryType_ENTRY_TYPE_NORMAL)
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 || term != c.term {
		t.Fatalf("proposed at (%d, %d), want (2, %d)", index, term, c.term)
	}

	out := c.takeOutbox()
	req := appendReqTo(t, out, 2)
	if len(req.GetEntries()) != 2 { // no-op + the command
		t.Fatalf("replicating %d entries, want 2", len(req.GetEntries()))
	}

	// One follower acks: leader + follower = 2 of 3.
	if err := c.handleAppendResponse(2, req, ok(c.term)); err != nil {
		t.Fatal(err)
	}
	if c.commitIndex != 2 {
		t.Fatalf("commitIndex %d after majority ack, want 2", c.commitIndex)
	}
	got := c.takeCommitted()
	if len(got) != 2 || !bytes.Equal(got[1].Command, []byte("cmd")) {
		t.Fatalf("committed %+v, want no-op then cmd", got)
	}
	if again := c.takeCommitted(); len(again) != 0 {
		t.Fatalf("entries delivered twice: %+v", again)
	}
}

func TestProposeOnNonLeaderRejectedWithHint(t *testing.T) {
	c := newTestCore(t)
	if _, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{Term: 1, LeaderId: 3}); err != nil {
		t.Fatal(err)
	}

	_, _, err := c.propose([]byte("x"), raftv1.EntryType_ENTRY_TYPE_NORMAL)
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("propose on follower = %v, want NotLeaderError", err)
	}
	if nle.LeaderID != 3 {
		t.Fatalf("leader hint %d, want 3", nle.LeaderID)
	}
}

func TestCommitRequiresMajorityNotHope(t *testing.T) {
	c := newTestCore(t)
	makeLeader(t, c)

	if _, _, err := c.propose([]byte("cmd"), raftv1.EntryType_ENTRY_TYPE_NORMAL); err != nil {
		t.Fatal(err)
	}
	if c.commitIndex != 0 {
		t.Fatalf("commitIndex %d with zero acks, want 0", c.commitIndex)
	}
}

// TestFigure8 reproduces the paper's Figure 8: an entry from an OLDER
// term reaching a majority is NOT commit — a newer leader that never saw
// it can still be elected and overwrite it. Only when an entry of the
// CURRENT term reaches a majority does everything below it commit.
func TestFigure8(t *testing.T) {
	c := newTestCore(t)

	// Node 1 holds an entry from term 2 (as leader back then), and has
	// since been re-elected at term 4.
	old := Entry{Term: 2, Index: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Command: []byte("old")}
	if err := c.store.append(old); err != nil {
		t.Fatal(err)
	}
	c.entries = []Entry{old}
	if err := c.stepDown(4); err != nil {
		t.Fatal(err)
	}
	c.role = leader
	c.leaderID = c.cfg.ID
	c.nextIndex = map[uint64]uint64{2: 2, 3: 2}
	c.matchIndex = map[uint64]uint64{2: 0, 3: 0}

	// The term-2 entry replicates to a majority (nodes 1 and 2)...
	c.matchIndex[2] = 1
	c.maybeAdvanceCommit()
	if c.commitIndex != 0 {
		t.Fatalf("commitIndex %d: committed a prior-term entry by counting replicas — Figure 8 violated", c.commitIndex)
	}

	// ...but only a current-term entry on a majority commits, and it
	// carries the old entry with it transitively.
	noop := Entry{Term: 4, Index: 2, Type: raftv1.EntryType_ENTRY_TYPE_NOOP}
	if err := c.store.append(noop); err != nil {
		t.Fatal(err)
	}
	c.entries = append(c.entries, noop)
	c.matchIndex[2] = 2
	c.maybeAdvanceCommit()
	if c.commitIndex != 2 {
		t.Fatalf("commitIndex %d after current-term entry on majority, want 2", c.commitIndex)
	}
	got := c.takeCommitted()
	if len(got) != 2 || !bytes.Equal(got[0].Command, []byte("old")) {
		t.Fatalf("committed %+v, want the term-2 entry then the no-op", got)
	}
}

func TestFollowerAppendsAndCommits(t *testing.T) {
	c := newTestCore(t)

	resp, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{
		Term: 1, LeaderId: 2,
		Entries: []*raftv1.LogEntry{
			{Term: 1, Index: 1, Type: raftv1.EntryType_ENTRY_TYPE_NOOP},
			{Term: 1, Index: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Command: []byte("a")},
		},
		LeaderCommit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetSuccess() {
		t.Fatal("append rejected")
	}
	if c.lastLogIndex() != 2 || c.commitIndex != 1 {
		t.Fatalf("lastIndex=%d commit=%d, want 2, 1", c.lastLogIndex(), c.commitIndex)
	}
	if got := c.takeCommitted(); len(got) != 1 || got[0].Index != 1 {
		t.Fatalf("committed %+v, want just entry 1", got)
	}
}

func TestFollowerRejectsWhenLogTooShort(t *testing.T) {
	c := newTestCore(t)

	resp, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 5, PrevLogTerm: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSuccess() {
		t.Fatal("accepted an append beyond the end of the log")
	}
	if resp.GetConflictIndex() != 1 || resp.GetConflictTerm() != 0 {
		t.Fatalf("hints (%d, %d), want (1, 0): log is empty", resp.GetConflictIndex(), resp.GetConflictTerm())
	}
}

func TestFollowerConflictHintsNameTheIntrudingTerm(t *testing.T) {
	c := newTestCore(t)
	seed := []Entry{entry(1, 1, "a"), entry(2, 2, "b"), entry(2, 3, "c")}
	if err := c.store.append(seed...); err != nil {
		t.Fatal(err)
	}
	c.entries = seed

	resp, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{
		Term: 3, LeaderId: 2, PrevLogIndex: 3, PrevLogTerm: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSuccess() {
		t.Fatal("accepted despite a term mismatch at prevLogIndex")
	}
	if resp.GetConflictTerm() != 2 || resp.GetConflictIndex() != 2 {
		t.Fatalf("hints (term %d, index %d), want (2, 2): term 2 starts at index 2",
			resp.GetConflictTerm(), resp.GetConflictIndex())
	}
}

func TestFollowerTruncatesConflictingSuffix(t *testing.T) {
	c := newTestCore(t)
	seed := []Entry{entry(1, 1, "a"), entry(1, 2, "stale"), entry(1, 3, "staler")}
	if err := c.store.append(seed...); err != nil {
		t.Fatal(err)
	}
	c.entries = seed

	resp, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{
		Term: 2, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*raftv1.LogEntry{
			{Term: 2, Index: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Command: []byte("fresh")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetSuccess() {
		t.Fatal("append rejected")
	}
	if c.lastLogIndex() != 2 || !bytes.Equal(c.entries[1].Command, []byte("fresh")) {
		t.Fatalf("log after conflict repair: %+v", c.entries)
	}

	// Disk must agree after a restart (the logical-truncation replay).
	c.close()
	reopened, onDisk, err := openLog(c.cfg.DataDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if len(onDisk) != 2 || !bytes.Equal(onDisk[1].Command, []byte("fresh")) {
		t.Fatalf("disk after conflict repair: %+v", onDisk)
	}
}

func TestDuplicateAppendIsIdempotent(t *testing.T) {
	c := newTestCore(t)
	req := &raftv1.AppendEntriesRequest{
		Term: 1, LeaderId: 2,
		Entries: []*raftv1.LogEntry{
			{Term: 1, Index: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Command: []byte("a")},
		},
	}

	for i := 0; i < 3; i++ { // a retransmitted RPC arrives several times
		resp, err := c.handleAppendEntries(req)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("retransmission %d rejected", i)
		}
	}
	if c.lastLogIndex() != 1 {
		t.Fatalf("log has %d entries after retransmissions, want 1", c.lastLogIndex())
	}
}

func TestFollowerCommitBoundedByVerifiedEntries(t *testing.T) {
	c := newTestCore(t)

	// The leader claims commit 10, but this exchange only verifies
	// entries through index 1 — trusting beyond that would apply entries
	// we can't know we share.
	resp, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{
		Term: 1, LeaderId: 2,
		Entries: []*raftv1.LogEntry{
			{Term: 1, Index: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Command: []byte("a")},
		},
		LeaderCommit: 10,
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatal(err)
	}
	if c.commitIndex != 1 {
		t.Fatalf("commitIndex %d, want 1", c.commitIndex)
	}
}

func TestLeaderBacktracksByWholeTerm(t *testing.T) {
	c := newTestCore(t)
	// Leader log: [t1@1, t1@2] then elected at some later term.
	seed := []Entry{entry(1, 1, "a"), entry(1, 2, "b")}
	if err := c.store.append(seed...); err != nil {
		t.Fatal(err)
	}
	c.entries = seed
	makeLeader(t, c) // appends no-op at index 3

	// Follower 2 rejects: it has term 2 entries starting at index 2
	// (a term the leader never saw).
	lastReq := &raftv1.AppendEntriesRequest{Term: c.term, PrevLogIndex: 3, PrevLogTerm: c.term}
	err := c.handleAppendResponse(2, lastReq, &raftv1.AppendEntriesResponse{
		Term: c.term, ConflictTerm: 2, ConflictIndex: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Leader has no term-2 entries → jump straight to the follower's
	// first index of that term.
	if c.nextIndex[2] != 2 {
		t.Fatalf("nextIndex %d, want 2 (skip the follower's whole term-2 run)", c.nextIndex[2])
	}

	// If instead the conflict names a term the leader DOES have, retry
	// from just past the leader's last entry of it.
	c.nextIndex[3] = 4
	err = c.handleAppendResponse(3, lastReq, &raftv1.AppendEntriesResponse{
		Term: c.term, ConflictTerm: 1, ConflictIndex: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.nextIndex[3] != 3 {
		t.Fatalf("nextIndex %d, want 3 (just past our last term-1 entry)", c.nextIndex[3])
	}
}

func TestStaleRejectionNeverAdvancesFrontier(t *testing.T) {
	c := newTestCore(t)
	makeLeader(t, c)
	c.nextIndex[2] = 2

	// A reordered rejection with hints pointing FORWARD of the frontier
	// must not be trusted.
	req := &raftv1.AppendEntriesRequest{Term: c.term, PrevLogIndex: 1}
	if err := c.handleAppendResponse(2, req, &raftv1.AppendEntriesResponse{Term: c.term, ConflictIndex: 9}); err != nil {
		t.Fatal(err)
	}
	if c.nextIndex[2] >= 2 {
		t.Fatalf("nextIndex %d moved forward on a stale rejection", c.nextIndex[2])
	}
}

func TestLeaderStepsDownOnHigherTermAppendResponse(t *testing.T) {
	c := newTestCore(t)
	makeLeader(t, c)
	term := c.term

	req := &raftv1.AppendEntriesRequest{Term: term}
	if err := c.handleAppendResponse(2, req, &raftv1.AppendEntriesResponse{Term: term + 5}); err != nil {
		t.Fatal(err)
	}
	if c.role != follower || c.term != term+5 {
		t.Fatalf("role=%v term=%d, want follower at term %d", c.role, c.term, term+5)
	}
}

func TestFollowerRefusesToTruncateCommittedEntries(t *testing.T) {
	c := newTestCore(t)

	// Commit entry 1 legitimately.
	if _, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{
		Term: 1, LeaderId: 2,
		Entries:      []*raftv1.LogEntry{{Term: 1, Index: 1, Command: []byte("committed"), Type: raftv1.EntryType_ENTRY_TYPE_NORMAL}},
		LeaderCommit: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// A (buggy or byzantine) leader now tries to overwrite it.
	_, err := c.handleAppendEntries(&raftv1.AppendEntriesRequest{
		Term: 2, LeaderId: 3,
		Entries: []*raftv1.LogEntry{{Term: 2, Index: 1, Command: []byte("rewrite"), Type: raftv1.EntryType_ENTRY_TYPE_NORMAL}},
	})
	if err == nil {
		t.Fatal("follower silently truncated a committed entry")
	}
}
