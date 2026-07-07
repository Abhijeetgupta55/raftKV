package raft

import (
	"fmt"
	"log/slog"
	"math/rand"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// core is the Raft state machine as a plain single-threaded object: every
// method mutates state synchronously and returns; there are no goroutines,
// channels, or clocks in here. Incoming RPCs are handled synchronously
// (handleRequestVote, handleAppendEntries return the response); sends the
// node initiates (campaign votes, heartbeats) accumulate in the outbox for
// the event loop to transmit *after* the method returns.
//
// That sequencing is a correctness property, not a convenience: state is
// always persisted during the method call, and messages leave only after
// it returns, so nothing observable ever precedes its own durability.
// It also makes every test in core_test.go deterministic — elections are
// driven by calling tick() by hand.

type role uint8

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// Config describes one member of a Raft cluster.
type Config struct {
	// ID of this node; must appear in every peer's PeerIDs. 1-based.
	ID uint64
	// PeerIDs lists the other cluster members (not including ID).
	// Membership is static until Milestone 3.
	PeerIDs []uint64
	// DataDir holds the log and term/vote files.
	DataDir string

	// Timing, measured in ticks (the node layer owns the tick duration).
	// The election timeout is re-randomized in [Min, Max) at every reset —
	// the paper's mechanism for breaking repeated split votes.
	ElectionTicksMin int // default 20
	ElectionTicksMax int // default 40
	HeartbeatTicks   int // default 2

	Logger *slog.Logger // default slog.Default()
}

func (cfg *Config) applyDefaults() {
	if cfg.ElectionTicksMin == 0 {
		cfg.ElectionTicksMin = 20
	}
	if cfg.ElectionTicksMax == 0 {
		cfg.ElectionTicksMax = 40
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 2
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
}

// outbound is one message the core wants sent. Exactly one field is set.
type outbound struct {
	to            uint64
	requestVote   *raftv1.RequestVoteRequest
	appendEntries *raftv1.AppendEntriesRequest
}

type core struct {
	cfg    Config
	logger *slog.Logger

	role     role
	term     uint64
	votedFor uint64
	leaderID uint64 // 0 = unknown

	entries     []Entry // in-memory image of the whole log (compaction is M3)
	store       *logStore
	commitIndex uint64

	// saveState persists term+votedFor; a test seam over saveTermAndVote.
	saveState func(term, votedFor uint64) error

	electionElapsed  int
	electionTimeout  int // current randomized value
	heartbeatElapsed int
	rng              *rand.Rand

	votes  map[uint64]bool // candidate: who granted us this term
	outbox []outbound
}

func newCore(cfg Config, rngSeed int64) (*core, error) {
	cfg.applyDefaults()
	if cfg.ID == 0 {
		return nil, fmt.Errorf("raft: node ID must be nonzero")
	}
	for _, p := range cfg.PeerIDs {
		if p == cfg.ID {
			return nil, fmt.Errorf("raft: PeerIDs must not contain the node's own ID %d", cfg.ID)
		}
	}

	term, votedFor, err := loadTermAndVote(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	store, entries, err := openLog(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	c := &core{
		cfg:      cfg,
		logger:   cfg.Logger.With("node", cfg.ID),
		role:     follower,
		term:     term,
		votedFor: votedFor,
		entries:  entries,
		store:    store,
		rng:      rand.New(rand.NewSource(rngSeed)),
	}
	c.saveState = func(term, votedFor uint64) error {
		return saveTermAndVote(cfg.DataDir, term, votedFor)
	}
	c.resetElectionTimer()
	return c, nil
}

func (c *core) close() error {
	return c.store.close()
}

// takeOutbox hands the accumulated sends to the caller and clears them.
func (c *core) takeOutbox() []outbound {
	out := c.outbox
	c.outbox = nil
	return out
}

func (c *core) lastLogIndex() uint64 {
	if len(c.entries) == 0 {
		return 0
	}
	return c.entries[len(c.entries)-1].Index
}

func (c *core) lastLogTerm() uint64 {
	if len(c.entries) == 0 {
		return 0
	}
	return c.entries[len(c.entries)-1].Term
}

// majority is the quorum size: more than half of the full cluster.
func (c *core) majority() int {
	return (len(c.cfg.PeerIDs)+1)/2 + 1
}

func (c *core) resetElectionTimer() {
	c.electionElapsed = 0
	span := c.cfg.ElectionTicksMax - c.cfg.ElectionTicksMin
	c.electionTimeout = c.cfg.ElectionTicksMin + c.rng.Intn(span)
}

// tick advances virtual time by one unit. Followers and candidates creep
// toward an election; leaders emit heartbeats.
func (c *core) tick() error {
	if c.role == leader {
		c.heartbeatElapsed++
		if c.heartbeatElapsed >= c.cfg.HeartbeatTicks {
			c.heartbeatElapsed = 0
			c.broadcastHeartbeats()
		}
		return nil
	}

	c.electionElapsed++
	if c.electionElapsed >= c.electionTimeout {
		return c.startCampaign()
	}
	return nil
}

// startCampaign begins a new election: new term, vote for self, ask
// everyone else. Also the split-vote retry path — a candidate that times
// out campaigns again with a fresh randomized timeout.
func (c *core) startCampaign() error {
	c.role = candidate
	c.term++
	c.votedFor = c.cfg.ID
	c.leaderID = 0
	c.votes = map[uint64]bool{c.cfg.ID: true}
	c.resetElectionTimer()

	// Persisted before the method returns; the outbox is transmitted only
	// after it returns. A node can therefore never be asked about a
	// candidacy it wouldn't remember surviving a crash.
	if err := c.saveState(c.term, c.votedFor); err != nil {
		return fmt.Errorf("raft: persisting candidacy: %w", err)
	}
	c.logger.Info("starting election", "term", c.term)

	for _, id := range c.cfg.PeerIDs {
		c.outbox = append(c.outbox, outbound{to: id, requestVote: &raftv1.RequestVoteRequest{
			Term:         c.term,
			CandidateId:  c.cfg.ID,
			LastLogIndex: c.lastLogIndex(),
			LastLogTerm:  c.lastLogTerm(),
		}})
	}

	c.maybeWinElection() // a single-node cluster wins immediately
	return nil
}

// handleRequestVote implements the voter's side of elections, including
// the election restriction (§5.4.1) that makes a won election imply
// possession of every committed entry.
func (c *core) handleRequestVote(req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	if req.GetTerm() > c.term {
		if err := c.stepDown(req.GetTerm()); err != nil {
			return nil, err
		}
	}

	resp := &raftv1.RequestVoteResponse{Term: c.term}
	if req.GetTerm() < c.term {
		return resp, nil
	}

	canVote := c.votedFor == noVote || c.votedFor == req.GetCandidateId()
	if canVote && c.logUpToDate(req.GetLastLogIndex(), req.GetLastLogTerm()) {
		if c.votedFor != req.GetCandidateId() {
			c.votedFor = req.GetCandidateId()
			// The vote is on disk before the response can exist. A node
			// that forgot a granted vote could vote twice in one term and
			// elect two leaders.
			if err := c.saveState(c.term, c.votedFor); err != nil {
				return nil, err
			}
		}
		// Granting a vote is the one non-leader event that resets the
		// election timer: we've just endorsed someone else's candidacy,
		// so give them time to win. A denied candidate gets no such
		// courtesy, or a stale-logged node could suppress elections
		// indefinitely.
		c.resetElectionTimer()
		resp.VoteGranted = true
		c.logger.Info("granted vote", "term", c.term, "candidate", req.GetCandidateId())
	}
	return resp, nil
}

// logUpToDate reports whether a candidate's log is at least as complete
// as ours: later last term wins; equal terms compare length.
func (c *core) logUpToDate(lastIndex, lastTerm uint64) bool {
	if lastTerm != c.lastLogTerm() {
		return lastTerm > c.lastLogTerm()
	}
	return lastIndex >= c.lastLogIndex()
}

// handleVoteResponse tallies a vote from an earlier broadcast. Responses
// from superseded campaigns are dropped: campaignTerm pins which election
// the vote belonged to.
func (c *core) handleVoteResponse(from, campaignTerm uint64, resp *raftv1.RequestVoteResponse) error {
	if resp.GetTerm() > c.term {
		return c.stepDown(resp.GetTerm())
	}
	if c.role != candidate || campaignTerm != c.term {
		return nil // a stale answer to a question we're no longer asking
	}
	if resp.GetVoteGranted() {
		c.votes[from] = true
		c.maybeWinElection()
	}
	return nil
}

func (c *core) maybeWinElection() {
	if c.role != candidate || len(c.votes) < c.majority() {
		return
	}
	c.role = leader
	c.leaderID = c.cfg.ID
	c.heartbeatElapsed = 0
	c.logger.Info("won election", "term", c.term, "votes", len(c.votes))

	// Announce immediately — every unclaimed tick is a chance for another
	// node to time out and start a competing election.
	c.broadcastHeartbeats()
}

// handleAppendEntries is the follower's side of replication. In this
// unit it covers term handling and leader recognition; the log-matching
// check, entry append, and commit advancement land with the replication
// unit.
func (c *core) handleAppendEntries(req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	if req.GetTerm() > c.term {
		if err := c.stepDown(req.GetTerm()); err != nil {
			return nil, err
		}
	}

	resp := &raftv1.AppendEntriesResponse{Term: c.term}
	if req.GetTerm() < c.term {
		return resp, nil
	}

	// An AppendEntries at our own term is proof of a legitimate current
	// leader: election safety guarantees at most one per term, so a
	// candidate concedes and a follower refreshes its patience.
	if c.role != follower {
		c.role = follower
		c.votes = nil
		c.logger.Info("recognized leader", "term", c.term, "leader", req.GetLeaderId())
	}
	c.leaderID = req.GetLeaderId()
	c.resetElectionTimer()

	// TODO(M2 replication unit): log-matching consistency check, entry
	// append with conflict resolution, commit-index advancement.
	resp.Success = true
	return resp, nil
}

// stepDown adopts a higher term and reverts to follower. It deliberately
// does NOT reset the election timer: discovering a bigger term number is
// not evidence of a live leader, and deferring our own candidacy for it
// would let a crashed candidate's ghost suppress elections.
func (c *core) stepDown(term uint64) error {
	oldRole, oldTerm := c.role, c.term
	c.role = follower
	c.term = term
	c.votedFor = noVote
	c.leaderID = 0
	c.votes = nil
	if err := c.saveState(c.term, c.votedFor); err != nil {
		return fmt.Errorf("raft: persisting step-down: %w", err)
	}
	c.logger.Info("stepped down", "from_role", oldRole.String(), "from_term", oldTerm, "to_term", term)
	return nil
}

func (c *core) broadcastHeartbeats() {
	for _, id := range c.cfg.PeerIDs {
		c.outbox = append(c.outbox, outbound{to: id, appendEntries: &raftv1.AppendEntriesRequest{
			Term:         c.term,
			LeaderId:     c.cfg.ID,
			PrevLogIndex: c.lastLogIndex(),
			PrevLogTerm:  c.lastLogTerm(),
			LeaderCommit: c.commitIndex,
		}})
	}
}
