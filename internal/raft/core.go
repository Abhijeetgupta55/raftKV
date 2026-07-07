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
	// ID of this node. 1-based; 0 is reserved for "no node".
	ID uint64
	// PeerIDs lists the other cluster members (not including ID), for
	// callers that don't need addresses (unit tests). Ignored when
	// Members is set.
	PeerIDs []uint64
	// Members is the initial cluster membership including this node
	// (id → address). Superseded by any membership recorded in the
	// node's snapshot or log — a restarted node believes its own
	// history, not its flags.
	Members map[uint64]string
	// DataDir holds the log, term/vote, and snapshot files.
	DataDir string

	// Timing, measured in ticks (the node layer owns the tick duration).
	// The election timeout is re-randomized in [Min, Max) at every reset —
	// the paper's mechanism for breaking repeated split votes.
	ElectionTicksMin int // default 20
	ElectionTicksMax int // default 40
	HeartbeatTicks   int // default 2

	// DisablePreVote turns off the pre-vote round (leadership.go).
	// Production keeps it on; some unit tests disable it to drive basic
	// elections directly.
	DisablePreVote bool

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
	to              uint64
	requestVote     *raftv1.RequestVoteRequest
	appendEntries   *raftv1.AppendEntriesRequest
	installSnapshot *raftv1.InstallSnapshotRequest
	timeoutNow      *raftv1.TimeoutNowRequest
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

	// ticksSinceLeader counts ticks since we last heard from a live leader
	// of our term (AppendEntries or InstallSnapshot). Unlike
	// electionElapsed — which also resets when WE start a campaign or grant
	// a vote — this is reset ONLY by genuine leader contact, so it is a
	// truthful measure of leader recency. Pre-vote uses it to decide
	// whether a probe would disrupt a healthy cluster (recent contact) or
	// is a legitimate response to a vanished leader (prolonged silence).
	ticksSinceLeader int

	votes    map[uint64]bool // candidate: who granted us this term
	preVotes map[uint64]bool // pre-candidate: who would vote for us
	outbox   []outbound

	// Leader volatile state (paper Figure 2), rebuilt at every election
	// win: nextIndex is where to try replicating to each peer next,
	// matchIndex is the highest entry each peer is known to store.
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Committed-but-unapplied entries queued for the node layer, and the
	// high-water mark of what has been queued (never re-delivered).
	committed     []Entry
	lastDelivered uint64

	// Snapshot state: the log is compacted through snapIndex/snapTerm;
	// snapConfig is the membership as of that point. pendingRestore
	// carries an installed snapshot's data to the node layer.
	snapIndex      uint64
	snapTerm       uint64
	snapConfig     map[uint64]string
	pendingRestore []byte

	// Active membership (id → address) and change tracking.
	members         map[uint64]string
	lastConfigIndex uint64
	configVersion   uint64

	// Leadership transfer in progress: proposals are refused and the
	// transfer aborts after transferTicks.
	transferTarget uint64
	transferTicks  int

	// ReadIndex machinery (readindex.go): appendSeq stamps outgoing
	// AppendEntries so fresh acks are distinguishable from stale ones.
	appendSeq    uint64
	readCounter  uint64
	pendingReads []pendingRead
	readOutcomes []readOutcome
}

func newCore(cfg Config, rngSeed int64) (*core, error) {
	cfg.applyDefaults()
	if cfg.ID == 0 {
		return nil, fmt.Errorf("raft: node ID must be nonzero")
	}
	if cfg.Members == nil {
		cfg.Members = make(map[uint64]string, len(cfg.PeerIDs)+1)
		cfg.Members[cfg.ID] = ""
		for _, p := range cfg.PeerIDs {
			if p == cfg.ID {
				return nil, fmt.Errorf("raft: PeerIDs must not contain the node's own ID %d", cfg.ID)
			}
			cfg.Members[p] = ""
		}
	}

	term, votedFor, err := loadTermAndVote(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	snap, err := loadRaftSnapshot(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	store, entries, err := openLog(cfg.DataDir, snap.lastIndex)
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

		snapIndex:  snap.lastIndex,
		snapTerm:   snap.lastTerm,
		snapConfig: snap.config,

		// Everything in a snapshot is committed and applied by
		// definition; the state machine is restored to it at boot.
		commitIndex:    snap.lastIndex,
		lastDelivered:  snap.lastIndex,
		pendingRestore: snap.data,
	}
	if c.snapConfig == nil {
		// A node with no snapshot yet trusts its startup flags for the
		// initial membership.
		c.snapConfig = cfg.Members
	}
	c.saveState = func(term, votedFor uint64) error {
		return saveTermAndVote(cfg.DataDir, term, votedFor)
	}
	// The active membership is the snapshot's, updated by any config
	// entries recovered from the log.
	c.recalcConfig()
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

// peerIDs returns the other members of the current configuration.
func (c *core) peerIDs() []uint64 {
	ids := make([]uint64, 0, len(c.members))
	for id := range c.members {
		if id != c.cfg.ID {
			ids = append(ids, id)
		}
	}
	return ids
}

func (c *core) lastLogIndex() uint64 {
	if len(c.entries) == 0 {
		return c.snapIndex
	}
	return c.entries[len(c.entries)-1].Index
}

// termAt returns the term of the entry at index, or 0 when the index is
// not available (term 0 is never valid, so it doubles as "absent"). The
// snapshot boundary keeps its term even though the entry itself is gone.
func (c *core) termAt(index uint64) uint64 {
	if index == c.snapIndex {
		return c.snapTerm
	}
	if index == 0 || len(c.entries) == 0 {
		return 0
	}
	base := c.entries[0].Index
	if index < base || index > c.lastLogIndex() {
		return 0
	}
	return c.entries[index-base].Term
}

func (c *core) lastLogTerm() uint64 {
	if len(c.entries) == 0 {
		return c.snapTerm
	}
	return c.entries[len(c.entries)-1].Term
}

// majority is the quorum size: more than half of the current
// configuration. Computed over members (not peers+self) because a node
// may legitimately no longer be a member of its own cluster mid-removal.
func (c *core) majority() int {
	return len(c.members)/2 + 1
}

// isMember reports whether this node is part of the current config.
func (c *core) isMember() bool {
	_, m := c.members[c.cfg.ID]
	return m
}

func (c *core) resetElectionTimer() {
	c.electionElapsed = 0
	span := c.cfg.ElectionTicksMax - c.cfg.ElectionTicksMin
	c.electionTimeout = c.cfg.ElectionTicksMin + c.rng.Intn(span)
}

// tick advances virtual time by one unit. Followers and candidates creep
// toward an election; leaders emit heartbeats and age out a stalled
// leadership transfer.
func (c *core) tick() error {
	if c.role == leader {
		c.ticksSinceLeader = 0 // a leader is, trivially, in contact with itself
		if c.transferTarget != 0 {
			c.transferTicks--
			if c.transferTicks <= 0 {
				c.logger.Warn("leadership transfer timed out", "target", c.transferTarget)
				c.transferTarget = 0
			}
		}
		c.heartbeatElapsed++
		if c.heartbeatElapsed >= c.cfg.HeartbeatTicks {
			c.heartbeatElapsed = 0
			c.broadcastAppends()
		}
		return nil
	}

	c.electionElapsed++
	c.ticksSinceLeader++
	if c.electionElapsed >= c.electionTimeout {
		// A node removed from the cluster must not disturb it with
		// elections it can no longer win or even legitimately join.
		if !c.isMember() {
			c.resetElectionTimer()
			return nil
		}
		if c.cfg.DisablePreVote {
			return c.startCampaign()
		}
		return c.startPreCampaign()
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

	for _, id := range c.peerIDs() {
		c.outbox = append(c.outbox, outbound{to: id, requestVote: &raftv1.RequestVoteRequest{
			Term:         c.term,
			CandidateId:  c.cfg.ID,
			LastLogIndex: c.lastLogIndex(),
			LastLogTerm:  c.lastLogTerm(),
		}})
	}

	return c.maybeWinElection() // a single-node cluster wins immediately
}

// handleRequestVote implements the voter's side of elections, including
// the election restriction (§5.4.1) that makes a won election imply
// possession of every committed entry.
func (c *core) handleRequestVote(req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	// Pre-vote probes are answered stateless-ly and must be checked
	// FIRST: their inflated term is hypothetical and must not trigger
	// the step-down below — that's the disruption pre-vote exists to
	// prevent.
	if req.GetPreVote() {
		return c.handlePreVote(req), nil
	}

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
		return c.maybeWinElection()
	}
	return nil
}

func (c *core) maybeWinElection() error {
	if c.role != candidate || len(c.votes) < c.majority() {
		return nil
	}
	c.role = leader
	c.leaderID = c.cfg.ID
	c.heartbeatElapsed = 0
	c.preVotes = nil
	c.transferTarget = 0

	// Rebuild the replication frontier: optimistically assume every peer
	// has our whole log; rejections walk nextIndex back to the truth.
	c.nextIndex = make(map[uint64]uint64, len(c.peerIDs()))
	c.matchIndex = make(map[uint64]uint64, len(c.peerIDs()))
	for _, id := range c.peerIDs() {
		c.nextIndex[id] = c.lastLogIndex() + 1
	}
	c.logger.Info("won election", "term", c.term, "votes", len(c.votes))

	// Append a no-op for the new term. Until an entry of OUR term
	// commits, nothing can commit (the Figure-8 rule), and client
	// proposals may be scarce — the no-op unblocks the pipeline
	// immediately and later gives ReadIndex its commit floor.
	noop := Entry{Term: c.term, Index: c.lastLogIndex() + 1, Type: raftv1.EntryType_ENTRY_TYPE_NOOP}
	if err := c.store.append(noop); err != nil {
		return fmt.Errorf("raft: persisting term-start no-op: %w", err)
	}
	c.entries = append(c.entries, noop)

	// Announce immediately — every unclaimed tick is a chance for another
	// node to time out and start a competing election.
	c.broadcastAppends()
	c.maybeAdvanceCommit() // single-node cluster: the no-op commits now
	return nil
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
	c.preVotes = nil
	c.transferTarget = 0
	c.abortPendingReads()
	if err := c.saveState(c.term, c.votedFor); err != nil {
		return fmt.Errorf("raft: persisting step-down: %w", err)
	}
	c.logger.Info("stepped down", "from_role", oldRole.String(), "from_term", oldTerm, "to_term", term)
	return nil
}
