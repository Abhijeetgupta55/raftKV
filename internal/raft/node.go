package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// StateMachine consumes committed commands in log order. Raft neither
// knows nor cares what the bytes mean; Snapshot/Restore round-trip the
// whole state for log compaction and follower catch-up.
type StateMachine interface {
	Apply(index uint64, command []byte)
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Transport delivers one RPC to one peer. The gRPC implementation lives
// in transport.go; tests substitute in-memory ones (including the fault-
// injecting transport the verification harness uses).
type Transport interface {
	RequestVote(ctx context.Context, to uint64, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to uint64, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to uint64, req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error)
	TimeoutNow(ctx context.Context, to uint64, req *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error)
}

// NodeConfig assembles a runnable Raft node.
type NodeConfig struct {
	Config
	Group        uint64 // which Raft group this node instance belongs to (sharding)
	Transport    Transport
	StateMachine StateMachine
	// TickInterval drives the node's own clock; 0 disables the internal
	// ticker, and something else (a test, or the shared ticker fanning
	// out to every group in a process) must call Tick().
	TickInterval time.Duration
	// RPCTimeout bounds each outgoing RPC attempt. Default 1s.
	RPCTimeout time.Duration
	// CompactionThreshold triggers a snapshot + log compaction once this
	// many applied entries have accumulated past the last snapshot.
	// Default 4096; tests use tiny values.
	CompactionThreshold uint64
	// Seed for the election-timeout RNG; 0 derives one from the clock.
	Seed int64
}

// ErrStopped is returned for operations on a stopped node.
var ErrStopped = errors.New("raft: node is stopped")

// Node runs a core on an event loop. The loop goroutine is the ONLY
// thing that touches the core; RPC handlers, ticks, and proposals are
// messages into it. Sends and applies happen strictly after the core
// call that produced them returns — see afterStep.
type Node struct {
	group     uint64
	c         *core
	transport Transport
	sm        StateMachine
	rpcTO     time.Duration
	compactAt uint64

	msgc  chan any
	stopc chan struct{}
	donec chan struct{}
	once  sync.Once

	status         atomic.Pointer[Status]
	waiters        map[uint64]waiter // by log index; loop-owned
	syncedConfigV  uint64            // last configVersion pushed to the transport
	transportPeers interface {       // optional: transports that track peers
		SetPeer(id uint64, addr string)
		RemovePeer(id uint64)
	}

	// Loop-owned read/apply plumbing (ReadIndex).
	applied        uint64
	readWaiters    map[uint64]chan readReply
	appliedWaiters []appliedWaiter
}

type readReply struct {
	index uint64
	err   error
}

type appliedWaiter struct {
	index uint64
	c     chan readReply
}

// Status is a read-only snapshot of the node, published after every
// event-loop step and readable without touching the loop.
type Status struct {
	ID           uint64
	Group        uint64
	Term         uint64
	LeaderID     uint64
	IsLeader     bool
	Role         string
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
	Members      map[uint64]string
}

type waiter struct {
	term uint64 // the term the proposal was appended under
	done chan error
}

// ErrProposalLost reports that leadership changed and the proposed entry
// was overwritten before committing. The command was NOT applied; the
// client may safely retry (sessions make the retry exactly-once).
var ErrProposalLost = errors.New("raft: leadership changed, proposal lost")

// Loop messages.
type (
	evTick        struct{}
	evRequestVote struct {
		req   *raftv1.RequestVoteRequest
		respc chan rpcReply[*raftv1.RequestVoteResponse]
	}
	evAppendEntries struct {
		req   *raftv1.AppendEntriesRequest
		respc chan rpcReply[*raftv1.AppendEntriesResponse]
	}
	evVoteResp struct {
		from         uint64
		campaignTerm uint64
		preVote      bool
		resp         *raftv1.RequestVoteResponse
	}
	evAppendResp struct {
		from uint64
		req  *raftv1.AppendEntriesRequest
		resp *raftv1.AppendEntriesResponse
	}
	evInstallSnapshot struct {
		req   *raftv1.InstallSnapshotRequest
		respc chan rpcReply[*raftv1.InstallSnapshotResponse]
	}
	evSnapshotResp struct {
		from uint64
		req  *raftv1.InstallSnapshotRequest
		resp *raftv1.InstallSnapshotResponse
	}
	evTimeoutNow struct {
		req   *raftv1.TimeoutNowRequest
		respc chan rpcReply[*raftv1.TimeoutNowResponse]
	}
	evPropose struct {
		command []byte
		typ     raftv1.EntryType
		done    chan error
		respc   chan proposeReply
	}
	evConfChange struct {
		cc    confChange
		done  chan error
		respc chan proposeReply
	}
	evTransfer struct {
		to    uint64
		respc chan error
	}
	evRead struct {
		respc chan readReply // registration errors OR final resolution
	}
)

type rpcReply[T any] struct {
	resp T
	err  error
}

type proposeReply struct {
	index, term uint64
	err         error
}

func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.Transport == nil || cfg.StateMachine == nil {
		return nil, fmt.Errorf("raft: NodeConfig needs Transport and StateMachine")
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	c, err := newCore(cfg.Config, seed)
	if err != nil {
		return nil, err
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = time.Second
	}

	if cfg.CompactionThreshold == 0 {
		cfg.CompactionThreshold = 4096
	}

	n := &Node{
		group:       cfg.Group,
		c:           c,
		transport:   cfg.Transport,
		sm:          cfg.StateMachine,
		rpcTO:       cfg.RPCTimeout,
		compactAt:   cfg.CompactionThreshold,
		msgc:        make(chan any, 256),
		stopc:       make(chan struct{}),
		donec:       make(chan struct{}),
		waiters:     make(map[uint64]waiter),
		readWaiters: make(map[uint64]chan readReply),
	}
	n.applied = c.snapIndex
	if tp, okt := cfg.Transport.(interface {
		SetPeer(id uint64, addr string)
		RemovePeer(id uint64)
	}); okt {
		n.transportPeers = tp
	}

	// A node recovering from a snapshot resets its state machine to it
	// before the loop can apply anything newer.
	if data := c.takeRestore(); data != nil {
		if err := n.sm.Restore(data); err != nil {
			c.close()
			return nil, fmt.Errorf("raft: restoring state machine from snapshot: %w", err)
		}
	}
	n.syncTransportConfig()
	n.publishStatus()

	go n.run()
	if cfg.TickInterval > 0 {
		go n.tickLoop(cfg.TickInterval)
	}
	return n, nil
}

// Stop halts the loop and releases the log file. Safe to call twice.
func (n *Node) Stop() {
	n.once.Do(func() { close(n.stopc) })
	<-n.donec
}

func (n *Node) tickLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			n.enqueue(evTick{})
		case <-n.stopc:
			return
		}
	}
}

// Tick advances the node's virtual clock by one unit. Exposed so a
// process hosting many groups can drive them all from one ticker, and so
// tests control time exactly.
func (n *Node) Tick() {
	n.enqueue(evTick{})
}

func (n *Node) enqueue(m any) {
	select {
	case n.msgc <- m:
	case <-n.stopc:
	}
}

func (n *Node) run() {
	defer close(n.donec)
	defer n.c.close()

	for {
		select {
		case <-n.stopc:
			return
		case m := <-n.msgc:
			if err := n.dispatch(m); err != nil {
				// A core error here means persistence failed: the node
				// cannot safely vote, append, or campaign anymore.
				// Halting loudly beats limping into a safety violation.
				n.c.logger.Error("raft node halting", "err", err)
				n.once.Do(func() { close(n.stopc) })
				return
			}
			// The structural guarantee: everything below happens only
			// after the core call above returned — and the core persists
			// state before returning — so no message or state-machine
			// apply can ever precede its own durability.
			n.afterStep()
		}
	}
}

func (n *Node) dispatch(m any) error {
	switch ev := m.(type) {
	case evTick:
		return n.c.tick()
	case evRequestVote:
		resp, err := n.c.handleRequestVote(ev.req)
		ev.respc <- rpcReply[*raftv1.RequestVoteResponse]{resp, err}
		return err
	case evAppendEntries:
		resp, err := n.c.handleAppendEntries(ev.req)
		ev.respc <- rpcReply[*raftv1.AppendEntriesResponse]{resp, err}
		return err
	case evInstallSnapshot:
		resp, err := n.c.handleInstallSnapshot(ev.req)
		ev.respc <- rpcReply[*raftv1.InstallSnapshotResponse]{resp, err}
		return err
	case evTimeoutNow:
		resp, err := n.c.handleTimeoutNow(ev.req)
		ev.respc <- rpcReply[*raftv1.TimeoutNowResponse]{resp, err}
		return err
	case evVoteResp:
		if ev.preVote {
			return n.c.handlePreVoteResponse(ev.from, ev.campaignTerm, ev.resp)
		}
		return n.c.handleVoteResponse(ev.from, ev.campaignTerm, ev.resp)
	case evAppendResp:
		return n.c.handleAppendResponse(ev.from, ev.req, ev.resp)
	case evSnapshotResp:
		return n.c.handleInstallSnapshotResponse(ev.from, ev.req, ev.resp)
	case evConfChange:
		index, term, err := n.c.proposeConfChange(ev.cc)
		if err == nil {
			n.waiters[index] = waiter{term: term, done: ev.done}
		}
		ev.respc <- proposeReply{index, term, err}
		if errors.Is(err, errPersistFailure) {
			return err
		}
		return nil
	case evTransfer:
		ev.respc <- n.c.transferLeadership(ev.to)
		return nil
	case evRead:
		id, err := n.c.requestRead()
		if err != nil {
			ev.respc <- readReply{err: err}
			return nil
		}
		n.readWaiters[id] = ev.respc
		return nil
	case evPropose:
		index, term, err := n.c.propose(ev.command, ev.typ)
		if err == nil {
			n.waiters[index] = waiter{term: term, done: ev.done}
		}
		ev.respc <- proposeReply{index, term, err}
		if errors.Is(err, errPersistFailure) {
			return err // the node must halt
		}
		return nil // rejections (e.g. NotLeaderError) are answers
	default:
		return fmt.Errorf("raft: unknown event %T", m)
	}
}

// afterStep drains what the core produced, in dependency order: a
// snapshot restore replaces the state machine before newer entries
// apply; applies precede read-barrier resolution (reads may be waiting
// on them); sends go last.
func (n *Node) afterStep() {
	if data := n.c.takeRestore(); data != nil {
		if err := n.sm.Restore(data); err != nil {
			// Same severity as a persist failure: state is unknown.
			n.c.logger.Error("state machine restore failed; halting", "err", err)
			n.once.Do(func() { close(n.stopc) })
			return
		}
		n.applied = n.c.snapIndex
		n.fireAppliedWaiters()
	}
	for _, e := range n.c.takeCommitted() {
		n.applyEntry(e)
	}
	for _, r := range n.c.takeReadOutcomes() {
		n.resolveRead(r)
	}
	n.maybeCompact()
	n.syncTransportConfig()
	for _, o := range n.c.takeOutbox() {
		n.sendAsync(o)
	}
	n.publishStatus()
}

func (n *Node) applyEntry(e Entry) {
	if e.Type == raftv1.EntryType_ENTRY_TYPE_NORMAL {
		n.sm.Apply(e.Index, e.Command)
	}
	n.applied = e.Index
	n.fireAppliedWaiters()

	if w, okw := n.waiters[e.Index]; okw {
		delete(n.waiters, e.Index)
		if w.term == e.Term {
			w.done <- nil
		} else {
			// Some other leader's entry landed on this index: ours was
			// truncated away before committing, provably unapplied.
			w.done <- ErrProposalLost
		}
	}
}

// maybeCompact snapshots the state machine once enough applied entries
// have piled up past the last snapshot. Runs on the loop, so the image
// is a consistent point in time.
func (n *Node) maybeCompact() {
	if n.applied < n.c.snapIndex+n.compactAt {
		return
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		n.c.logger.Warn("state machine snapshot failed; log keeps growing", "err", err)
		return
	}
	if err := n.c.compact(n.applied, data); err != nil {
		n.c.logger.Warn("log compaction failed; will retry", "err", err)
	}
}

// syncTransportConfig pushes membership changes into the transport so
// new members are dialable and removed ones aren't.
func (n *Node) syncTransportConfig() {
	if n.transportPeers == nil || n.c.configVersion == n.syncedConfigV {
		return
	}
	for id, addr := range n.c.members {
		if id != n.c.cfg.ID && addr != "" {
			n.transportPeers.SetPeer(id, addr)
		}
	}
	n.syncedConfigV = n.c.configVersion
}

// resolveRead completes a read barrier: a confirmed read waits (if
// needed) for the apply cursor to reach its index; an aborted one fails
// so the client can retry on the current leader.
func (n *Node) resolveRead(r readOutcome) {
	w, okw := n.readWaiters[r.id]
	if !okw {
		return
	}
	delete(n.readWaiters, r.id)
	if !r.confirmed {
		w <- readReply{err: ErrProposalLost}
		return
	}
	if n.applied >= r.index {
		w <- readReply{index: r.index}
		return
	}
	n.appliedWaiters = append(n.appliedWaiters, appliedWaiter{index: r.index, c: w})
}

func (n *Node) fireAppliedWaiters() {
	if len(n.appliedWaiters) == 0 {
		return
	}
	remaining := n.appliedWaiters[:0]
	for _, w := range n.appliedWaiters {
		if n.applied >= w.index {
			w.c <- readReply{index: w.index}
		} else {
			remaining = append(remaining, w)
		}
	}
	n.appliedWaiters = remaining
}

// sendAsync ships one outbound message without ever blocking the loop;
// the response (if any) re-enters as an event. Transport errors are
// dropped by design: Raft's own retry machinery (heartbeats, election
// timeouts) is the recovery path, not the RPC layer.
func (n *Node) sendAsync(o outbound) {
	switch {
	case o.requestVote != nil:
		o.requestVote.Group = n.group
		req := o.requestVote
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTO)
			defer cancel()
			if resp, err := n.transport.RequestVote(ctx, o.to, req); err == nil {
				n.enqueue(evVoteResp{from: o.to, campaignTerm: req.GetTerm(), preVote: req.GetPreVote(), resp: resp})
			}
		}()
	case o.appendEntries != nil:
		o.appendEntries.Group = n.group
		req := o.appendEntries
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTO)
			defer cancel()
			if resp, err := n.transport.AppendEntries(ctx, o.to, req); err == nil {
				n.enqueue(evAppendResp{from: o.to, req: req, resp: resp})
			}
		}()
	case o.installSnapshot != nil:
		o.installSnapshot.Group = n.group
		req := o.installSnapshot
		go func() {
			// Snapshots can be big; give them more room than a heartbeat.
			ctx, cancel := context.WithTimeout(context.Background(), 4*n.rpcTO)
			defer cancel()
			if resp, err := n.transport.InstallSnapshot(ctx, o.to, req); err == nil {
				n.enqueue(evSnapshotResp{from: o.to, req: req, resp: resp})
			}
		}()
	case o.timeoutNow != nil:
		o.timeoutNow.Group = n.group
		req := o.timeoutNow
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTO)
			defer cancel()
			n.transport.TimeoutNow(ctx, o.to, req) // fire and forget
		}()
	}
}

func (n *Node) publishStatus() {
	members := make(map[uint64]string, len(n.c.members))
	for id, addr := range n.c.members {
		members[id] = addr
	}
	s := &Status{
		ID:           n.c.cfg.ID,
		Group:        n.group,
		Term:         n.c.term,
		LeaderID:     n.c.leaderID,
		IsLeader:     n.c.role == leader,
		Role:         n.c.role.String(),
		CommitIndex:  n.c.commitIndex,
		LastApplied:  n.applied,
		LastLogIndex: n.c.lastLogIndex(),
		Members:      members,
	}
	n.status.Store(s)
}

// Status returns the node's latest published state without touching the
// event loop.
func (n *Node) Status() Status {
	return *n.status.Load()
}

// Propose replicates a command and blocks until it is applied to the
// state machine (nil), provably lost (ErrProposalLost), rejected because
// this node is not the leader (NotLeaderError), or ctx expires — in
// which case the outcome is UNKNOWN: the command may commit later, which
// is exactly why clients carry session serials.
func (n *Node) Propose(ctx context.Context, command []byte) error {
	done := make(chan error, 1)
	respc := make(chan proposeReply, 1)
	select {
	case n.msgc <- evPropose{command: command, typ: raftv1.EntryType_ENTRY_TYPE_NORMAL, done: done, respc: respc}:
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case r := <-respc:
		if r.err != nil {
			return r.err
		}
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-done:
		return err
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleRequestVote is the gRPC service's entry into the loop.
func (n *Node) HandleRequestVote(ctx context.Context, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	respc := make(chan rpcReply[*raftv1.RequestVoteResponse], 1)
	select {
	case n.msgc <- evRequestVote{req: req, respc: respc}:
	case <-n.stopc:
		return nil, ErrStopped
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-respc:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// HandleAppendEntries is the gRPC service's entry into the loop.
func (n *Node) HandleAppendEntries(ctx context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	respc := make(chan rpcReply[*raftv1.AppendEntriesResponse], 1)
	select {
	case n.msgc <- evAppendEntries{req: req, respc: respc}:
	case <-n.stopc:
		return nil, ErrStopped
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-respc:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// HandleInstallSnapshot is the gRPC service's entry into the loop.
func (n *Node) HandleInstallSnapshot(ctx context.Context, req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error) {
	respc := make(chan rpcReply[*raftv1.InstallSnapshotResponse], 1)
	select {
	case n.msgc <- evInstallSnapshot{req: req, respc: respc}:
	case <-n.stopc:
		return nil, ErrStopped
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-respc:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// HandleTimeoutNow is the gRPC service's entry into the loop.
func (n *Node) HandleTimeoutNow(ctx context.Context, req *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error) {
	respc := make(chan rpcReply[*raftv1.TimeoutNowResponse], 1)
	select {
	case n.msgc <- evTimeoutNow{req: req, respc: respc}:
	case <-n.stopc:
		return nil, ErrStopped
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-respc:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ReadBarrier implements the ReadIndex protocol: when it returns nil,
// the local state machine reflects every write that completed before
// this call began, so reading it is linearizable. Errors are retryable
// (not leader, leader not ready, leadership lost mid-confirmation).
func (n *Node) ReadBarrier(ctx context.Context) error {
	respc := make(chan readReply, 1)
	select {
	case n.msgc <- evRead{respc: respc}:
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case r := <-respc:
		return r.err
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AddMember proposes adding a node to the cluster; blocks like Propose
// until the config change commits.
func (n *Node) AddMember(ctx context.Context, id uint64, addr string) error {
	return n.confChange(ctx, confChange{op: confChangeAdd, id: id, addr: addr})
}

// RemoveMember proposes removing a node from the cluster.
func (n *Node) RemoveMember(ctx context.Context, id uint64) error {
	return n.confChange(ctx, confChange{op: confChangeRemove, id: id})
}

func (n *Node) confChange(ctx context.Context, cc confChange) error {
	done := make(chan error, 1)
	respc := make(chan proposeReply, 1)
	select {
	case n.msgc <- evConfChange{cc: cc, done: done, respc: respc}:
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case r := <-respc:
		if r.err != nil {
			return r.err
		}
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TransferLeadership asks this node (which must lead) to hand off to a
// specific peer. Returns once the handoff is initiated; completion is
// observable as a term change.
func (n *Node) TransferLeadership(ctx context.Context, to uint64) error {
	respc := make(chan error, 1)
	select {
	case n.msgc <- evTransfer{to: to, respc: respc}:
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-respc:
		return err
	case <-n.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}
