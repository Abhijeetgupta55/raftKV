package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// This file exercises the whole Raft stack — Node event loop, log
// replication, election, ReadIndex, and membership — across a real
// multi-node cluster wired together by an in-memory network. It is the
// integration counterpart to the single-core unit tests, and proves the
// Milestone 2 deliverable: one state machine replicated across 3+ nodes,
// correct under leader failure.
//
// The memNetwork also injects the first fault the verification harness
// (Milestone 6) will build on: node isolation (a symmetric partition of
// one node from the rest).

// ---- in-memory transport -------------------------------------------------

type memNetwork struct {
	mu       sync.RWMutex
	nodes    map[uint64]*Node
	isolated map[uint64]bool
}

func newMemNetwork() *memNetwork {
	return &memNetwork{nodes: make(map[uint64]*Node), isolated: make(map[uint64]bool)}
}

func (nw *memNetwork) register(id uint64, n *Node) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.nodes[id] = n
}

// setIsolated symmetrically partitions id from the rest of the cluster:
// while isolated, no RPC crosses the boundary in either direction, which
// is exactly what a leader losing quorum experiences.
func (nw *memNetwork) setIsolated(id uint64, v bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.isolated[id] = v
}

// dst returns the peer's Node if an RPC from->to can cross the network.
func (nw *memNetwork) dst(from, to uint64) (*Node, bool) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	if nw.isolated[from] || nw.isolated[to] {
		return nil, false
	}
	n, ok := nw.nodes[to]
	return n, ok
}

var errPartitioned = fmt.Errorf("raft test: partitioned")

// memTransport is one node's view of the network. It delivers each RPC
// straight into the target node's loop; the target processes it on its
// own goroutine, so this never deadlocks the sender's loop.
type memTransport struct {
	nw   *memNetwork
	from uint64
}

func (t *memTransport) RequestVote(ctx context.Context, to uint64, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	n, ok := t.nw.dst(t.from, to)
	if !ok {
		return nil, errPartitioned
	}
	return n.HandleRequestVote(ctx, req)
}

func (t *memTransport) AppendEntries(ctx context.Context, to uint64, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	n, ok := t.nw.dst(t.from, to)
	if !ok {
		return nil, errPartitioned
	}
	return n.HandleAppendEntries(ctx, req)
}

func (t *memTransport) InstallSnapshot(ctx context.Context, to uint64, req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error) {
	n, ok := t.nw.dst(t.from, to)
	if !ok {
		return nil, errPartitioned
	}
	return n.HandleInstallSnapshot(ctx, req)
}

func (t *memTransport) TimeoutNow(ctx context.Context, to uint64, req *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error) {
	n, ok := t.nw.dst(t.from, to)
	if !ok {
		return nil, errPartitioned
	}
	return n.HandleTimeoutNow(ctx, req)
}

// ---- test state machine --------------------------------------------------

// kvSM is a trivial replicated key-value map. Commands are "key=value";
// Snapshot/Restore round-trip the whole map through gob so the snapshot
// and InstallSnapshot paths have something real to carry.
type kvSM struct {
	mu   sync.Mutex
	data map[string]string
}

func newKVSM() *kvSM { return &kvSM{data: make(map[string]string)} }

func (s *kvSM) Apply(index uint64, command []byte) {
	k, v := splitKV(command)
	s.mu.Lock()
	s.data[k] = v
	s.mu.Unlock()
}

func (s *kvSM) get(k string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[k]
	return v, ok
}

func (s *kvSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *kvSM) Restore(data []byte) error {
	m := make(map[string]string)
	if len(data) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.data = m
	s.mu.Unlock()
	return nil
}

func splitKV(command []byte) (string, string) {
	for i := 0; i < len(command); i++ {
		if command[i] == '=' {
			return string(command[:i]), string(command[i+1:])
		}
	}
	return string(command), ""
}

// ---- cluster harness -----------------------------------------------------

type testCluster struct {
	t   *testing.T
	nw  *memNetwork
	ids []uint64
	sms map[uint64]*kvSM
	nn  map[uint64]*Node
}

// newCluster brings up ids as a running Raft cluster over the in-memory
// network, each node ticking on its own clock.
func newCluster(t *testing.T, ids ...uint64) *testCluster {
	t.Helper()
	nw := newMemNetwork()
	c := &testCluster{t: t, nw: nw, ids: ids, sms: map[uint64]*kvSM{}, nn: map[uint64]*Node{}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, id := range ids {
		peers := make([]uint64, 0, len(ids)-1)
		members := map[uint64]string{}
		for _, o := range ids {
			members[o] = fmt.Sprintf("mem://%d", o)
			if o != id {
				peers = append(peers, o)
			}
		}
		sm := newKVSM()
		n, err := NewNode(NodeConfig{
			Config: Config{
				ID:               id,
				Members:          members,
				DataDir:          t.TempDir(),
				ElectionTicksMin: 10,
				ElectionTicksMax: 20,
				HeartbeatTicks:   1,
				Logger:           logger,
			},
			Transport:           &memTransport{nw: nw, from: id},
			StateMachine:        sm,
			TickInterval:        5 * time.Millisecond,
			CompactionThreshold: 8,
			Seed:                int64(id), // distinct election timeouts
		})
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		c.sms[id] = sm
		c.nn[id] = n
		nw.register(id, n)
	}
	t.Cleanup(c.stop)
	return c
}

func (c *testCluster) stop() {
	for _, n := range c.nn {
		n.Stop()
	}
}

// waitLeader blocks until exactly one node reports leadership at the
// highest observed term, and returns it.
func (c *testCluster) waitLeader(timeout time.Duration) *Node {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *Node
		leaders := 0
		for _, id := range c.ids {
			if c.nw.isIsolated(id) {
				continue
			}
			s := c.nn[id].Status()
			if s.IsLeader {
				leaders++
				leader = c.nn[id]
			}
		}
		if leaders == 1 {
			// Give a beat for followers to learn the leader, avoiding a
			// flap where a just-elected leader hasn't heartbeated yet.
			time.Sleep(20 * time.Millisecond)
			if leader.Status().IsLeader {
				return leader
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no unique leader within %s", timeout)
	return nil
}

func (nw *memNetwork) isIsolated(id uint64) bool {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	return nw.isolated[id]
}

// proposeTo drives a command through whichever node currently leads,
// retrying across leader changes until it commits or the deadline passes.
func (c *testCluster) proposeTo(cmd string, timeout time.Duration) error {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		leader := c.waitLeader(2 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := leader.Propose(ctx, []byte(cmd))
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("propose %q never committed: %w", cmd, lastErr)
}

// waitApplied blocks until every non-isolated node's state machine holds
// key=want.
func (c *testCluster) waitApplied(key, want string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range c.ids {
			if c.nw.isIsolated(id) {
				continue
			}
			if v, ok := c.sms[id].get(key); !ok || v != want {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("key %q never converged to %q on all reachable nodes", key, want)
}

// ---- tests ---------------------------------------------------------------

func TestClusterElectsSingleLeader(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	leader := c.waitLeader(3 * time.Second)
	t.Logf("elected node %d at term %d", leader.Status().ID, leader.Status().Term)
}

func TestClusterReplicatesProposals(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	if err := c.proposeTo("color=blue", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.waitApplied("color", "blue", 2*time.Second)
}

func TestClusterSurvivesLeaderFailure(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	if err := c.proposeTo("k=v1", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.waitApplied("k", "v1", 2*time.Second)

	// Partition the current leader away; the majority must elect a new one.
	old := c.waitLeader(2 * time.Second)
	oldID := old.Status().ID
	c.nw.setIsolated(oldID, true)
	t.Logf("isolated old leader %d", oldID)

	newLeader := c.waitLeader(3 * time.Second)
	if newLeader.Status().ID == oldID {
		t.Fatalf("isolated node %d still reported as leader", oldID)
	}

	// The surviving majority keeps accepting writes.
	if err := c.proposeTo("k=v2", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.waitApplied("k", "v2", 2*time.Second)

	// Heal the partition; the old leader rejoins and catches up to v2.
	c.nw.setIsolated(oldID, false)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := c.sms[oldID].get("k"); ok && v == "v2" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reintegrated node %d never caught up to v2", oldID)
}

func TestLinearizableReadReflectsCommittedWrite(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	if err := c.proposeTo("x=42", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	leader := c.waitLeader(2 * time.Second)

	// A ReadBarrier that returns nil guarantees the local state machine
	// reflects every write that completed before the read began.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := leader.ReadBarrier(ctx); err != nil {
		t.Fatalf("read barrier: %v", err)
	}
	sm := c.sms[leader.Status().ID]
	if v, ok := sm.get("x"); !ok || v != "42" {
		t.Fatalf("linearizable read saw x=%q,%v; want 42", v, ok)
	}
}

// addNode spins up a brand-new node that knows the current membership
// plus itself, registers it on the network so the leader's AppendEntries
// (or InstallSnapshot) can reach it, and returns its id. The caller then
// drives AddMember through the leader to make the change official.
func (c *testCluster) addNode(id uint64) uint64 {
	c.t.Helper()
	members := map[uint64]string{id: fmt.Sprintf("mem://%d", id)}
	for _, o := range c.ids {
		members[o] = fmt.Sprintf("mem://%d", o)
	}
	sm := newKVSM()
	n, err := NewNode(NodeConfig{
		Config: Config{
			ID:               id,
			Members:          members,
			DataDir:          c.t.TempDir(),
			ElectionTicksMin: 10,
			ElectionTicksMax: 20,
			HeartbeatTicks:   1,
			Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		Transport:           &memTransport{nw: c.nw, from: id},
		StateMachine:        sm,
		TickInterval:        5 * time.Millisecond,
		CompactionThreshold: 8,
		Seed:                int64(id),
	})
	if err != nil {
		c.t.Fatalf("add node %d: %v", id, err)
	}
	c.sms[id] = sm
	c.nn[id] = n
	c.ids = append(c.ids, id)
	c.nw.register(id, n)
	return id
}

// waitNodeApplied waits for one specific node's state machine to hold
// key=want.
func (c *testCluster) waitNodeApplied(id uint64, key, want string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := c.sms[id].get(key); ok && v == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := c.sms[id].get(key)
	c.t.Fatalf("node %d never saw %s=%s (have %q)", id, key, want, got)
}

func TestMembershipAddNodeCatchesUp(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	if err := c.proposeTo("a=1", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.waitApplied("a", "1", 2*time.Second)

	leader := c.waitLeader(2 * time.Second)
	joiner := c.addNode(4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := leader.AddMember(ctx, joiner, fmt.Sprintf("mem://%d", joiner)); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	// The new member must receive the existing state and stay in sync.
	c.waitNodeApplied(joiner, "a", "1", 3*time.Second)
	if err := c.proposeTo("b=2", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.waitNodeApplied(joiner, "b", "2", 3*time.Second)
}

func TestInstallSnapshotCatchesUpNewMember(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	// Push well past the compaction threshold (8) so the early log is
	// snapshotted and truncated away on the leader.
	for i := 0; i < 20; i++ {
		if err := c.proposeTo(fmt.Sprintf("counter=%d", i), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied("counter", "19", 3*time.Second)

	leader := c.waitLeader(2 * time.Second)
	// The leader's log no longer contains index 1, so the only way to
	// bring a fresh node current is InstallSnapshot.
	joiner := c.addNode(5)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := leader.AddMember(ctx, joiner, fmt.Sprintf("mem://%d", joiner)); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	c.waitNodeApplied(joiner, "counter", "19", 4*time.Second)
}

func TestLeadershipTransfer(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	if err := c.proposeTo("k=v", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	leader := c.waitLeader(2 * time.Second)
	from := leader.Status().ID
	var target uint64
	for _, id := range c.ids {
		if id != from {
			target = id
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := leader.TransferLeadership(ctx, target); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	// The target should take over promptly (TimeoutNow skips the timeout).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.nn[target].Status().IsLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("transfer target %d never became leader", target)
}
