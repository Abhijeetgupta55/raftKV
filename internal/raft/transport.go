package raft

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// GRPCTransport sends Raft RPCs over gRPC, lazily dialing one connection
// per peer and reusing it. Peer addresses can change at runtime
// (membership changes), guarded by mu.
type GRPCTransport struct {
	mu    sync.RWMutex
	addrs map[uint64]string
	conns map[uint64]*grpc.ClientConn
}

func NewGRPCTransport(addrs map[uint64]string) *GRPCTransport {
	m := make(map[uint64]string, len(addrs))
	for id, a := range addrs {
		m[id] = a
	}
	return &GRPCTransport{addrs: m, conns: make(map[uint64]*grpc.ClientConn)}
}

// SetPeer adds or updates a peer's address (dropping any stale
// connection); used when membership changes.
func (t *GRPCTransport) SetPeer(id uint64, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.addrs[id] == addr {
		return
	}
	t.addrs[id] = addr
	if c, okc := t.conns[id]; okc {
		c.Close()
		delete(t.conns, id)
	}
}

func (t *GRPCTransport) RemovePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.addrs, id)
	if c, okc := t.conns[id]; okc {
		c.Close()
		delete(t.conns, id)
	}
}

func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.conns {
		c.Close()
		delete(t.conns, id)
	}
}

func (t *GRPCTransport) client(to uint64) (raftv1.RaftClient, error) {
	t.mu.RLock()
	conn, okc := t.conns[to]
	t.mu.RUnlock()
	if okc {
		return raftv1.NewRaftClient(conn), nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, okc = t.conns[to]; okc { // raced with another sender
		return raftv1.NewRaftClient(conn), nil
	}
	addr, oka := t.addrs[to]
	if !oka {
		return nil, fmt.Errorf("raft transport: no address for node %d", to)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[to] = conn
	return raftv1.NewRaftClient(conn), nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, to uint64, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, to uint64, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, to uint64, req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return c.InstallSnapshot(ctx, req)
}

func (t *GRPCTransport) TimeoutNow(ctx context.Context, to uint64, req *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return c.TimeoutNow(ctx, req)
}
