package kvraft

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Abhijeetgupta55/raftkv/internal/raft"
	"github.com/Abhijeetgupta55/raftkv/internal/storage"
	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

// Server bundles a Raft node with the replicated KV state machine and the
// client-facing gRPC service that sits on top of it. One per node.
type Server struct {
	Node *raft.Node
	KV   *KVService
	sm   *stateMachine
}

// New constructs the state machine, injects it into the node config, and
// starts the node. The caller registers Server.KV (kvv1) and a
// raft.Service wrapping Server.Node (raftv1) on its gRPC server.
func New(cfg raft.NodeConfig) (*Server, error) {
	sm := newStateMachine()
	cfg.StateMachine = sm
	n, err := raft.NewNode(cfg)
	if err != nil {
		return nil, err
	}
	return &Server{Node: n, KV: &KVService{node: n, sm: sm, shard: cfg.Group}, sm: sm}, nil
}

func (s *Server) Stop() { s.Node.Stop() }

// KVService implements the client-facing kv.v1 API. Writes go through the
// Raft log (Propose blocks until the command is applied on this node);
// reads go through a ReadBarrier so a stale ex-leader can't serve old
// data. A write or read arriving at a follower is rejected with a leader
// hint the client uses to retry.
type KVService struct {
	kvv1.UnimplementedKVServer
	node  *raft.Node
	sm    *stateMachine
	shard uint64 // this service's Raft group; 0 in unsharded mode
}

// errEmptyKey is shared by the sharded router and the per-shard service —
// key validation must happen before hashing an empty key to a shard.
var errEmptyKey = status.Error(codes.InvalidArgument, "key must not be empty")

func (s *KVService) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	cmd := encodeCommand(command{
		clientID: req.GetClientId(),
		serial:   req.GetSerial(),
		inner:    storage.EncodeCommand(storage.Command{Op: storage.OpPut, Key: req.GetKey(), Value: req.GetValue()}),
	})
	if err := s.node.Propose(ctx, cmd); err != nil {
		return nil, toStatus(s.node, s.shard, err)
	}
	return &kvv1.PutResponse{}, nil
}

func (s *KVService) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	// Best-effort existed report: the linearizable answer would require the
	// state machine to return it from Apply; for now we read the leader's
	// applied state just before proposing. (Known limitation, STATUS.md.)
	_, existed := s.sm.get(req.GetKey())
	cmd := encodeCommand(command{
		clientID: req.GetClientId(),
		serial:   req.GetSerial(),
		inner:    storage.EncodeCommand(storage.Command{Op: storage.OpDelete, Key: req.GetKey()}),
	})
	if err := s.node.Propose(ctx, cmd); err != nil {
		return nil, toStatus(s.node, s.shard, err)
	}
	return &kvv1.DeleteResponse{Existed: existed}, nil
}

func (s *KVService) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	// ReadIndex: block until this node has confirmed leadership and applied
	// through the read index, so the read is linearizable.
	if err := s.node.ReadBarrier(ctx); err != nil {
		return nil, toStatus(s.node, s.shard, err)
	}
	value, found := s.sm.get(req.GetKey())
	return &kvv1.GetResponse{Value: value, Found: found}, nil
}

// toStatus maps a raft error to a gRPC status. A not-leader error carries
// the shard and its current leader's address so the client can retry there
// and cache the leader per shard; both are embedded in the message as
// shard=<id> leader_addr=<addr> for the CLI to parse (a wire field would
// need a proto change).
func toStatus(node *raft.Node, shard uint64, err error) error {
	var nl *raft.NotLeaderError
	if errors.As(err, &nl) {
		addr := ""
		if nl.LeaderID != 0 {
			addr = node.Status().Members[nl.LeaderID]
		}
		return status.Errorf(codes.FailedPrecondition, "not leader; shard=%d leader_id=%d leader_addr=%s", shard, nl.LeaderID, addr)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.FromContextError(err).Err()
	}
	return status.Errorf(codes.Unavailable, "raft: %v", err)
}
