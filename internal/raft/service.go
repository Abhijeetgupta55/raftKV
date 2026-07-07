package raft

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// Service exposes local Raft nodes over gRPC, routing each request to
// the group it names. One Service per process, registered once on the
// shared gRPC server.
type Service struct {
	raftv1.UnimplementedRaftServer
	nodes map[uint64]*Node // by group
}

func NewService(nodes map[uint64]*Node) *Service {
	return &Service{nodes: nodes}
}

func (s *Service) node(group uint64) (*Node, error) {
	n, okn := s.nodes[group]
	if !okn {
		return nil, status.Errorf(codes.NotFound, "raft: no group %d on this node", group)
	}
	return n, nil
}

func (s *Service) RequestVote(ctx context.Context, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	n, err := s.node(req.GetGroup())
	if err != nil {
		return nil, err
	}
	return n.HandleRequestVote(ctx, req)
}

func (s *Service) AppendEntries(ctx context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	n, err := s.node(req.GetGroup())
	if err != nil {
		return nil, err
	}
	return n.HandleAppendEntries(ctx, req)
}

func (s *Service) InstallSnapshot(ctx context.Context, req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error) {
	n, err := s.node(req.GetGroup())
	if err != nil {
		return nil, err
	}
	return n.HandleInstallSnapshot(ctx, req)
}

func (s *Service) TimeoutNow(ctx context.Context, req *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error) {
	n, err := s.node(req.GetGroup())
	if err != nil {
		return nil, err
	}
	return n.HandleTimeoutNow(ctx, req)
}
