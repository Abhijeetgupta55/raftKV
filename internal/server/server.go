// Package server implements the kv.v1 gRPC service on top of a storage
// engine. It owns request validation and the mapping between storage
// results and wire responses — and nothing else, so the same service
// implementation keeps working when the storage engine underneath it
// gains a WAL and, later, a Raft log.
package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Abhijeetgupta55/raftkv/internal/storage"
	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

// Request size limits, enforced here rather than in the schema so they can
// change without a wire-format break (ADR 0002). The value cap keeps
// messages comfortably inside gRPC's default 4 MiB receive limit.
const (
	MaxKeyBytes   = 4 * 1024
	MaxValueBytes = 1 * 1024 * 1024
)

// KVServer serves the client-facing KV API for a single node.
type KVServer struct {
	kvv1.UnimplementedKVServer
	store *storage.MemStore
}

func New(store *storage.MemStore) *KVServer {
	return &KVServer{store: store}
}

func (s *KVServer) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	if len(req.GetValue()) > MaxValueBytes {
		return nil, status.Errorf(codes.InvalidArgument,
			"value is %d bytes, limit is %d", len(req.GetValue()), MaxValueBytes)
	}

	s.store.Put(req.GetKey(), req.GetValue())
	return &kvv1.PutResponse{}, nil
}

func (s *KVServer) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}

	value, found := s.store.Get(req.GetKey())
	return &kvv1.GetResponse{Value: value, Found: found}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}

	existed := s.store.Delete(req.GetKey())
	return &kvv1.DeleteResponse{Existed: existed}, nil
}

func validateKey(key string) error {
	if key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(key) > MaxKeyBytes {
		return status.Error(codes.InvalidArgument,
			fmt.Sprintf("key is %d bytes, limit is %d", len(key), MaxKeyBytes))
	}
	return nil
}
