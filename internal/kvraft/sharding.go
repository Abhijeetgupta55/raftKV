package kvraft

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Abhijeetgupta55/raftkv/internal/raft"
	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

// Sharding (M5): the keyspace is partitioned across independent Raft
// groups by key hash. Raft knows nothing about this — each group
// replicates opaque bytes exactly as before; this file is the routing
// layer above it. Design and trade-offs: DESIGN.md "Sharding (M5)".

// ShardFor maps a key to its shard: fnv1a64(key) % shardCount. Stateless
// and cheap, so any node — and any client that knows the shard count —
// computes the same answer with no shard-map service.
func ShardFor(key string, shardCount int) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64() % uint64(shardCount)
}

// ShardedServer hosts one replica of every shard (static total placement)
// inside a single process: S raft.Nodes, S state machines, one shared
// ticker driving them all.
type ShardedServer struct {
	shards []*Server // index == group id
	KV     *ShardedKV

	stopc chan struct{}
	once  sync.Once
	wg    sync.WaitGroup
}

// NewSharded brings up shardCount groups. base supplies everything the
// groups share (node ID, members, transport, timings); per group it is
// specialized with Group=g, DataDir=dataRoot/shard-g, and TickInterval=0 —
// the shared ticker here is the only clock, so tens of groups don't run
// tens of timer goroutines and no group can starve another's elections.
//
// The shard count is pinned in dataRoot/shards on first boot; a restart
// with a different --shards refuses to start rather than silently routing
// keys into the wrong group's log.
func NewSharded(base raft.NodeConfig, shardCount int, dataRoot string, tick time.Duration) (*ShardedServer, error) {
	if shardCount < 1 {
		return nil, fmt.Errorf("kvraft: shard count must be >= 1, got %d", shardCount)
	}
	if err := pinShardCount(dataRoot, shardCount); err != nil {
		return nil, err
	}

	s := &ShardedServer{stopc: make(chan struct{})}
	for g := 0; g < shardCount; g++ {
		cfg := base
		cfg.Group = uint64(g)
		cfg.Config.DataDir = filepath.Join(dataRoot, fmt.Sprintf("shard-%d", g))
		cfg.TickInterval = 0 // driven by the shared ticker below
		if err := os.MkdirAll(cfg.Config.DataDir, 0o755); err != nil {
			s.Stop()
			return nil, fmt.Errorf("create shard dir: %w", err)
		}
		srv, err := New(cfg)
		if err != nil {
			s.Stop()
			return nil, fmt.Errorf("start shard %d: %w", g, err)
		}
		s.shards = append(s.shards, srv)
	}
	s.KV = &ShardedKV{shards: s.shards}

	s.wg.Add(1)
	go s.tickLoop(tick)
	return s, nil
}

// tickLoop is the shared clock: one timer, fanned out to every group.
// Tick delivery is a non-blocking enqueue on each group's own loop, so a
// slow group delays its neighbors by at most the fan-out iteration.
func (s *ShardedServer) tickLoop(interval time.Duration) {
	defer s.wg.Done()
	if interval <= 0 {
		interval = 30 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			for _, sh := range s.shards {
				sh.Node.Tick()
			}
		case <-s.stopc:
			return
		}
	}
}

// Nodes returns group id -> node, in the shape raft.NewService wants, so
// the process registers ONE raftv1 service for all its groups.
func (s *ShardedServer) Nodes() map[uint64]*raft.Node {
	m := make(map[uint64]*raft.Node, len(s.shards))
	for g, sh := range s.shards {
		m[uint64(g)] = sh.Node
	}
	return m
}

// SetUnsafeNoReadBarrier propagates the mutation-check hook (see
// service.go) to every shard. Test harness use only.
func (s *ShardedServer) SetUnsafeNoReadBarrier(v bool) {
	for _, sh := range s.shards {
		sh.SetUnsafeNoReadBarrier(v)
	}
}

func (s *ShardedServer) Stop() {
	s.once.Do(func() { close(s.stopc) })
	s.wg.Wait()
	for _, sh := range s.shards {
		if sh != nil {
			sh.Stop()
		}
	}
}

// pinShardCount records shardCount in dataRoot/shards on first boot and
// verifies it on every subsequent one.
func pinShardCount(dataRoot string, shardCount int) error {
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return fmt.Errorf("create data root: %w", err)
	}
	pin := filepath.Join(dataRoot, "shards")
	b, err := os.ReadFile(pin)
	if os.IsNotExist(err) {
		return os.WriteFile(pin, []byte(strconv.Itoa(shardCount)), 0o644)
	}
	if err != nil {
		return err
	}
	prev, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return fmt.Errorf("corrupt shard pin %s: %q", pin, b)
	}
	if prev != shardCount {
		return fmt.Errorf("data dir was created with %d shards, refusing to start with %d (resharding is not supported)", prev, shardCount)
	}
	return nil
}

// ShardedKV implements the client-facing kv.v1 API across shards: compute
// the key's shard, delegate to that group's per-shard service. Rejections
// carry the shard id in the leader hint so clients cache leaders per
// shard rather than per cluster.
type ShardedKV struct {
	kvv1.UnimplementedKVServer
	shards []*Server
}

func (s *ShardedKV) shardFor(key string) *Server {
	return s.shards[ShardFor(key, len(s.shards))]
}

func (s *ShardedKV) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req.GetKey() == "" {
		return nil, errEmptyKey
	}
	return s.shardFor(req.GetKey()).KV.Put(ctx, req)
}

func (s *ShardedKV) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, errEmptyKey
	}
	return s.shardFor(req.GetKey()).KV.Get(ctx, req)
}

func (s *ShardedKV) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, errEmptyKey
	}
	return s.shardFor(req.GetKey()).KV.Delete(ctx, req)
}
