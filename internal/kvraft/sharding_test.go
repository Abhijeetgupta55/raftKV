package kvraft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Abhijeetgupta55/raftkv/internal/raft"
	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

// TestShardDistribution pins the partitioning function's two contracts:
// determinism (same key, same shard, always) and rough uniformity (no
// shard starves or hogs the keyspace).
func TestShardDistribution(t *testing.T) {
	const shards, keys = 4, 2000
	counts := make([]int, shards)
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("user/%d/profile", i)
		g := ShardFor(k, shards)
		if g2 := ShardFor(k, shards); g2 != g {
			t.Fatalf("ShardFor not deterministic: %d then %d", g, g2)
		}
		counts[g]++
	}
	for g, n := range counts {
		frac := float64(n) / keys
		if frac < 0.15 || frac > 0.35 {
			t.Fatalf("shard %d holds %.0f%% of keys, want roughly uniform (15–35%%): %v", g, frac*100, counts)
		}
	}
}

// TestShardCountPinRefusesMismatch: restarting a data dir with a different
// --shards must fail loudly, never silently re-route keys.
func TestShardCountPinRefusesMismatch(t *testing.T) {
	dir := t.TempDir()
	if err := pinShardCount(dir, 4); err != nil {
		t.Fatal(err)
	}
	if err := pinShardCount(dir, 4); err != nil {
		t.Fatalf("same count must be accepted: %v", err)
	}
	if err := pinShardCount(dir, 8); err == nil {
		t.Fatal("shard-count mismatch accepted — keys would silently land in wrong groups")
	}
}

// ---- in-process multi-group cluster over real gRPC on loopback ----------

type shardedNode struct {
	id   uint64
	addr string
	srv  *ShardedServer
	grpc *grpc.Server
}

// startShardedCluster brings up n processes' worth of ShardedServers in
// one test process, each serving KV+Raft on a real loopback listener.
func startShardedCluster(t *testing.T, n, shards int) (map[uint64]*shardedNode, []string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Reserve real ports first so every node knows the full member map.
	listeners := make([]net.Listener, n)
	members := map[uint64]string{}
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		members[uint64(i+1)] = lis.Addr().String()
	}

	nodes := map[uint64]*shardedNode{}
	var addrs []string
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		transport := raft.NewGRPCTransport(members)
		srv, err := NewSharded(raft.NodeConfig{
			Config: raft.Config{
				ID:               id,
				Members:          members,
				ElectionTicksMin: 10,
				ElectionTicksMax: 20,
				HeartbeatTicks:   2,
				Logger:           logger,
			},
			Transport:  transport,
			RPCTimeout: time.Second,
			Seed:       int64(id),
		}, shards, t.TempDir(), 5*time.Millisecond)
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		gs := grpc.NewServer()
		kvv1.RegisterKVServer(gs, srv.KV)
		raftv1.RegisterRaftServer(gs, raft.NewService(srv.Nodes()))
		go gs.Serve(listeners[i])

		nd := &shardedNode{id: id, addr: members[id], srv: srv, grpc: gs}
		nodes[id] = nd
		addrs = append(addrs, nd.addr)
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.grpc.Stop()
			nd.srv.Stop()
		}
	})
	return nodes, addrs
}

// kvDo drives one op against the cluster, rotating/following hints like a
// client would, until success or deadline.
func kvDo(t *testing.T, addrs []string, timeout time.Duration, op func(kvv1.KVClient, context.Context) error) error {
	t.Helper()
	conns := map[string]kvv1.KVClient{}
	for _, a := range addrs {
		c, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		conns[a] = kvv1.NewKVClient(c)
	}
	deadline := time.Now().Add(timeout)
	cur := 0
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := op(conns[addrs[cur]], ctx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if hint := hintAddr(err); hint != "" && conns[hint] != nil {
			for i, a := range addrs {
				if a == hint {
					cur = i
				}
			}
			continue
		}
		cur = (cur + 1) % len(addrs)
		time.Sleep(20 * time.Millisecond)
	}
	return lastErr
}

func hintAddr(err error) string {
	for _, tok := range strings.Fields(err.Error()) {
		if strings.HasPrefix(tok, "leader_addr=") {
			return strings.TrimPrefix(tok, "leader_addr=")
		}
	}
	return ""
}

// TestShardsSurviveNodeLossIndependently is the M5 in-process acceptance:
// 3 nodes × 4 groups; stop one whole node (leader of at least one group)
// and every shard must keep serving — each group's election and quorum are
// independent, so losing one replica of each never blocks any of them.
func TestShardsSurviveNodeLossIndependently(t *testing.T) {
	nodes, addrs := startShardedCluster(t, 3, 4)

	put := func(k, v string, timeout time.Duration) error {
		return kvDo(t, addrs, timeout, func(c kvv1.KVClient, ctx context.Context) error {
			_, err := c.Put(ctx, &kvv1.PutRequest{Key: k, Value: []byte(v)})
			return err
		})
	}
	get := func(k string, timeout time.Duration) (string, error) {
		var got string
		err := kvDo(t, addrs, timeout, func(c kvv1.KVClient, ctx context.Context) error {
			r, err := c.Get(ctx, &kvv1.GetRequest{Key: k})
			if err == nil {
				got = string(r.GetValue())
			}
			return err
		})
		return got, err
	}

	// Seed keys that land on every shard (pick keys until all 4 covered).
	seeded := map[uint64]string{}
	for i := 0; len(seeded) < 4; i++ {
		k := fmt.Sprintf("seed-%d", i)
		g := ShardFor(k, 4)
		if _, done := seeded[g]; done {
			continue
		}
		if err := put(k, "v1", 10*time.Second); err != nil {
			t.Fatalf("seeding shard %d: %v", g, err)
		}
		seeded[g] = k
	}

	// Find a node that currently leads at least one group and stop it.
	var victim *shardedNode
	for _, nd := range nodes {
		for _, sh := range nd.srv.shards {
			if sh.Node.Status().IsLeader {
				victim = nd
			}
		}
	}
	if victim == nil {
		t.Fatal("no leader found on any node")
	}
	victim.grpc.Stop()
	victim.srv.Stop()
	t.Logf("stopped node %d (hosted a replica of every shard)", victim.id)
	var survivors []string
	for _, a := range addrs {
		if a != victim.addr {
			survivors = append(survivors, a)
		}
	}

	// Every shard must elect (if it lost its leader) and keep serving both
	// its old data and new writes.
	for g, k := range seeded {
		if got, err := get(k, 15*time.Second); err != nil || got != "v1" {
			t.Fatalf("shard %d lost availability or data after node loss: %q %v", g, got, err)
		}
		k2 := k + "-after"
		if err := kvDoAddrs(t, survivors, k2, "v2"); err != nil {
			t.Fatalf("shard %d refused new writes after node loss: %v", g, err)
		}
	}
}

// kvDoAddrs is a small put helper against an explicit address set.
func kvDoAddrs(t *testing.T, addrs []string, k, v string) error {
	return kvDo(t, addrs, 15*time.Second, func(c kvv1.KVClient, ctx context.Context) error {
		_, err := c.Put(ctx, &kvv1.PutRequest{Key: k, Value: []byte(v)})
		return err
	})
}
