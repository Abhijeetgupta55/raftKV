package cluster_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestShardedClusterFailoverNoAckedLoss is the M5 real-process acceptance
// gate: 3 real server processes over TCP, each hosting a replica of FOUR
// Raft groups (--shards 4), under a cross-shard write storm. kill -9 one
// node — every shard loses a replica at once, at least one loses its
// leader — and every shard must stay available with zero acknowledged
// loss anywhere. Then the node restarts from its per-shard data dirs and
// the cluster returns to full health.
func TestShardedClusterFailoverNoAckedLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process cluster test skipped in -short")
	}
	dir := t.TempDir()
	addrs := []string{"127.0.0.1:5511", "127.0.0.1:5512", "127.0.0.1:5513"}
	peers := "1@127.0.0.1:5511,2@127.0.0.1:5512,3@127.0.0.1:5513"
	nodes := map[uint64]*node{}
	for i, a := range addrs {
		id := uint64(i + 1)
		nodes[id] = &node{id: id, addr: a, dir: filepath.Join(dir, fmt.Sprint(id)), peers: peers,
			extra: []string{"--shards", "4"}}
		nodes[id].start(t)
	}
	defer func() {
		for _, n := range nodes {
			n.kill()
		}
	}()

	cli := newClient(t, addrs)
	if err := cli.put(1, 1, "boot", "ok", time.Now().Add(15*time.Second)); err != nil {
		t.Fatalf("cluster never became writable: %v", err)
	}

	// Cross-shard storm: sequential keys hash across all four groups.
	acked := map[string]string{}
	var serial uint64 = 1
	for i := 0; i < 80; i++ {
		serial++
		k := fmt.Sprintf("shardkey-%03d", i)
		v := fmt.Sprintf("v%03d", i)
		if err := cli.put(1, serial, k, v, time.Now().Add(5*time.Second)); err != nil {
			t.Fatalf("write %s failed pre-kill: %v", k, err)
		}
		acked[k] = v
	}

	// kill -9 the node the client last talked to — it leads at least the
	// shard of the last key, and hosts a replica of all four.
	leaderAddr := cli.last
	var killedID uint64
	for id, n := range nodes {
		if n.addr == leaderAddr {
			killedID = id
			n.kill()
		}
	}
	t.Logf("killed node %d (%s) hosting replicas of all 4 shards", killedID, leaderAddr)

	// The storm continues; every shard must fail over independently.
	for i := 80; i < 120; i++ {
		serial++
		k := fmt.Sprintf("shardkey-%03d", i)
		v := fmt.Sprintf("v%03d", i)
		if err := cli.put(1, serial, k, v, time.Now().Add(15*time.Second)); err != nil {
			t.Fatalf("write %s failed after node kill: %v", k, err)
		}
		acked[k] = v
	}

	// Zero acknowledged loss on ANY shard.
	for k, want := range acked {
		got, found, err := cli.get(k, time.Now().Add(15*time.Second))
		if err != nil {
			t.Fatalf("read-back %s: %v", k, err)
		}
		if !found || got != want {
			t.Fatalf("acked write lost on shard %s: %s=%q (found=%v), want %q", k, k, got, found, want)
		}
	}

	// Rejoin: the dead node restarts from its per-shard dirs (this is also
	// the per-shard crash-recovery check — each group's log replays).
	nodes[killedID].start(t)
	time.Sleep(2 * time.Second)
	serial++
	if err := cli.put(1, serial, "after-rejoin", "yes", time.Now().Add(15*time.Second)); err != nil {
		t.Fatalf("cluster not writable after rejoin: %v", err)
	}
	for _, probe := range []string{"shardkey-000", "shardkey-041", "shardkey-082", "shardkey-119"} {
		got, found, err := cli.get(probe, time.Now().Add(15*time.Second))
		if err != nil || !found || got != acked[probe] {
			t.Fatalf("data wrong after rejoin: %s=%q found=%v err=%v", probe, got, found, err)
		}
	}
}
