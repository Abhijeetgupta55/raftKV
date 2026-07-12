// Package cluster_test is the Milestone 2/3 acceptance gate for the wired
// server binary: it launches three REAL server processes over TCP, drives
// a write storm through the leader, hard-kills (kill -9) the leader,
// proves a new leader is elected and that every acknowledged write
// survives, then restarts the dead node and confirms the cluster returns
// to full health with all data intact.
package cluster_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

var serverBin string

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "raftkv-cluster-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmp)

	serverBin = filepath.Join(tmp, "kvserver")
	if runtime.GOOS == "windows" {
		serverBin += ".exe"
	}
	build := exec.Command("go", "build", "-o", serverBin, "github.com/Abhijeetgupta55/raftkv/cmd/server")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building server:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

type node struct {
	id    uint64
	addr  string
	dir   string
	peers string
	extra []string // extra flags, e.g. --shards 4
	proc  *exec.Cmd
}

func (n *node) start(t *testing.T) {
	t.Helper()
	args := []string{
		"--id", fmt.Sprint(n.id),
		"--listen", n.addr,
		"--peers", n.peers,
		"--data-dir", n.dir,
	}
	args = append(args, n.extra...)
	n.proc = exec.Command(serverBin, args...)
	n.proc.Stdout, n.proc.Stderr = os.Stdout, os.Stderr
	if err := n.proc.Start(); err != nil {
		t.Fatalf("start node %d: %v", n.id, err)
	}
}

func (n *node) kill() {
	if n.proc != nil && n.proc.Process != nil {
		_ = n.proc.Process.Kill() // kill -9 equivalent
		_, _ = n.proc.Process.Wait()
	}
}

// client dials all node addresses and routes each write/read to the
// leader, following the leader_addr hint a follower returns and retrying
// across elections until the deadline.
type client struct {
	conns map[string]kvv1.KVClient
	addrs []string
	last  string // last known leader
}

func newClient(t *testing.T, addrs []string) *client {
	t.Helper()
	c := &client{conns: map[string]kvv1.KVClient{}, addrs: addrs, last: addrs[0]}
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial %s: %v", a, err)
		}
		c.conns[a] = kvv1.NewKVClient(conn)
	}
	return c
}

// leaderHint pulls "leader_addr=host:port" out of a FailedPrecondition.
func leaderHint(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, tok := range strings.Fields(st.Message()) {
		if strings.HasPrefix(tok, "leader_addr=") {
			return strings.TrimPrefix(tok, "leader_addr=")
		}
	}
	return ""
}

func (c *client) put(clientID, serial uint64, key, val string, deadline time.Time) error {
	try := c.last
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := c.conns[try].Put(ctx, &kvv1.PutRequest{
			Key: key, Value: []byte(val), ClientId: clientID, Serial: serial,
		})
		cancel()
		if err == nil {
			c.last = try
			return nil
		}
		lastErr = err
		if hint := leaderHint(err); hint != "" && c.conns[hint] != nil {
			try = hint
			continue
		}
		try = c.nextAddr(try)
		time.Sleep(30 * time.Millisecond)
	}
	return fmt.Errorf("put %s timed out: %w", key, lastErr)
}

func (c *client) get(key string, deadline time.Time) (string, bool, error) {
	try := c.last
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := c.conns[try].Get(ctx, &kvv1.GetRequest{Key: key})
		cancel()
		if err == nil {
			c.last = try
			return string(resp.GetValue()), resp.GetFound(), nil
		}
		lastErr = err
		if hint := leaderHint(err); hint != "" && c.conns[hint] != nil {
			try = hint
			continue
		}
		try = c.nextAddr(try)
		time.Sleep(30 * time.Millisecond)
	}
	return "", false, fmt.Errorf("get %s timed out: %w", key, lastErr)
}

func (c *client) nextAddr(cur string) string {
	for i, a := range c.addrs {
		if a == cur {
			return c.addrs[(i+1)%len(c.addrs)]
		}
	}
	return c.addrs[0]
}

func TestClusterFailoverNoAckedLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process cluster test skipped in -short")
	}
	dir := t.TempDir()
	addrs := []string{"127.0.0.1:5501", "127.0.0.1:5502", "127.0.0.1:5503"}
	peers := "1@127.0.0.1:5501,2@127.0.0.1:5502,3@127.0.0.1:5503"
	nodes := map[uint64]*node{}
	for i, a := range addrs {
		id := uint64(i + 1)
		nodes[id] = &node{id: id, addr: a, dir: filepath.Join(dir, fmt.Sprint(id)), peers: peers}
		nodes[id].start(t)
	}
	defer func() {
		for _, n := range nodes {
			n.kill()
		}
	}()

	cli := newClient(t, addrs)

	// Wait for the cluster to elect a leader and accept the first write.
	if err := cli.put(1, 1, "boot", "ok", time.Now().Add(15*time.Second)); err != nil {
		t.Fatalf("cluster never became writable: %v", err)
	}

	// Write storm. Every acked write is recorded; each carries a session so
	// retries across the failover are exactly-once.
	acked := map[string]string{}
	var serial uint64 = 1
	for i := 0; i < 60; i++ {
		serial++
		k := fmt.Sprintf("k%03d", i)
		v := fmt.Sprintf("v%03d", i)
		if err := cli.put(1, serial, k, v, time.Now().Add(15*time.Second)); err != nil {
			t.Fatalf("write %s failed pre-kill: %v", k, err)
		}
		acked[k] = v
	}

	// Identify and hard-kill the leader (the node the client last used).
	leaderAddr := cli.last
	var killedID uint64
	for id, n := range nodes {
		if n.addr == leaderAddr {
			killedID = id
			n.kill()
		}
	}
	t.Logf("killed leader node %d (%s)", killedID, leaderAddr)

	// Keep writing; the surviving two must elect a new leader and accept.
	for i := 60; i < 90; i++ {
		serial++
		k := fmt.Sprintf("k%03d", i)
		v := fmt.Sprintf("v%03d", i)
		if err := cli.put(1, serial, k, v, time.Now().Add(15*time.Second)); err != nil {
			t.Fatalf("write %s failed after failover: %v", k, err)
		}
		acked[k] = v
	}

	// Zero acknowledged loss: every acked write must read back correctly.
	for k, want := range acked {
		got, found, err := cli.get(k, time.Now().Add(15*time.Second))
		if err != nil {
			t.Fatalf("read-back %s: %v", k, err)
		}
		if !found || got != want {
			t.Fatalf("acked write lost/wrong: %s = %q (found=%v), want %q", k, got, found, want)
		}
	}

	// Restart the killed node from its data dir; the cluster returns to
	// full health and still serves all data.
	nodes[killedID].start(t)
	time.Sleep(2 * time.Second)
	if err := cli.put(1, serial+1, "afterrejoin", "yes", time.Now().Add(15*time.Second)); err != nil {
		t.Fatalf("cluster not writable after rejoin: %v", err)
	}
	if got, found, err := cli.get("k000", time.Now().Add(15*time.Second)); err != nil || !found || got != "v000" {
		t.Fatalf("data lost after rejoin: k000=%q found=%v err=%v", got, found, err)
	}
}
