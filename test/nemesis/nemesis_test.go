// Package nemesis_test runs the named fault-injection scenarios (M6 part
// 1) against REAL server processes over TCP, with every inter-node byte
// flowing through the nemesis's interposition proxies. Each scenario is
// seeded (-args -seed=N reproduces the workload op sequence and the
// soak's fault schedule), prints its seed, and writes a timestamped
// operation history file for the linearizability checker (RUN 3).
//
// Verification here is exact zero-acked-loss on per-client keys (see
// nemesis.Workload); full shared-key linearizability checking arrives
// with Porcupine in RUN 3.
package nemesis_test

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Abhijeetgupta55/raftkv/internal/nemesis"
	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

var seedFlag = flag.Int64("seed", 0, "scenario seed; 0 = derive from clock (printed for reruns)")

func newRand(s int64) *rand.Rand { return rand.New(rand.NewSource(s)) }

var serverBin string

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "raftkv-nemesis-*")
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

func seed(t *testing.T) int64 {
	s := *seedFlag
	if s == 0 {
		s = time.Now().UnixNano() % 1_000_000_007
	}
	t.Logf("SEED %d (rerun: go test ./test/nemesis/ -run %s -v -args -seed=%d)", s, t.Name(), s)
	return s
}

// cluster is three real processes whose inter-node traffic crosses the
// nemesis proxies; clients reach nodes directly.
type cluster struct {
	t     *testing.T
	net   *nemesis.Net
	procs map[uint64]*nemesis.Proc
	real  map[uint64]string
	addrs []string
	args  map[uint64][]string
	logs  map[uint64]*os.File // per-node process logs: failure evidence
	env   []string            // extra process env (mutation check)
}

var portBase atomic.Int64

func init() { portBase.Store(5600) }

func startCluster(t *testing.T, extra ...string) *cluster {
	t.Helper()
	return startClusterEnv(t, nil, extra...)
}

func startClusterEnv(t *testing.T, env []string, extra ...string) *cluster {
	t.Helper()
	base := int(portBase.Add(10))
	c := &cluster{t: t, procs: map[uint64]*nemesis.Proc{}, real: map[uint64]string{},
		args: map[uint64][]string{}, logs: map[uint64]*os.File{}, env: env}
	for id := uint64(1); id <= 3; id++ {
		c.real[id] = fmt.Sprintf("127.0.0.1:%d", base+int(id))
		c.addrs = append(c.addrs, c.real[id])
	}
	nw, err := nemesis.NewNet(c.real)
	if err != nil {
		t.Fatal(err)
	}
	c.net = nw

	dir := t.TempDir()
	for id := uint64(1); id <= 3; id++ {
		peers := fmt.Sprintf("%d@%s", id, c.real[id])
		for to := uint64(1); to <= 3; to++ {
			if to != id {
				peers += fmt.Sprintf(",%d@%s", to, nw.PeerAddr(id, to))
			}
		}
		args := append([]string{
			"--id", fmt.Sprint(id),
			"--listen", c.real[id],
			"--peers", peers,
			"--data-dir", filepath.Join(dir, fmt.Sprint(id)),
		}, extra...)
		c.args[id] = args
		lf, err := os.Create(filepath.Join(dir, fmt.Sprintf("node-%d.log", id)))
		if err != nil {
			t.Fatal(err)
		}
		c.logs[id] = lf
		c.start(id)
	}
	t.Cleanup(func() {
		for _, p := range c.procs {
			p.Kill()
		}
		nw.Close()
		// On failure, surface each node's log tail — the post-mortem.
		if t.Failed() {
			for id, lf := range c.logs {
				lf.Sync()
				if data, err := os.ReadFile(lf.Name()); err == nil {
					tail := data
					if len(tail) > 2000 {
						tail = tail[len(tail)-2000:]
					}
					t.Logf("---- node %d log tail ----\n%s", id, tail)
				}
			}
		}
		for _, lf := range c.logs {
			lf.Close()
		}
	})
	return c
}

func (c *cluster) start(id uint64) {
	p, err := nemesis.StartProc(fmt.Sprintf("node-%d", id), c.logs[id], c.env, serverBin, c.args[id]...)
	if err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}
	c.procs[id] = p
}

// leaderOf probes each node with a direct linearizable Get; the one that
// answers is the leader (single-shard clusters). On failure it reports
// every node's last rejection — "no leader" without evidence is useless.
func (c *cluster) leaderOf(timeout time.Duration) uint64 {
	c.t.Helper()
	lastErr := map[uint64]string{}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, addr := range c.real {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				lastErr[id] = err.Error()
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err = kvv1.NewKVClient(conn).Get(ctx, &kvv1.GetRequest{Key: "leader-probe"})
			cancel()
			conn.Close()
			if err == nil {
				return id
			}
			lastErr[id] = err.Error()
		}
		time.Sleep(100 * time.Millisecond)
	}
	for id, e := range lastErr {
		c.t.Logf("node %d last probe error: %s", id, e)
	}
	c.t.Fatal("no leader found")
	return 0
}

// directPut / directGet talk to ONE node only — no retries, no rerouting.
func (c *cluster) directPut(id uint64, clientID, serial uint64, key, val string) error {
	conn, err := grpc.NewClient(c.real[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = kvv1.NewKVClient(conn).Put(ctx, &kvv1.PutRequest{Key: key, Value: []byte(val), ClientId: clientID, Serial: serial})
	return err
}

func (c *cluster) directGet(id uint64, key string) (string, error) {
	conn, err := grpc.NewClient(c.real[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := kvv1.NewKVClient(conn).Get(ctx, &kvv1.GetRequest{Key: key})
	if err != nil {
		return "", err
	}
	return string(resp.GetValue()), nil
}

// scenario runs a workload while fault() misbehaves, heals, verifies zero
// acked loss, and writes the history file. The current leader is probed
// BEFORE the storm starts (a mid-storm probe starves when other
// real-process suites share the machine) and handed to the fault.
func (c *cluster) scenario(name string, s int64, stormFor time.Duration, fault func(leader uint64)) {
	c.t.Helper()
	c.runScenario(name, s, stormFor, false, fault)
}

// scenarioShared contends every client on one key range (plus deletes);
// the linearizability checker is the sole verifier.
func (c *cluster) scenarioShared(name string, s int64, stormFor time.Duration, fault func(leader uint64)) {
	c.t.Helper()
	c.runScenario(name, s, stormFor, true, fault)
}

func (c *cluster) runScenario(name string, s int64, stormFor time.Duration, shared bool, fault func(leader uint64)) {
	c.t.Helper()
	leader := c.leaderOf(30 * time.Second)
	rec := nemesis.NewRecorder()
	w := &nemesis.Workload{
		Rec: rec, Seed: s, Clients: 3, KeysPerClient: 5,
		Addrs:      c.addrs,
		SharedKeys: shared,
		Resolve: func(hint string) (string, bool) {
			if owner, ok := c.net.ProxyOwner(hint); ok {
				return c.real[owner], true
			}
			return "", false
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), stormFor)
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()

	time.Sleep(stormFor / 4) // let the storm establish before the fault
	fault(leader)

	<-done
	cancel()
	c.net.Heal()
	time.Sleep(2 * time.Second) // converge

	if !shared { // per-client-key exact check; shared mode has no such oracle
		if err := w.Verify(); err != nil {
			c.t.Fatalf("%s: %v", name, err)
		}
	}

	// Evidence first: persist the history BEFORE judging it, so a failed
	// verdict always leaves the artifact that proves (or acquits) it.
	histDir := os.Getenv("NEMESIS_HISTORY_DIR")
	if histDir == "" {
		histDir = c.t.TempDir()
	}
	path := filepath.Join(histDir, name+".jsonl")
	if err := rec.WriteFile(path); err != nil {
		c.t.Fatalf("writing history: %v", err)
	}

	// The proof (M6 part 2): the whole recorded history must linearize
	// against the sequential KV model.
	lin, err := nemesis.CheckLinearizability(rec.Ops(), 2*time.Minute)
	if err != nil {
		c.t.Fatalf("%s: %v", name, err)
	}
	if !lin {
		key, kops := nemesis.FindViolatingKey(rec.Ops(), time.Minute)
		c.t.Logf("violating key %q (%d ops); full history: %s", key, len(kops), path)
		for i, op := range kops {
			c.t.Logf("  op[%d]: %+v", i, op)
		}
		c.t.Fatalf("%s: HISTORY IS NOT LINEARIZABLE — a real consistency violation; triage before touching consensus code", name)
	}
	ops, err := nemesis.ReadHistory(path)
	if err != nil || len(ops) == 0 {
		c.t.Fatalf("history round-trip: %d ops, err=%v", len(ops), err)
	}
	acked := 0
	for _, op := range ops {
		if op.Ok {
			acked++
		}
	}
	if acked == 0 {
		c.t.Fatalf("%s: nemesis ate every op — the workload never got through, nothing was tested", name)
	}
	c.t.Logf("%s: %d ops (%d acked) -> %s", name, len(ops), acked, path)
}

func TestNemesisLeaderKill(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process nemesis skipped in -short")
	}
	s := seed(t)
	c := startCluster(t)
	c.scenario("NemesisLeaderKill", s, 12*time.Second, func(leader uint64) {
		c.procs[leader].Kill()
		t.Logf("killed leader node %d", leader)
		time.Sleep(4 * time.Second) // survivors elect; storm continues
		c.start(leader)             // rejoin from its data dir
	})
}

func TestNemesisPartitionLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process nemesis skipped in -short")
	}
	s := seed(t)
	c := startCluster(t)
	c.scenario("NemesisPartitionLeader", s, 12*time.Second, func(leader uint64) {
		var others []uint64
		for id := range c.real {
			if id != leader {
				others = append(others, id)
			}
		}
		c.net.Partition([]uint64{leader}, others)
		t.Logf("partitioned leader %d away from %v", leader, others)
		time.Sleep(4 * time.Second)
		c.net.Heal() // old leader must step down and catch up
	})
}

func TestNemesisPartitionDuringSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process nemesis skipped in -short")
	}
	s := seed(t)
	// Small compaction threshold: the writes during the partition compact
	// the leader's log PAST the isolated follower's position, so healing
	// forces catch-up via InstallSnapshot, not plain appends. (64, not
	// something brutal like 16 — at storm rates that would snapshot every
	// few dozen milliseconds and starve the leader probe itself.)
	c := startCluster(t, "--compaction-threshold", "64")
	c.scenario("NemesisPartitionDuringSnapshot", s, 14*time.Second, func(leader uint64) {
		var follower uint64
		for id := range c.real {
			if id != leader {
				follower = id
			}
		}
		var rest []uint64
		for id := range c.real {
			if id != follower {
				rest = append(rest, id)
			}
		}
		c.net.Partition([]uint64{follower}, rest)
		t.Logf("isolated follower %d; storm now drives compaction past its log", follower)
		time.Sleep(6 * time.Second)
		c.net.Heal()
	})
}

// TestNemesisZombieLeader is where ReadIndex earns its keep: a leader is
// frozen (debug-API suspend / SIGSTOP), the cluster moves on and commits
// new writes, then the zombie thaws still believing it leads. A direct
// read from the zombie must NEVER return the pre-freeze value: its
// ReadBarrier cannot assemble a majority of fresh acks at its stale term.
func TestNemesisZombieLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process nemesis skipped in -short")
	}
	s := seed(t)
	c := startCluster(t)
	_ = s

	// Sentinel through the initial leader.
	zombie := c.leaderOf(10 * time.Second)
	if err := c.directPut(zombie, 99, 1, "zombie-key", "v1"); err != nil {
		t.Fatalf("sentinel v1: %v", err)
	}

	if err := c.procs[zombie].Suspend(); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	t.Logf("suspended leader %d (zombie)", zombie)

	// The survivors elect and commit v2 while the zombie sleeps.
	deadline := time.Now().Add(15 * time.Second)
	var newLeader uint64
	for time.Now().Before(deadline) {
		for id := range c.real {
			if id == zombie {
				continue
			}
			if err := c.directPut(id, 99, 2, "zombie-key", "v2"); err == nil {
				newLeader = id
			}
		}
		if newLeader != 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if newLeader == 0 {
		t.Fatal("survivors never elected a writable leader")
	}
	t.Logf("new leader %d committed v2 while zombie slept", newLeader)

	if err := c.procs[zombie].Resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	// Immediately read from the zombie, repeatedly, while it still thinks
	// it leads. Every response must be an error or v2 — v1 would be a
	// linearizability violation (stale read from a deposed leader).
	for i := 0; i < 20; i++ {
		got, err := c.directGet(zombie, "zombie-key")
		if err == nil && got == "v1" {
			t.Fatalf("ZOMBIE SERVED STALE READ: got v1 after v2 committed — ReadIndex failed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("zombie never served the stale value (ReadIndex held)")
}

// TestNemesisAsymmetricPartition cuts only the leader's OUTBOUND edges:
// it can be reached but cannot initiate. Its heartbeats die, followers
// time out and elect (their edges to each other and TO the old leader are
// intact), and the old leader adopts the new term through the vote
// request that reaches it. Connection-level asymmetry — see DESIGN.md for
// why this is the honest fault a userspace proxy can inject.
func TestNemesisAsymmetricPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process nemesis skipped in -short")
	}
	s := seed(t)
	c := startCluster(t)
	c.scenario("NemesisAsymmetricPartition", s, 12*time.Second, func(leader uint64) {
		for id := range c.real {
			if id != leader {
				c.net.Blackhole(leader, id)
			}
		}
		t.Logf("blackholed all outbound edges of leader %d", leader)
		time.Sleep(4 * time.Second)
		c.net.Heal()
	})
}

// TestNemesisRandomSoak runs a seeded random fault schedule — partitions,
// kills, suspends, delays — healing between rounds, then verifies zero
// acked loss. Gated behind NEMESIS_SOAK=1 (nightly/manual CI; too long
// for every push). NEMESIS_SOAK_SECONDS overrides the default 45s storm.
func TestNemesisRandomSoak(t *testing.T) {
	if os.Getenv("NEMESIS_SOAK") == "" {
		t.Skip("set NEMESIS_SOAK=1 to run the random soak")
	}
	s := seed(t)
	dur := 45 * time.Second
	if v := os.Getenv("NEMESIS_SOAK_SECONDS"); v != "" {
		if n, err := time.ParseDuration(v + "s"); err == nil {
			dur = n
		}
	}
	c := startCluster(t)
	rng := newRand(s)
	c.scenario("NemesisRandomSoak", s, dur, func(uint64) {
		soakDeadline := time.Now().Add(dur - 8*time.Second) // leave heal+converge room
		for time.Now().Before(soakDeadline) {
			victim := uint64(1 + rng.Intn(3))
			switch rng.Intn(4) {
			case 0:
				t.Logf("soak: kill -9 node %d", victim)
				c.procs[victim].Kill()
				time.Sleep(time.Duration(1+rng.Intn(3)) * time.Second)
				c.start(victim)
			case 1:
				var rest []uint64
				for id := range c.real {
					if id != victim {
						rest = append(rest, id)
					}
				}
				t.Logf("soak: partition node %d", victim)
				c.net.Partition([]uint64{victim}, rest)
				time.Sleep(time.Duration(1+rng.Intn(3)) * time.Second)
				c.net.Heal()
			case 2:
				t.Logf("soak: suspend node %d", victim)
				if err := c.procs[victim].Suspend(); err == nil {
					time.Sleep(time.Duration(1+rng.Intn(2)) * time.Second)
					c.procs[victim].Resume()
				}
			case 3:
				t.Logf("soak: 50ms delay on every edge")
				c.net.DelayAll(50 * time.Millisecond)
				time.Sleep(time.Duration(1+rng.Intn(2)) * time.Second)
				c.net.DelayAll(0)
			}
			time.Sleep(500 * time.Millisecond) // brief calm between faults
		}
	})

	// Suspicion rule: a soak that finds nothing must prove the nemesis
	// actually bit. Boot elects exactly once; kills/partitions/suspends
	// must have forced additional elections, visible in the node logs.
	elections := 0
	for id, lf := range c.logs {
		lf.Sync()
		data, err := os.ReadFile(lf.Name())
		if err != nil {
			continue
		}
		n := strings.Count(string(data), "won election")
		t.Logf("node %d: %d elections won", id, n)
		elections += n
	}
	if elections < 2 {
		t.Fatalf("soak ran but only %d election(s) occurred — the nemesis never bit; a green result would be meaningless", elections)
	}
	t.Logf("nemesis bit: %d elections across the soak (boot accounts for 1)", elections)
}

// TestNemesisSharedKeyContention: every client hammers the SAME small key
// range (plus deletes) while the leader is partitioned away — maximal
// write contention across a failover. There is no per-client oracle for
// multi-writer keys; the linearizability checker is the entire proof.
func TestNemesisSharedKeyContention(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process nemesis skipped in -short")
	}
	s := seed(t)
	c := startCluster(t)
	c.scenarioShared("NemesisSharedKeyContention", s, 12*time.Second, func(leader uint64) {
		var others []uint64
		for id := range c.real {
			if id != leader {
				others = append(others, id)
			}
		}
		c.net.Partition([]uint64{leader}, others)
		t.Logf("partitioned leader %d under shared-key contention", leader)
		time.Sleep(4 * time.Second)
		c.net.Heal()
	})
}

// writableNode finds the current leader by WRITE probe — the Get-based
// leaderOf is useless when the read barrier is deliberately disabled
// (every node happily answers reads; only the leader can commit).
func (c *cluster) writableNode(clientID uint64, serial *uint64, timeout time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id := range c.real {
			*serial++
			if err := c.directPut(id, clientID, *serial, "mut-probe", "x"); err == nil {
				return id
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatal("no writable node found")
	return 0
}

// TestMutationCheckerCatchesDisabledReadBarrier is the mutation check the
// plan demands: prove the checker CAN catch a broken build. The cluster
// runs with RAFTKV_UNSAFE_NO_READ_BARRIER=1 (Gets served straight from
// local state, no leadership confirmation). A deposed, partitioned leader
// then serves a provably stale read; the recorded history must be
// declared NON-linearizable. If the checker passes it, the checker is
// blind and this test fails.
func TestMutationCheckerCatchesDisabledReadBarrier(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process nemesis skipped in -short")
	}
	c := startClusterEnv(t, []string{"RAFTKV_UNSAFE_NO_READ_BARRIER=1"})
	rec := nemesis.NewRecorder()
	var probeSerial uint64

	old := c.writableNode(50, &probeSerial, 20*time.Second)

	// put k=1 through the current leader (recorded).
	invoke := rec.Now()
	probeSerial++
	if err := c.directPut(old, 50, probeSerial, "mut-k", "1"); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	rec.Record(nemesis.Op{Client: 50, Kind: "put", Key: "mut-k", Value: "1",
		Ok: true, Invoke: invoke, Return: rec.Now()})

	// Partition the leader away; commit k=2 on the surviving majority.
	var others []uint64
	for id := range c.real {
		if id != old {
			others = append(others, id)
		}
	}
	c.net.Partition([]uint64{old}, others)
	t.Logf("partitioned old leader %d; committing v2 on the majority", old)

	invoke = rec.Now()
	committed := false
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !committed {
		for _, id := range others {
			probeSerial++
			if err := c.directPut(id, 50, probeSerial, "mut-k", "2"); err == nil {
				committed = true
				break
			}
		}
	}
	if !committed {
		t.Fatal("majority never committed v2")
	}
	rec.Record(nemesis.Op{Client: 50, Kind: "put", Key: "mut-k", Value: "2",
		Ok: true, Invoke: invoke, Return: rec.Now()})

	// Read from the deposed leader: with the barrier disabled it serves
	// its stale local state. Record whatever it says.
	invoke = rec.Now()
	got, err := c.directGet(old, "mut-k")
	if err != nil {
		t.Fatalf("stale read attempt errored (%v) — mutation didn't bite; the read path must answer locally with the barrier off", err)
	}
	rec.Record(nemesis.Op{Client: 51, Kind: "get", Key: "mut-k",
		Found: true, Output: got, Ok: true, Invoke: invoke, Return: rec.Now()})
	if got != "1" {
		t.Fatalf("expected the deposed leader to serve stale v1, got %q — mutation didn't produce staleness", got)
	}
	t.Logf("deposed leader served stale read %q after v2 committed", got)

	lin, err := nemesis.CheckLinearizability(rec.Ops(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lin {
		for i, op := range rec.Ops() {
			t.Logf("op[%d]: %+v", i, op)
		}
		t.Fatal("CHECKER IS BLIND: it accepted a provably stale read")
	}
	t.Log("checker correctly rejected the stale-read history (mutation check passed)")
}
