// Package crash_test is the Milestone 1 acceptance gate: it repeatedly
// hard-kills a real server process (the equivalent of kill -9) in the
// middle of a write storm and proves that after restart every write the
// server acknowledged is still there. The snapshot threshold is set
// absurdly low so the kill/restart cycles also cross snapshot and WAL
// rotation boundaries, not just plain log replay.
package crash_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

var serverBin string

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "raftkv-crash-*")
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
		fmt.Fprintln(os.Stderr, "building server for crash test:", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestAckedWritesSurviveHardKill(t *testing.T) {
	dataDir := t.TempDir()
	acked := make(map[string]string) // every write the server said OK to
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const rounds = 4
	for round := 0; round < rounds; round++ {
		addr := freeAddr(t)
		proc := startServer(t, addr, dataDir)
		client := dialAndWaitReady(t, addr)

		// Everything acknowledged before any previous kill must have
		// survived recovery.
		verifyAll(t, client, acked, round)

		// Hammer writes until the hard kill severs the connection. The
		// kill lands at a random moment, so across rounds it hits plain
		// appends, fsyncs, snapshot writes, and WAL rotations.
		killAfter := time.Duration(100+rng.Intn(400)) * time.Millisecond
		timer := time.AfterFunc(killAfter, func() { proc.Process.Kill() })

		for i := 0; ; i++ {
			key := fmt.Sprintf("r%d-k%05d", round, i)
			value := fmt.Sprintf("v%d-%d", round, rng.Int63())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.Put(ctx, &kvv1.PutRequest{Key: key, Value: []byte(value)})
			cancel()
			if err != nil {
				// The write in flight when the kill landed may or may not
				// have been persisted; both are correct because it was
				// never acknowledged. Only acked writes go in the map.
				break
			}
			acked[key] = value
		}
		timer.Stop()
		proc.Wait()
	}

	if len(acked) == 0 {
		t.Fatal("no writes were acknowledged before any kill; test exercised nothing")
	}

	// One final clean restart and full audit.
	addr := freeAddr(t)
	proc := startServer(t, addr, dataDir)
	defer func() { proc.Process.Kill(); proc.Wait() }()
	client := dialAndWaitReady(t, addr)
	verifyAll(t, client, acked, rounds)

	t.Logf("verified %d acknowledged writes across %d kill/restart cycles", len(acked), rounds)
}

func startServer(t *testing.T, addr, dataDir string) *exec.Cmd {
	t.Helper()
	// 4 KiB threshold forces frequent snapshots + rotations under load.
	proc := exec.Command(serverBin,
		"-listen", addr,
		"-data-dir", dataDir,
		"-snapshot-threshold-bytes", "4096",
	)
	proc.Stdout, proc.Stderr = os.Stdout, os.Stderr
	if err := proc.Start(); err != nil {
		t.Fatal(err)
	}
	return proc
}

func dialAndWaitReady(t *testing.T, addr string) kvv1.KVClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := kvv1.NewKVClient(conn)

	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := client.Get(ctx, &kvv1.GetRequest{Key: "__readiness_probe__"})
		cancel()
		if err == nil {
			return client
		}
		if time.Now().After(deadline) {
			t.Fatalf("server at %s never became ready: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func verifyAll(t *testing.T, client kvv1.KVClient, acked map[string]string, round int) {
	t.Helper()
	for key, want := range acked {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.Get(ctx, &kvv1.GetRequest{Key: key})
		cancel()
		if err != nil {
			t.Fatalf("round %d: Get(%s): %v", round, key, err)
		}
		if !resp.GetFound() {
			t.Fatalf("round %d: acknowledged write %s lost after crash", round, key)
		}
		if got := string(resp.GetValue()); got != want {
			t.Fatalf("round %d: %s = %q after recovery, want %q", round, key, got, want)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}
