// Command server runs one raftkv node.
//
// Standalone (durable single-node engine — WAL + snapshots, no --peers):
//
//	server --listen 127.0.0.1:5001 --data-dir data
//
// Cluster (Raft replication; run one per terminal, same --peers on each):
//
//	server --id 1 --listen 127.0.0.1:5001 \
//	  --peers 1@127.0.0.1:5001,2@127.0.0.1:5002,3@127.0.0.1:5003 --data-dir data/n1
//
// With --peers the node exposes both the client-facing kv.v1 API (backed
// by a Raft-replicated state machine) and the internal raft.v1 API peers
// use to replicate. Without --peers it serves kv.v1 from the M1 durable
// store directly — same binary, no consensus overhead for a single node.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Abhijeetgupta55/raftkv/internal/kvraft"
	"github.com/Abhijeetgupta55/raftkv/internal/raft"
	"github.com/Abhijeetgupta55/raftkv/internal/server"
	"github.com/Abhijeetgupta55/raftkv/internal/storage"
	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
	raftv1 "github.com/Abhijeetgupta55/raftkv/proto/raft/v1"
)

func main() {
	id := flag.Uint64("id", 1, "this node's Raft id (1-based); used with --peers")
	listenAddr := flag.String("listen", "127.0.0.1:5001", "address to serve the gRPC API(s) on")
	peers := flag.String("peers", "", "comma-separated id@addr of all members incl. self; empty = standalone durable node")
	dataDir := flag.String("data-dir", "data", "directory for the log and snapshots")
	snapThreshold := flag.Int64("snapshot-threshold-bytes", 0, "standalone only: WAL size that triggers a snapshot (0 = default)")
	shards := flag.Int("shards", 1, "cluster only: number of Raft groups the keyspace partitions across (fixed at first boot)")
	compactAt := flag.Uint64("compaction-threshold", 0, "cluster only: applied entries between snapshots (0 = default 4096; tests use tiny values)")
	flag.Parse()

	var err error
	if strings.TrimSpace(*peers) == "" {
		err = runStandalone(*listenAddr, *dataDir, *snapThreshold)
	} else {
		var members map[uint64]string
		members, err = parseMembers(*peers, *id, *listenAddr)
		if err == nil {
			err = runCluster(*id, *listenAddr, *dataDir, members, *shards, *compactAt)
		}
	}
	if err != nil {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}

// runStandalone is the unchanged Milestone 1 path: the durable single-node
// store behind the KV API. No Raft, no election — just WAL + snapshots.
func runStandalone(listenAddr, dataDir string, snapThreshold int64) error {
	store, err := storage.Open(dataDir, storage.Options{SnapshotThresholdBytes: snapThreshold})
	if err != nil {
		return fmt.Errorf("open storage in %s: %w", dataDir, err)
	}
	defer store.Close()
	slog.Info("storage recovered", "data_dir", dataDir)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	kvv1.RegisterKVServer(grpcServer, server.New(store))
	serveUntilSignal(grpcServer, lis)
	slog.Info("serving KV API (standalone durable)", "addr", lis.Addr().String())
	return grpcServer.Serve(lis)
}

// parseMembers turns "1@addr1,2@addr2" into an id->addr map. An empty spec
// means a single-node cluster of just this node.
func parseMembers(spec string, selfID uint64, selfAddr string) (map[uint64]string, error) {
	members := map[uint64]string{}
	if strings.TrimSpace(spec) == "" {
		members[selfID] = selfAddr
		return members, nil
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		at := strings.IndexByte(part, '@')
		if at < 0 {
			return nil, fmt.Errorf("member %q is not id@addr", part)
		}
		pid, err := strconv.ParseUint(part[:at], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("member %q: %w", part, err)
		}
		members[pid] = part[at+1:]
	}
	if _, ok := members[selfID]; !ok {
		return nil, fmt.Errorf("--peers must include this node's own id %d", selfID)
	}
	return members, nil
}

// runCluster hosts one replica of every shard (Raft group) in this
// process. shards=1 is the unsharded cluster — same runtime, one group.
func runCluster(id uint64, listenAddr, dataDir string, members map[uint64]string, shards int, compactAt uint64) error {
	transport := raft.NewGRPCTransport(members)
	defer transport.Close()

	srv, err := kvraft.NewSharded(raft.NodeConfig{
		Config: raft.Config{
			ID:               id,
			Members:          members,
			ElectionTicksMin: 10,
			ElectionTicksMax: 20,
			HeartbeatTicks:   3,
			Logger:           slog.Default(),
		},
		Transport:           transport,
		RPCTimeout:          time.Second,
		CompactionThreshold: compactAt,
	}, shards, dataDir, 30*time.Millisecond)
	if err != nil {
		return fmt.Errorf("start raft groups: %w", err)
	}
	defer srv.Stop()

	// Mutation-check hook: deliberately breaks read linearizability so the
	// verification harness can prove its checker catches a broken build.
	// NEVER set this outside test/nemesis.
	if os.Getenv("RAFTKV_UNSAFE_NO_READ_BARRIER") == "1" {
		slog.Error("UNSAFE MODE: read barrier disabled — reads are NOT linearizable (mutation-check hook)")
		srv.SetUnsafeNoReadBarrier(true)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	kvv1.RegisterKVServer(grpcServer, srv.KV)
	raftv1.RegisterRaftServer(grpcServer, raft.NewService(srv.Nodes()))

	serveUntilSignal(grpcServer, lis)
	slog.Info("raftkv node serving", "id", id, "addr", lis.Addr().String(), "members", len(members), "shards", shards)
	return grpcServer.Serve(lis)
}

// serveUntilSignal wires graceful shutdown on Ctrl-C / SIGTERM.
func serveUntilSignal(grpcServer *grpc.Server, lis net.Listener) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-stop
		slog.Info("shutting down", "signal", sig.String())
		grpcServer.GracefulStop()
	}()
}
