// Command cli is a small client for the raftkv API.
//
//	kvcli [-addr host:port[,host:port...]] put <key> <value>
//	kvcli [-addr ...] get <key>
//	kvcli [-addr ...] delete <key>
//
// -addr may list several nodes; the CLI routes to the leader, following
// the leader hint a follower returns and retrying across elections, so it
// keeps working through a leader failover. Exit codes: 0 success, 1 error
// or key not found on get, 2 usage error.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

func main() {
	os.Exit(run())
}

func run() int {
	addr := flag.String("addr", "127.0.0.1:5001", "server address(es), comma-separated; the CLI follows leader hints")
	clientID := flag.Uint64("client", 0, "client id for exactly-once writes (0 = at-least-once)")
	serial := flag.Uint64("serial", 0, "monotonic write serial for this client id")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		return 2
	}

	addrs := strings.Split(*addr, ",")
	conns := map[string]kvv1.KVClient{}
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, "kvcli:", err)
			return 1
		}
		defer conn.Close()
		conns[a] = kvv1.NewKVClient(conn)
	}
	c := &leaderClient{conns: conns, order: addrs, cur: strings.TrimSpace(addrs[0]), shardLeader: map[string]string{}}

	switch cmd, rest := args[0], args[1:]; cmd {
	case "put":
		if len(rest) != 2 {
			return usageError("put needs exactly <key> <value>")
		}
		err := c.retry(func(kv kvv1.KVClient, ctx context.Context) error {
			_, e := kv.Put(ctx, &kvv1.PutRequest{Key: rest[0], Value: []byte(rest[1]), ClientId: *clientID, Serial: *serial})
			return e
		})
		if err != nil {
			return rpcError(err)
		}
		fmt.Println("OK")
		return 0

	case "get":
		if len(rest) != 1 {
			return usageError("get needs exactly <key>")
		}
		var resp *kvv1.GetResponse
		err := c.retry(func(kv kvv1.KVClient, ctx context.Context) error {
			r, e := kv.Get(ctx, &kvv1.GetRequest{Key: rest[0]})
			if e == nil {
				resp = r
			}
			return e
		})
		if err != nil {
			return rpcError(err)
		}
		if !resp.GetFound() {
			fmt.Fprintln(os.Stderr, "kvcli: key not found")
			return 1
		}
		os.Stdout.Write(resp.GetValue())
		fmt.Println()
		return 0

	case "delete":
		if len(rest) != 1 {
			return usageError("delete needs exactly <key>")
		}
		var resp *kvv1.DeleteResponse
		err := c.retry(func(kv kvv1.KVClient, ctx context.Context) error {
			r, e := kv.Delete(ctx, &kvv1.DeleteRequest{Key: rest[0], ClientId: *clientID, Serial: *serial})
			if e == nil {
				resp = r
			}
			return e
		})
		if err != nil {
			return rpcError(err)
		}
		if resp.GetExisted() {
			fmt.Println("deleted")
		} else {
			fmt.Println("key did not exist")
		}
		return 0

	default:
		return usageError("unknown command " + cmd)
	}
}

// leaderClient routes an RPC to the right shard's leader. Leaders are
// cached PER SHARD (learned from "shard=K ... leader_addr=A" hints in
// NotLeader rejections) and a cache entry is invalidated only when that
// same shard rejects again — one shard's failover never evicts another
// shard's good leader. With no cache entry it rotates through known nodes.
type leaderClient struct {
	conns       map[string]kvv1.KVClient
	order       []string
	cur         string
	shardLeader map[string]string // shard id -> leader addr
	opShard     string            // this op's shard, once a hint reveals it
}

func (c *leaderClient) retry(call func(kv kvv1.KVClient, ctx context.Context) error) error {
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		try := c.cur
		if a, ok := c.shardLeader[c.opShard]; ok && c.opShard != "" && c.conns[a] != nil {
			try = a
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := call(c.conns[try], ctx)
		cancel()
		if err == nil {
			c.cur = try
			return nil
		}
		lastErr = err
		if shard, addr, ok := leaderHint(err); ok {
			c.opShard = shard
			if try == c.shardLeader[shard] {
				delete(c.shardLeader, shard) // the cached leader itself rejected: stale
			}
			if addr != "" && c.conns[addr] != nil {
				c.shardLeader[shard] = addr
				continue // go straight to the named leader
			}
		}
		c.cur = c.next(try)
		time.Sleep(50 * time.Millisecond)
	}
	return lastErr
}

func (c *leaderClient) next(cur string) string {
	for i, a := range c.order {
		if strings.TrimSpace(a) == cur {
			return strings.TrimSpace(c.order[(i+1)%len(c.order)])
		}
	}
	return strings.TrimSpace(c.order[0])
}

// leaderHint parses "shard=K ... leader_addr=A" out of a NotLeader
// rejection. ok reports that this was a routable not-leader error (even if
// the leader is momentarily unknown and addr is empty).
func leaderHint(err error) (shard, addr string, ok bool) {
	st, stok := status.FromError(err)
	if !stok {
		return "", "", false
	}
	for _, tok := range strings.Fields(st.Message()) {
		switch {
		case strings.HasPrefix(tok, "shard="):
			shard, ok = strings.TrimPrefix(tok, "shard="), true
		case strings.HasPrefix(tok, "leader_addr="):
			addr = strings.TrimPrefix(tok, "leader_addr=")
		}
	}
	return shard, addr, ok
}

func usageError(msg string) int {
	fmt.Fprintln(os.Stderr, "kvcli:", msg)
	usage()
	return 2
}

func rpcError(err error) int {
	fmt.Fprintln(os.Stderr, "kvcli:", err)
	return 1
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: kvcli [-addr host:port[,host:port...]] [-client N -serial M] <command>

commands:
  put <key> <value>   store value under key
  get <key>           print the value stored under key
  delete <key>        remove key

flags:
`)
	flag.PrintDefaults()
}
