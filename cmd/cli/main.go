// Command cli is a small client for the raftkv API.
//
//	kvcli [-addr host:port] put <key> <value>
//	kvcli [-addr host:port] get <key>
//	kvcli [-addr host:port] delete <key>
//
// Exit codes: 0 success, 1 error or key not found on get, 2 usage error.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

func main() {
	os.Exit(run())
}

func run() int {
	addr := flag.String("addr", "127.0.0.1:5001", "server address (flags go before the subcommand)")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		return 2
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvcli:", err)
		return 1
	}
	defer conn.Close()
	client := kvv1.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch cmd, rest := args[0], args[1:]; cmd {
	case "put":
		if len(rest) != 2 {
			return usageError("put needs exactly <key> <value>")
		}
		if _, err := client.Put(ctx, &kvv1.PutRequest{Key: rest[0], Value: []byte(rest[1])}); err != nil {
			return rpcError(err)
		}
		fmt.Println("OK")
		return 0

	case "get":
		if len(rest) != 1 {
			return usageError("get needs exactly <key>")
		}
		resp, err := client.Get(ctx, &kvv1.GetRequest{Key: rest[0]})
		if err != nil {
			return rpcError(err)
		}
		if !resp.GetFound() {
			fmt.Fprintln(os.Stderr, "kvcli: key not found")
			return 1
		}
		// Value bytes go to stdout untouched (plus a newline) so output
		// can be piped; kvcli does not assume values are text.
		os.Stdout.Write(resp.GetValue())
		fmt.Println()
		return 0

	case "delete":
		if len(rest) != 1 {
			return usageError("delete needs exactly <key>")
		}
		resp, err := client.Delete(ctx, &kvv1.DeleteRequest{Key: rest[0]})
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
	fmt.Fprint(os.Stderr, `usage: kvcli [-addr host:port] <command>

commands:
  put <key> <value>   store value under key
  get <key>           print the value stored under key
  delete <key>        remove key

flags:
`)
	flag.PrintDefaults()
}
