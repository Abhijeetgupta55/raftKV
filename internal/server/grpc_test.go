package server

import (
	"bytes"
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Abhijeetgupta55/raftkv/internal/storage"
	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

// TestOverRealGRPCConnection exercises the full wire path — proto
// marshaling, the gRPC server plumbing, status propagation — over an
// in-memory connection, so a registration or codec mistake can't hide
// behind direct method calls.
func TestOverRealGRPCConnection(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	kvv1.RegisterKVServer(grpcServer, New(storage.NewMemStore()))
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := kvv1.NewKVClient(conn)
	ctx := context.Background()

	if _, err := client.Put(ctx, &kvv1.PutRequest{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Put over gRPC: %v", err)
	}

	got, err := client.Get(ctx, &kvv1.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get over gRPC: %v", err)
	}
	if !got.GetFound() || !bytes.Equal(got.GetValue(), []byte("v")) {
		t.Fatalf("Get(k) = %q, found=%v; want %q, true", got.GetValue(), got.GetFound(), "v")
	}

	del, err := client.Delete(ctx, &kvv1.DeleteRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Delete over gRPC: %v", err)
	}
	if !del.GetExisted() {
		t.Fatal("Delete over gRPC reported the key did not exist")
	}
}
