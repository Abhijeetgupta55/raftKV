package server

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Abhijeetgupta55/raftkv/internal/storage"
	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

func newTestServer() *KVServer {
	return New(storage.NewMemStore())
}

func TestPutGetDeleteRoundTrip(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	if _, err := s.Put(ctx, &kvv1.PutRequest{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.Get(ctx, &kvv1.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() || !bytes.Equal(got.GetValue(), []byte("v")) {
		t.Fatalf("Get(k) = %q, found=%v; want %q, true", got.GetValue(), got.GetFound(), "v")
	}

	del, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !del.GetExisted() {
		t.Fatal("Delete reported the key did not exist")
	}

	got, err = s.Get(ctx, &kvv1.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got.GetFound() {
		t.Fatal("Get found the key after Delete")
	}
}

func TestGetMissIsNotAnError(t *testing.T) {
	s := newTestServer()

	got, err := s.Get(context.Background(), &kvv1.GetRequest{Key: "absent"})
	if err != nil {
		t.Fatalf("Get on absent key returned error %v; a miss is a normal answer", err)
	}
	if got.GetFound() {
		t.Fatal("Get reported found for an absent key")
	}
}

func TestDeleteAbsentKeyIsIdempotent(t *testing.T) {
	s := newTestServer()

	del, err := s.Delete(context.Background(), &kvv1.DeleteRequest{Key: "absent"})
	if err != nil {
		t.Fatalf("Delete on absent key returned error %v", err)
	}
	if del.GetExisted() {
		t.Fatal("Delete reported existed=true for an absent key")
	}
}

func TestRequestValidation(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	tests := []struct {
		name string
		call func() error
	}{
		{"put empty key", func() error {
			_, err := s.Put(ctx, &kvv1.PutRequest{Key: "", Value: []byte("v")})
			return err
		}},
		{"get empty key", func() error {
			_, err := s.Get(ctx, &kvv1.GetRequest{Key: ""})
			return err
		}},
		{"delete empty key", func() error {
			_, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: ""})
			return err
		}},
		{"oversized key", func() error {
			_, err := s.Put(ctx, &kvv1.PutRequest{
				Key:   strings.Repeat("k", MaxKeyBytes+1),
				Value: []byte("v"),
			})
			return err
		}},
		{"oversized value", func() error {
			_, err := s.Put(ctx, &kvv1.PutRequest{
				Key:   "k",
				Value: make([]byte, MaxValueBytes+1),
			})
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("got %v; want InvalidArgument", err)
			}
		})
	}
}

func TestLimitBoundariesAreInclusive(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	_, err := s.Put(ctx, &kvv1.PutRequest{
		Key:   strings.Repeat("k", MaxKeyBytes),
		Value: make([]byte, MaxValueBytes),
	})
	if err != nil {
		t.Fatalf("Put at exactly the size limits failed: %v", err)
	}
}
