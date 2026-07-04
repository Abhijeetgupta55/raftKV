package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	s := NewMemStore()

	if _, found := s.Get("missing"); found {
		t.Fatal("Get on empty store reported found")
	}

	s.Put("k", []byte("v1"))
	got, found := s.Get("k")
	if !found || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get(k) = %q, %v; want %q, true", got, found, "v1")
	}

	s.Put("k", []byte("v2"))
	got, _ = s.Get("k")
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("after overwrite, Get(k) = %q; want %q", got, "v2")
	}

	if existed := s.Delete("k"); !existed {
		t.Fatal("Delete(k) reported the key did not exist")
	}
	if _, found := s.Get("k"); found {
		t.Fatal("Get(k) found the key after Delete")
	}
	if existed := s.Delete("k"); existed {
		t.Fatal("second Delete(k) reported the key still existed")
	}
}

func TestEmptyValueIsDistinctFromAbsent(t *testing.T) {
	s := NewMemStore()
	s.Put("k", nil)

	got, found := s.Get("k")
	if !found {
		t.Fatal("key with empty value reported as absent")
	}
	if len(got) != 0 {
		t.Fatalf("Get(k) = %q; want empty", got)
	}
}

func TestCallerCannotMutateStoredData(t *testing.T) {
	s := NewMemStore()

	in := []byte("original")
	s.Put("k", in)
	in[0] = 'X' // mutate the slice passed to Put

	got, _ := s.Get("k")
	if !bytes.Equal(got, []byte("original")) {
		t.Fatalf("mutating Put input corrupted the store: got %q", got)
	}

	got[0] = 'Y' // mutate the slice returned by Get
	again, _ := s.Get("k")
	if !bytes.Equal(again, []byte("original")) {
		t.Fatalf("mutating Get output corrupted the store: got %q", again)
	}
}

// TestConcurrentAccess is only meaningful under `go test -race`, which CI
// always runs; without the race detector it merely exercises the locks.
func TestConcurrentAccess(t *testing.T) {
	s := NewMemStore()
	const goroutines = 8
	const opsEach = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("key-%d", i%10)
				s.Put(key, []byte(fmt.Sprintf("g%d-i%d", g, i)))
				s.Get(key)
				if i%3 == 0 {
					s.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()
}
