package nemesis

import (
	"testing"
	"time"
)

// Synthetic-history tests: the checker itself must be proven before real
// histories run through it — a model bug and a consensus bug look
// identical from the outside.

func TestCheckerAcceptsSequentialHistory(t *testing.T) {
	ops := []Op{
		{Client: 1, Kind: "put", Key: "k", Value: "1", Ok: true, Invoke: 0, Return: 10},
		{Client: 1, Kind: "get", Key: "k", Found: true, Output: "1", Ok: true, Invoke: 20, Return: 30},
		{Client: 1, Kind: "delete", Key: "k", Ok: true, Invoke: 40, Return: 50},
		{Client: 1, Kind: "get", Key: "k", Found: false, Ok: true, Invoke: 60, Return: 70},
	}
	ok, err := CheckLinearizability(ops, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("legal history rejected: ok=%v err=%v", ok, err)
	}
}

func TestCheckerFlagsStaleRead(t *testing.T) {
	// put(1) then put(2) complete strictly before the read begins; a read
	// of "1" after that is exactly what a deposed leader with no read
	// barrier serves. The checker MUST reject it.
	ops := []Op{
		{Client: 1, Kind: "put", Key: "k", Value: "1", Ok: true, Invoke: 0, Return: 10},
		{Client: 1, Kind: "put", Key: "k", Value: "2", Ok: true, Invoke: 20, Return: 30},
		{Client: 2, Kind: "get", Key: "k", Found: true, Output: "1", Ok: true, Invoke: 40, Return: 50},
	}
	ok, err := CheckLinearizability(ops, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("checker accepted a stale read — it cannot catch the naive-read bug")
	}
}

func TestCheckerAcceptsConcurrentOverlap(t *testing.T) {
	// Two overlapping puts: a read may see either order.
	ops := []Op{
		{Client: 1, Kind: "put", Key: "k", Value: "a", Ok: true, Invoke: 0, Return: 100},
		{Client: 2, Kind: "put", Key: "k", Value: "b", Ok: true, Invoke: 0, Return: 100},
		{Client: 3, Kind: "get", Key: "k", Found: true, Output: "a", Ok: true, Invoke: 200, Return: 300},
	}
	ok, err := CheckLinearizability(ops, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("legal concurrent history rejected: ok=%v err=%v", ok, err)
	}
}

func TestCheckerToleratesPhantomUnknownPut(t *testing.T) {
	// An unknown put that never took effect: reads keep seeing the old
	// value forever. Legal — the phantom linearizes after the last read.
	ops := []Op{
		{Client: 1, Kind: "put", Key: "k", Value: "1", Ok: true, Invoke: 0, Return: 10},
		{Client: 1, Kind: "put", Key: "k", Value: "2", Unknown: true, Invoke: 20, Return: 25},
		{Client: 2, Kind: "get", Key: "k", Found: true, Output: "1", Ok: true, Invoke: 30, Return: 40},
	}
	ok, err := CheckLinearizability(ops, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("phantom unknown put caused a false alarm: ok=%v err=%v", ok, err)
	}
}

func TestCheckerCatchesLostAckedWrite(t *testing.T) {
	// An ACKED write that a later read doesn't observe (and nothing else
	// wrote in between) is data loss; the checker must reject.
	ops := []Op{
		{Client: 1, Kind: "put", Key: "k", Value: "1", Ok: true, Invoke: 0, Return: 10},
		{Client: 2, Kind: "get", Key: "k", Found: false, Ok: true, Invoke: 20, Return: 30},
	}
	ok, err := CheckLinearizability(ops, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("checker accepted a lost acknowledged write")
	}
}
