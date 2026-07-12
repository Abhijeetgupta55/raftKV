package nemesis

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "github.com/Abhijeetgupta55/raftkv/proto/kv/v1"
)

// Workload drives concurrent session clients against a cluster while the
// nemesis misbehaves, recording every operation. Each client owns its key
// range ("c<id>-k<j>"), which makes the final expected state of every key
// exactly computable (see keyState) — the RUN-2 verification. Shared-key
// histories for the full linearizability checker are RUN-3 territory.
type Workload struct {
	Rec           *Recorder
	Seed          int64
	Clients       int
	KeysPerClient int
	Addrs         []string                         // real (direct) node addresses
	Resolve       func(hint string) (string, bool) // proxied hint addr -> real addr

	// SharedKeys makes every client contend on one global key range (and
	// mixes in deletes). Multi-writer keys have no per-client-computable
	// final state, so Verify() is unavailable — the linearizability
	// checker is the verification in this mode.
	SharedKeys bool

	mu    sync.Mutex
	state map[string]*keyState // key -> what the final value may legally be
}

// keyState tracks, per key, the last ACKNOWLEDGED write and every
// timed-out (unknown) write with a HIGHER serial. Session dedup applies
// serials at most once and never behind the applied high-water mark, so
// the final value must be the last ack — unless one of those later
// unknown writes actually committed. That whole set is the legal outcome.
type keyState struct {
	ackedSerial  uint64
	acked        string
	unknownLater map[uint64]string // serial -> value, for serials > ackedSerial
}

func (w *Workload) noteAck(key string, serial uint64, val string) {
	if w.SharedKeys {
		return // multi-writer keys: keyState logic does not apply
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.stateFor(key)
	if serial > st.ackedSerial {
		st.ackedSerial, st.acked = serial, val
		for s := range st.unknownLater {
			if s <= serial {
				delete(st.unknownLater, s)
			}
		}
	}
}

func (w *Workload) noteUnknown(key string, serial uint64, val string) {
	if w.SharedKeys {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.stateFor(key)
	if serial > st.ackedSerial {
		st.unknownLater[serial] = val
	}
}

func (w *Workload) stateFor(key string) *keyState {
	if w.state == nil {
		w.state = map[string]*keyState{}
	}
	st, ok := w.state[key]
	if !ok {
		st = &keyState{unknownLater: map[uint64]string{}}
		w.state[key] = st
	}
	return st
}

// conns dials every real address once; workers share the pool.
func (w *Workload) conns() (map[string]kvv1.KVClient, func(), error) {
	pool := map[string]kvv1.KVClient{}
	var closers []func() error
	for _, a := range w.Addrs {
		c, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		pool[a] = kvv1.NewKVClient(c)
		closers = append(closers, c.Close)
	}
	return pool, func() {
		for _, c := range closers {
			c()
		}
	}, nil
}

// Run storms until ctx is done. Client c is session client_id c+1; its rng
// is derived from Seed and c, so a seed reproduces the exact op sequence.
func (w *Workload) Run(ctx context.Context) error {
	pool, closeAll, err := w.conns()
	if err != nil {
		return err
	}
	defer closeAll()

	var wg sync.WaitGroup
	for c := 0; c < w.Clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			w.client(ctx, pool, c)
		}(c)
	}
	wg.Wait()
	return nil
}

func (w *Workload) client(ctx context.Context, pool map[string]kvv1.KVClient, c int) {
	rng := rand.New(rand.NewSource(w.Seed + int64(c)*7919))
	clientID := uint64(c + 1)
	cur := w.Addrs[rng.Intn(len(w.Addrs))]
	var serial uint64

	for op := 0; ctx.Err() == nil; op++ {
		key := fmt.Sprintf("c%d-k%d", c, rng.Intn(w.KeysPerClient))
		if w.SharedKeys {
			key = fmt.Sprintf("shared-k%d", rng.Intn(w.KeysPerClient))
		}
		switch pick := rng.Intn(100); {
		case pick < 55: // put
			serial++
			val := fmt.Sprintf("c%d-s%d", c, serial)
			cur = w.doPut(ctx, pool, cur, clientID, serial, key, val)
		case w.SharedKeys && pick < 65: // delete (shared mode only)
			serial++
			cur = w.doDelete(ctx, pool, cur, clientID, serial, key)
		default: // get
			cur = w.doGet(ctx, pool, cur, c, key)
		}
	}
}

// doDelete mirrors doPut: session-deduped, folded retries, unknown on
// overall timeout. Only used in shared mode, where Porcupine (not
// keyState) is the verifier — deletes are not tracked in keyState.
func (w *Workload) doDelete(ctx context.Context, pool map[string]kvv1.KVClient, cur string, clientID, serial uint64, key string) string {
	invoke := w.Rec.Now()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := pool[cur].Delete(attempt, &kvv1.DeleteRequest{
			Key: key, ClientId: clientID, Serial: serial,
		})
		cancel()
		if err == nil {
			w.Rec.Record(Op{Client: int(clientID), Kind: "delete", Key: key,
				Ok: true, Invoke: invoke, Return: w.Rec.Now()})
			return cur
		}
		cur = w.nextAddr(pool, cur, err)
	}
	w.Rec.Record(Op{Client: int(clientID), Kind: "delete", Key: key,
		Unknown: true, Invoke: invoke, Return: w.Rec.Now()})
	return cur
}

// doPut drives one session write to ack or overall timeout, folding
// retries into a single recorded op (dedup makes them one application).
func (w *Workload) doPut(ctx context.Context, pool map[string]kvv1.KVClient, cur string, clientID, serial uint64, key, val string) string {
	invoke := w.Rec.Now()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := pool[cur].Put(attempt, &kvv1.PutRequest{
			Key: key, Value: []byte(val), ClientId: clientID, Serial: serial,
		})
		cancel()
		if err == nil {
			w.Rec.Record(Op{Client: int(clientID), Kind: "put", Key: key, Value: val,
				Ok: true, Invoke: invoke, Return: w.Rec.Now()})
			w.noteAck(key, serial, val)
			return cur
		}
		cur = w.nextAddr(pool, cur, err)
	}
	w.Rec.Record(Op{Client: int(clientID), Kind: "put", Key: key, Value: val,
		Unknown: true, Invoke: invoke, Return: w.Rec.Now()})
	w.noteUnknown(key, serial, val)
	return cur
}

func (w *Workload) doGet(ctx context.Context, pool map[string]kvv1.KVClient, cur string, c int, key string) string {
	invoke := w.Rec.Now()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := pool[cur].Get(attempt, &kvv1.GetRequest{Key: key})
		cancel()
		if err == nil {
			w.Rec.Record(Op{Client: c + 1, Kind: "get", Key: key,
				Found: resp.GetFound(), Output: string(resp.GetValue()),
				Ok: true, Invoke: invoke, Return: w.Rec.Now()})
			return cur
		}
		cur = w.nextAddr(pool, cur, err)
	}
	w.Rec.Record(Op{Client: c + 1, Kind: "get", Key: key,
		Unknown: true, Invoke: invoke, Return: w.Rec.Now()})
	return cur
}

// nextAddr follows a resolvable leader hint or rotates to the next node.
func (w *Workload) nextAddr(pool map[string]kvv1.KVClient, cur string, err error) string {
	if err != nil {
		for _, tok := range strings.Fields(err.Error()) {
			if strings.HasPrefix(tok, "leader_addr=") {
				hint := strings.TrimPrefix(tok, "leader_addr=")
				if w.Resolve != nil {
					if real, ok := w.Resolve(hint); ok && pool[real] != nil {
						return real
					}
				}
				if pool[hint] != nil {
					return hint
				}
			}
		}
	}
	for i, a := range w.Addrs {
		if a == cur {
			return w.Addrs[(i+1)%len(w.Addrs)]
		}
	}
	return w.Addrs[0]
}

// Verify reads every touched key back (linearizable reads, healed
// cluster) and checks the value is a legal final state: the last
// acknowledged write, or a later write whose ack the nemesis ate.
// Zero acknowledged loss, precisely stated.
func (w *Workload) Verify() error {
	if w.SharedKeys {
		return fmt.Errorf("Verify is per-client-key only; shared-key runs are verified by the linearizability checker")
	}
	pool, closeAll, err := w.conns()
	if err != nil {
		return err
	}
	defer closeAll()

	w.mu.Lock()
	keys := make([]string, 0, len(w.state))
	for k := range w.state {
		keys = append(keys, k)
	}
	w.mu.Unlock()

	cur := w.Addrs[0]
	for _, key := range keys {
		var got string
		var found bool
		deadline := time.Now().Add(20 * time.Second)
		ok := false
		for time.Now().Before(deadline) {
			attempt, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := pool[cur].Get(attempt, &kvv1.GetRequest{Key: key})
			cancel()
			if err == nil {
				got, found, ok = string(resp.GetValue()), resp.GetFound(), true
				break
			}
			cur = w.nextAddr(pool, cur, err)
			time.Sleep(50 * time.Millisecond)
		}
		if !ok {
			return fmt.Errorf("verify: cluster unavailable reading %s", key)
		}

		w.mu.Lock()
		st := w.state[key]
		legal := map[string]bool{}
		if st.ackedSerial > 0 {
			legal[st.acked] = true
		}
		for _, v := range st.unknownLater {
			legal[v] = true
		}
		hadAck := st.ackedSerial > 0
		w.mu.Unlock()

		switch {
		case !found && hadAck:
			return fmt.Errorf("verify: acked key %s vanished (last ack %q)", key, st.acked)
		case found && len(legal) > 0 && !legal[got]:
			return fmt.Errorf("verify: key %s = %q, not a legal final state %v", key, got, legal)
		}
	}
	return nil
}
