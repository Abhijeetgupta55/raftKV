package nemesis

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Op is one client operation in the recorded history, in the shape a
// linearizability checker consumes: who did what, and the [Invoke, Return]
// wall-clock window inside which its linearization point must fall.
//
// Timeout semantics matter: an op whose final attempt timed out has
// Unknown=true — it MAY have taken effect (the command can commit after
// the client gave up), so the checker must treat it as possibly-applied
// with an open window. Retries of a session write are folded into ONE op
// (dedup makes them a single logical application): Invoke is the first
// attempt's start, Return the final ack.
type Op struct {
	Client  int    `json:"client"`
	Kind    string `json:"kind"` // put | get | delete
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`  // put: written value
	Found   bool   `json:"found"`            // get: key existed
	Output  string `json:"output,omitempty"` // get: value read
	Ok      bool   `json:"ok"`               // acknowledged success
	Unknown bool   `json:"unknown"`          // timed out: outcome unresolved
	Invoke  int64  `json:"invoke"`           // ns since history start
	Return  int64  `json:"return"`           // ns since history start
}

// Recorder collects ops concurrently and writes them as JSON lines.
type Recorder struct {
	mu    sync.Mutex
	start time.Time
	last  atomic.Int64
	ops   []Op
}

func NewRecorder() *Recorder { return &Recorder{start: time.Now()} }

// Now is the recorder's clock: nanoseconds since history start, made
// STRICTLY monotonic across all callers (war story WS-2). Coarse OS
// clocks (~0.5ms on Windows) stamp back-to-back events identically, and
// timestamp ties make the linearizability checker either blind (touching
// intervals read as concurrent) or, if patched with interval transforms,
// unsound (sub-tick ops get inverted windows). The bump is sound because
// the CAS serializes the stamping events in their true order, Return
// stamps are upper bounds of completions, and Invoke stamps are lower
// bounds of submissions — so stamp order implies a valid real-time order.
func (r *Recorder) Now() int64 {
	for {
		now := time.Since(r.start).Nanoseconds()
		last := r.last.Load()
		if now <= last {
			now = last + 1
		}
		if r.last.CompareAndSwap(last, now) {
			return now
		}
	}
}

func (r *Recorder) Record(op Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// Ops returns a copy of everything recorded so far.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.ops...)
}

// WriteFile persists the history as one JSON object per line.
func (r *Recorder) WriteFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, op := range r.Ops() {
		if err := enc.Encode(op); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ReadHistory loads a JSONL history file back — the checker's entry point
// and the round-trip guarantee that the on-disk format is usable.
func ReadHistory(path string) ([]Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ops []Op
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		var op Op
		if err := json.Unmarshal(sc.Bytes(), &op); err != nil {
			return nil, fmt.Errorf("history line %d: %w", len(ops)+1, err)
		}
		ops = append(ops, op)
	}
	return ops, sc.Err()
}
