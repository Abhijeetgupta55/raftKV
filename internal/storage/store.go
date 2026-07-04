package storage

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// DurableStore is the persistent storage engine: a MemStore whose every
// mutation is appended to a WAL and fsynced before being acknowledged,
// with periodic snapshots bounding recovery time and WAL growth.
//
// Recovery on Open is: load newest snapshot, replay WAL records with
// seq > snapshot's lastSeq, truncate any torn tail, serve. Replay is
// deterministic (same records in, same map out), so recovering twice
// from the same files yields the same state.
type DurableStore struct {
	mem *MemStore

	// mu serializes mutations: sequence numbering, the WAL append, the
	// state-machine apply, and snapshotting must happen as a unit.
	// Reads bypass it entirely and hit MemStore's own RWMutex.
	mu     sync.Mutex
	wal    *wal
	seq    uint64
	cmdBuf []byte

	snapDir           string
	snapshotThreshold int64
}

// Options configures a DurableStore. The zero value picks defaults.
type Options struct {
	// SnapshotThresholdBytes triggers a snapshot (and WAL truncation)
	// once the active WAL segment exceeds this size. Default 16 MiB.
	SnapshotThresholdBytes int64
}

const defaultSnapshotThreshold = 16 << 20

// Open recovers (or initializes) a store rooted at dataDir, which gets
// two subdirectories: wal/ and snap/.
func Open(dataDir string, opts Options) (*DurableStore, error) {
	if opts.SnapshotThresholdBytes <= 0 {
		opts.SnapshotThresholdBytes = defaultSnapshotThreshold
	}
	walDir := filepath.Join(dataDir, "wal")
	snapDir := filepath.Join(dataDir, "snap")
	for _, d := range []string{walDir, snapDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}

	mem, snapSeq, err := loadNewestSnapshot(snapDir)
	if err != nil {
		return nil, fmt.Errorf("recover snapshot: %w", err)
	}

	lastSeq, newestValidSize, err := replayWAL(walDir, snapSeq, func(seq uint64, command []byte) error {
		cmd, err := decodeCommand(command)
		if err != nil {
			return fmt.Errorf("wal record seq %d: %w", seq, err)
		}
		applyCommand(mem, cmd)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("replay wal: %w", err)
	}

	// Reopen the newest segment for appending, first truncating any torn
	// tail replay found. With no segments at all, start the log at seq+1.
	segs, err := listSegments(walDir)
	if err != nil {
		return nil, err
	}
	var w *wal
	if len(segs) == 0 {
		w, err = createWAL(walDir, lastSeq+1)
	} else {
		newest := segs[len(segs)-1]
		if err := os.Truncate(newest.path, newestValidSize); err != nil {
			return nil, fmt.Errorf("truncate torn wal tail: %w", err)
		}
		w, err = openWALForAppend(walDir, newest.startSeq, newestValidSize)
	}
	if err != nil {
		return nil, err
	}

	return &DurableStore{
		mem:               mem,
		wal:               w,
		seq:               lastSeq,
		snapDir:           snapDir,
		snapshotThreshold: opts.SnapshotThresholdBytes,
	}, nil
}

// applyCommand advances the state machine by one committed command. Both
// live writes and WAL replay funnel through here so they can never
// disagree about what a command means.
func applyCommand(mem *MemStore, cmd Command) (existed bool) {
	switch cmd.Op {
	case OpPut:
		mem.Put(cmd.Key, cmd.Value)
		return false
	case OpDelete:
		return mem.Delete(cmd.Key)
	default:
		// decodeCommand rejects unknown ops; reaching here is a bug.
		panic(fmt.Sprintf("storage: applyCommand on unknown op %d", cmd.Op))
	}
}

// Put durably stores value under key: WAL append + fsync first, then the
// in-memory apply. If the append fails the state machine is untouched
// and the WAL refuses further writes (its on-disk state is unknown), so
// an error here means "not stored", never "maybe stored".
func (s *DurableStore) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logAndApply(Command{Op: OpPut, Key: key, Value: value})
}

// Delete durably removes key, reporting whether it existed.
func (s *DurableStore) Delete(key string) (existed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// An absent key needs no log entry: deleting it changes nothing, and
	// skipping the append keeps the WAL free of no-op tombstones.
	if _, found := s.mem.Get(key); !found {
		return false, nil
	}
	if err := s.logAndApply(Command{Op: OpDelete, Key: key}); err != nil {
		return false, err
	}
	return true, nil
}

// Get serves straight from memory; reads need no disk I/O because every
// acknowledged mutation is already applied to mem.
func (s *DurableStore) Get(key string) (value []byte, found bool) {
	return s.mem.Get(key)
}

// logAndApply is the write path core: assign seq, persist, apply.
// Callers must hold mu.
func (s *DurableStore) logAndApply(cmd Command) error {
	s.cmdBuf = encodeCommand(s.cmdBuf[:0], cmd)
	if err := s.wal.Append(s.seq+1, s.cmdBuf); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	s.seq++
	applyCommand(s.mem, cmd)
	s.maybeSnapshot()
	return nil
}

// maybeSnapshot snapshots once the active segment outgrows the threshold.
// A snapshot failure is logged, not returned: the write that triggered it
// is already durable in the WAL, so correctness is unaffected — the WAL
// just keeps growing until a later attempt succeeds.
func (s *DurableStore) maybeSnapshot() {
	if s.wal.size < s.snapshotThreshold {
		return
	}
	if err := s.snapshot(); err != nil {
		slog.Warn("snapshot failed; wal will keep growing until one succeeds", "err", err)
	}
}

// snapshot dumps the state machine, rotates the WAL, and deletes
// now-covered segments and stale snapshots. Callers must hold mu, so the
// dump is a consistent point-in-time image at s.seq.
func (s *DurableStore) snapshot() error {
	if err := writeSnapshot(s.snapDir, s.seq, s.mem); err != nil {
		return err
	}
	if err := s.wal.rotate(s.seq + 1); err != nil {
		return err
	}

	// Everything <= s.seq is now covered by the snapshot; older segments
	// and snapshots are dead weight. Deletion failures are retried
	// implicitly by the next snapshot.
	segs, err := listSegments(s.wal.dir)
	if err != nil {
		return err
	}
	for _, seg := range segs[:len(segs)-1] { // all but the active segment
		if err := os.Remove(seg.path); err != nil {
			return err
		}
	}
	return pruneSnapshots(s.snapDir, 2)
}

// Snapshot forces an immediate snapshot, primarily for tests and, later,
// admin tooling.
func (s *DurableStore) Snapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot()
}

// Close releases the WAL file handle. It does not snapshot: recovery from
// the existing snapshot + WAL is always sufficient.
func (s *DurableStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}
