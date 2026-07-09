package kvraft

import (
	"testing"

	"github.com/Abhijeetgupta55/raftkv/internal/storage"
)

func putCmd(clientID, serial uint64, key, val string) []byte {
	return encodeCommand(command{
		clientID: clientID, serial: serial,
		inner: storage.EncodeCommand(storage.Command{Op: storage.OpPut, Key: key, Value: []byte(val)}),
	})
}

// TestRetryWithoutSessionClobbers DEMONSTRATES the hazard exactly-once
// exists to prevent: a client's write times out and is retried, but in the
// gap another client overwrote the key. Replaying the first write (no
// session id) reverts the newer value — a lost update. This is the "before"
// half of the demonstrated-then-fixed pair; if this test ever starts
// passing with the value at v2, dedup has silently become unnecessary and
// the pairing should be revisited.
func TestRetryWithoutSessionClobbers(t *testing.T) {
	sm := newStateMachine()
	sm.Apply(1, putCmd(0, 0, "k", "v1")) // client A's write
	sm.Apply(2, putCmd(0, 0, "k", "v2")) // client B overwrites
	sm.Apply(3, putCmd(0, 0, "k", "v1")) // client A's retry replays

	got, _ := sm.get("k")
	if string(got) != "v1" {
		t.Fatalf("expected the stale retry to clobber to v1 (the hazard), got %q", got)
	}
}

// TestSessionDedupPreventsStaleRetry is the "after" half: the same
// scenario, but client A carries a stable (client_id, serial). The state
// machine has already applied serial 1 for client 7, so the replay is a
// no-op and client B's newer value survives. Exactly-once.
func TestSessionDedupPreventsStaleRetry(t *testing.T) {
	sm := newStateMachine()
	sm.Apply(1, putCmd(7, 1, "k", "v1")) // client 7, write #1
	sm.Apply(2, putCmd(8, 1, "k", "v2")) // client 8 overwrites
	sm.Apply(3, putCmd(7, 1, "k", "v1")) // client 7 retries write #1 — deduped

	got, _ := sm.get("k")
	if string(got) != "v2" {
		t.Fatalf("dedup failed: stale retry reverted the key to %q, want v2", got)
	}
}

// TestSnapshotIncludesSessions proves the session table survives a
// snapshot/restore, so dedup keeps working across log compaction and
// InstallSnapshot — otherwise a restored replica would forget applied
// serials and re-apply a duplicate that arrives after the snapshot.
func TestSnapshotIncludesSessions(t *testing.T) {
	sm := newStateMachine()
	sm.Apply(1, putCmd(7, 1, "k", "v1"))
	sm.Apply(2, putCmd(8, 1, "j", "w1"))

	img, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := newStateMachine()
	if err := restored.Restore(img); err != nil {
		t.Fatal(err)
	}
	// A duplicate of client 7's already-applied serial 1 must be ignored
	// even though it arrives only after the restore.
	restored.Apply(3, putCmd(7, 1, "k", "STALE"))
	if got, _ := restored.get("k"); string(got) != "v1" {
		t.Fatalf("restored replica re-applied a duplicate: k=%q, want v1", got)
	}
	if got, _ := restored.get("j"); string(got) != "w1" {
		t.Fatalf("restore lost data: j=%q, want w1", got)
	}
}
