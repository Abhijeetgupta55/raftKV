package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTermAndVoteRoundTrip(t *testing.T) {
	dir := t.TempDir()

	term, votedFor, err := loadTermAndVote(dir)
	if err != nil {
		t.Fatal(err)
	}
	if term != 0 || votedFor != noVote {
		t.Fatalf("fresh node loaded term=%d votedFor=%d, want zeros", term, votedFor)
	}

	if err := saveTermAndVote(dir, 7, 2); err != nil {
		t.Fatal(err)
	}
	term, votedFor, err = loadTermAndVote(dir)
	if err != nil {
		t.Fatal(err)
	}
	if term != 7 || votedFor != 2 {
		t.Fatalf("loaded term=%d votedFor=%d, want 7, 2", term, votedFor)
	}

	// A later term with no vote yet must fully replace the old record.
	if err := saveTermAndVote(dir, 8, noVote); err != nil {
		t.Fatal(err)
	}
	term, votedFor, err = loadTermAndVote(dir)
	if err != nil {
		t.Fatal(err)
	}
	if term != 8 || votedFor != noVote {
		t.Fatalf("loaded term=%d votedFor=%d, want 8, noVote", term, votedFor)
	}
}

func TestCorruptStateFileIsFatal(t *testing.T) {
	dir := t.TempDir()
	if err := saveTermAndVote(dir, 3, 1); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, stateFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(stateMagic)+2] ^= 0xFF // flip a bit inside the term
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, _, err := loadTermAndVote(dir); err == nil {
		t.Fatal("corrupt state file loaded without error")
	}
}

func TestTruncatedStateFileIsFatal(t *testing.T) {
	dir := t.TempDir()
	if err := saveTermAndVote(dir, 3, 1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, stateFileName)
	if err := os.Truncate(path, int64(stateFileSize-5)); err != nil {
		t.Fatal(err)
	}

	if _, _, err := loadTermAndVote(dir); err == nil {
		t.Fatal("truncated state file loaded without error")
	}
}
