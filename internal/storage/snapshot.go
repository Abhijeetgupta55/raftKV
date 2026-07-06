package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/Abhijeetgupta55/raftkv/internal/record"
)

// A snapshot is a full dump of the state machine, named %020d.snap after
// the last sequence number it includes:
//
//	magic "RKVSNAP1" | uint64 lastSeq | uint64 count
//	count × ( uint32 keyLen | key | uint32 valLen | value )
//	uint32 crc32c over everything above
//
// Atomicity comes from the write path, not the format: the file is
// written to a .tmp name, fsynced, and only then renamed into place.
// A crash mid-snapshot therefore leaves at worst a stray .tmp (ignored
// and cleaned on recovery) — every *.snap file is complete by
// construction, so a *.snap that fails its checksum is real disk
// corruption and startup refuses to proceed rather than fall back to an
// older snapshot, which would silently resurrect deleted data.

const (
	snapExt   = ".snap"
	snapTmp   = ".tmp"
	snapMagic = "RKVSNAP1"
)

func snapshotPath(dir string, lastSeq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", lastSeq, snapExt))
}

// writeSnapshot durably writes the full contents of mem as the snapshot
// covering everything up to and including lastSeq.
func writeSnapshot(dir string, lastSeq uint64, mem *MemStore) (err error) {
	final := snapshotPath(dir, lastSeq)
	tmp := final + snapTmp

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()

	crc := record.NewHash()
	bw := bufio.NewWriter(f)
	w := io.MultiWriter(bw, crc) // every byte written also feeds the checksum

	var scratch [8]byte
	writeU32 := func(v uint32) error {
		binary.LittleEndian.PutUint32(scratch[:4], v)
		_, err := w.Write(scratch[:4])
		return err
	}
	writeU64 := func(v uint64) error {
		binary.LittleEndian.PutUint64(scratch[:8], v)
		_, err := w.Write(scratch[:8])
		return err
	}

	if _, err = io.WriteString(w, snapMagic); err != nil {
		return err
	}
	if err = writeU64(lastSeq); err != nil {
		return err
	}
	if err = writeU64(uint64(mem.Len())); err != nil {
		return err
	}
	mem.Range(func(key string, value []byte) bool {
		if err = writeU32(uint32(len(key))); err != nil {
			return false
		}
		if _, err = io.WriteString(w, key); err != nil {
			return false
		}
		if err = writeU32(uint32(len(value))); err != nil {
			return false
		}
		_, err = w.Write(value)
		return err == nil
	})
	if err != nil {
		return err
	}

	// The trailing CRC goes only to the file, not into its own checksum.
	binary.LittleEndian.PutUint32(scratch[:4], crc.Sum32())
	if _, err = bw.Write(scratch[:4]); err != nil {
		return err
	}
	if err = bw.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, final); err != nil {
		return err
	}
	return record.SyncDir(dir)
}

// loadNewestSnapshot restores the most recent snapshot in dir, returning
// a fresh empty store (and lastSeq 0) if none exists. Stray .tmp files
// from a crash mid-snapshot are deleted.
func loadNewestSnapshot(dir string) (*MemStore, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}

	var seqs []uint64
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, snapTmp) {
			os.Remove(filepath.Join(dir, name))
			continue
		}
		if e.IsDir() || !strings.HasSuffix(name, snapExt) {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(name, snapExt), 10, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("snapshot: unrecognized file %q in %s", name, dir)
		}
		seqs = append(seqs, seq)
	}
	if len(seqs) == 0 {
		return NewMemStore(), 0, nil
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	newest := seqs[len(seqs)-1]

	mem, lastSeq, err := readSnapshot(snapshotPath(dir, newest))
	if err != nil {
		return nil, 0, err
	}
	if lastSeq != newest {
		return nil, 0, fmt.Errorf("snapshot %s claims lastSeq %d", snapshotPath(dir, newest), lastSeq)
	}
	return mem, lastSeq, nil
}

func readSnapshot(path string) (*MemStore, uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	if len(data) < len(snapMagic)+8+8+4 {
		return nil, 0, fmt.Errorf("snapshot %s: too short (%d bytes)", path, len(data))
	}

	body, tail := data[:len(data)-4], data[len(data)-4:]
	if record.Checksum(body) != binary.LittleEndian.Uint32(tail) {
		return nil, 0, fmt.Errorf("snapshot %s: checksum mismatch — disk corruption", path)
	}
	if string(body[:len(snapMagic)]) != snapMagic {
		return nil, 0, fmt.Errorf("snapshot %s: bad magic", path)
	}
	body = body[len(snapMagic):]

	lastSeq := binary.LittleEndian.Uint64(body)
	count := binary.LittleEndian.Uint64(body[8:])
	body = body[16:]

	mem := NewMemStore()
	for i := uint64(0); i < count; i++ {
		if len(body) < 4 {
			return nil, 0, fmt.Errorf("snapshot %s: truncated at entry %d", path, i)
		}
		keyLen := binary.LittleEndian.Uint32(body)
		body = body[4:]
		if uint64(len(body)) < uint64(keyLen)+4 {
			return nil, 0, fmt.Errorf("snapshot %s: truncated key at entry %d", path, i)
		}
		key := string(body[:keyLen])
		body = body[keyLen:]
		valLen := binary.LittleEndian.Uint32(body)
		body = body[4:]
		if uint64(len(body)) < uint64(valLen) {
			return nil, 0, fmt.Errorf("snapshot %s: truncated value at entry %d", path, i)
		}
		mem.Put(key, body[:valLen])
		body = body[valLen:]
	}
	if len(body) != 0 {
		return nil, 0, fmt.Errorf("snapshot %s: %d trailing bytes after %d entries", path, len(body), count)
	}
	return mem, lastSeq, nil
}

// pruneSnapshots deletes all but the newest keep snapshots. The WAL
// covering older snapshots is already gone, so they are unusable history
// kept only as a hedge; keep=2 retains the previous one briefly.
func pruneSnapshots(dir string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), snapExt) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // %020d names sort chronologically
	for i := 0; i < len(names)-keep; i++ {
		if err := os.Remove(filepath.Join(dir, names[i])); err != nil {
			return err
		}
	}
	return nil
}
