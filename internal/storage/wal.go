package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// The write-ahead log is a directory of segment files, each a flat
// sequence of records:
//
//	uint32 payloadLen | uint32 crc32c(payload) | payload
//	payload: uint64 seq | command bytes
//
// Segments are named %020d.wal after the first sequence number they may
// contain; a new segment is started after each snapshot so fully-covered
// segments can be deleted as whole files.
//
// Torn-write policy — the part that makes recovery correct:
//   - The first invalid record (short header, short payload, or CRC
//     mismatch) in the NEWEST segment ends the log: everything from that
//     offset on is truncated. A record can only be half-written if the
//     process died mid-append, and an append is only acknowledged after
//     fsync, so whatever is being dropped was never acked.
//   - An invalid record in any OLDER segment is fatal corruption: those
//     segments were finished and fsynced, so an unreadable record there
//     means acknowledged data is damaged, and refusing to start beats
//     silently serving wrong data.
//   - Sequence numbers must increase by exactly 1 across records; a gap
//     is corruption even if every checksum passes.

const (
	walExt          = ".wal"
	walHeaderSize   = 8 // uint32 payloadLen + uint32 crc
	walSeqSize      = 8 // uint64 seq at the start of each payload
	walMaxRecordLen = walSeqSize + 1 + 4 + MaxKeyBytesOnDisk + 4 + MaxValueBytesOnDisk
)

// The storage layer enforces no policy of its own on key/value sizes (the
// service layer does), but replay needs a sanity bound to reject absurd
// lengths read from a corrupt header without allocating gigabytes. These
// are deliberately far above the service-layer limits.
const (
	MaxKeyBytesOnDisk   = 1 << 20 // 1 MiB
	MaxValueBytesOnDisk = 1 << 26 // 64 MiB
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// errWALFailed is returned by Append after any write or sync error: the
// file's on-disk state is unknown at that point, so the WAL refuses all
// further appends rather than risk writing records after a torn one.
var errWALFailed = errors.New("wal: failed by an earlier write error; storage is read-only")

type wal struct {
	dir    string
	f      *os.File
	size   int64 // bytes in the active segment
	buf    []byte
	failed bool
}

// segmentPath returns the path of the segment whose first sequence
// number is startSeq.
func segmentPath(dir string, startSeq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", startSeq, walExt))
}

// createWAL starts a fresh segment that will begin at startSeq.
func createWAL(dir string, startSeq uint64) (*wal, error) {
	f, err := os.OpenFile(segmentPath(dir, startSeq), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return &wal{dir: dir, f: f}, nil
}

// openWALForAppend opens an existing segment for appending, after
// recovery has already validated its contents up to validSize and
// truncated any torn tail.
func openWALForAppend(dir string, startSeq uint64, validSize int64) (*wal, error) {
	f, err := os.OpenFile(segmentPath(dir, startSeq), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{dir: dir, f: f, size: validSize}, nil
}

// Append frames, writes, and fsyncs one record. It does not return until
// the record is durably on disk — this is the fsync-before-ack rule that
// the durability guarantee rests on.
func (w *wal) Append(seq uint64, command []byte) error {
	if w.failed {
		return errWALFailed
	}

	payloadLen := walSeqSize + len(command)
	b := w.buf[:0]
	b = binary.LittleEndian.AppendUint32(b, uint32(payloadLen))
	b = append(b, 0, 0, 0, 0) // CRC placeholder, filled below
	b = binary.LittleEndian.AppendUint64(b, seq)
	b = append(b, command...)
	binary.LittleEndian.PutUint32(b[4:8], crc32.Checksum(b[walHeaderSize:], castagnoli))
	w.buf = b

	if _, err := w.f.Write(b); err != nil {
		w.failed = true
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.failed = true
		return err
	}
	w.size += int64(len(b))
	return nil
}

func (w *wal) Close() error {
	return w.f.Close()
}

// rotate closes the active segment and starts a new one beginning at
// startSeq. Called after a snapshot, so the old segment is disposable.
func (w *wal) rotate(startSeq uint64) error {
	if w.failed {
		return errWALFailed
	}
	next, err := createWAL(w.dir, startSeq)
	if err != nil {
		return err
	}
	old := w.f
	w.f, w.size = next.f, 0
	return old.Close()
}

type walSegment struct {
	startSeq uint64
	path     string
}

// listSegments returns the WAL segments in dir ordered by start sequence.
func listSegments(dir string) ([]walSegment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []walSegment
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, walExt) {
			continue
		}
		start, err := strconv.ParseUint(strings.TrimSuffix(name, walExt), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("wal: unrecognized file %q in %s", name, dir)
		}
		segs = append(segs, walSegment{startSeq: start, path: filepath.Join(dir, name)})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].startSeq < segs[j].startSeq })
	return segs, nil
}

// replayWAL feeds every intact record with seq > fromSeq to apply, in
// order, enforcing the torn-write policy documented at the top of this
// file. It returns the highest sequence number seen and, for the newest
// segment, the byte offset at which valid data ends (the caller truncates
// there before reopening for append).
func replayWAL(dir string, fromSeq uint64, apply func(seq uint64, command []byte) error) (lastSeq uint64, newestValidSize int64, err error) {
	segs, err := listSegments(dir)
	if err != nil {
		return 0, 0, err
	}

	lastSeq = fromSeq
	for i, seg := range segs {
		newest := i == len(segs)-1
		data, err := os.ReadFile(seg.path)
		if err != nil {
			return 0, 0, err
		}

		off := int64(0)
		for off < int64(len(data)) {
			rec, recLen, ok := parseRecord(data[off:])
			if !ok {
				if newest {
					// Torn tail: the process died mid-append. Nothing
					// from off onward was ever acknowledged.
					return lastSeq, off, nil
				}
				return 0, 0, fmt.Errorf("wal: corrupt record in finished segment %s at offset %d", seg.path, off)
			}
			if rec.seq > lastSeq {
				if rec.seq != lastSeq+1 {
					return 0, 0, fmt.Errorf("wal: sequence gap in %s: %d follows %d", seg.path, rec.seq, lastSeq)
				}
				if err := apply(rec.seq, rec.command); err != nil {
					return 0, 0, err
				}
				lastSeq = rec.seq
			}
			off += recLen
		}
		if newest {
			newestValidSize = off
		}
	}
	return lastSeq, newestValidSize, nil
}

type walRecord struct {
	seq     uint64
	command []byte
}

// parseRecord reads one record from the front of b. ok is false when the
// bytes do not form a complete, checksum-valid record — the caller
// decides whether that means "torn tail" or "corruption" based on which
// segment it is in.
func parseRecord(b []byte) (rec walRecord, recLen int64, ok bool) {
	if len(b) < walHeaderSize {
		return walRecord{}, 0, false
	}
	payloadLen := binary.LittleEndian.Uint32(b)
	crc := binary.LittleEndian.Uint32(b[4:])
	if payloadLen < walSeqSize || payloadLen > walMaxRecordLen {
		return walRecord{}, 0, false
	}
	end := walHeaderSize + int(payloadLen)
	if len(b) < end {
		return walRecord{}, 0, false
	}
	payload := b[walHeaderSize:end]
	if crc32.Checksum(payload, castagnoli) != crc {
		return walRecord{}, 0, false
	}
	return walRecord{
		seq:     binary.LittleEndian.Uint64(payload),
		command: payload[walSeqSize:],
	}, int64(end), true
}

// syncDir fsyncs a directory so that file creations and renames inside it
// are themselves durable. Windows does not support syncing directory
// handles; there the OS metadata journal is relied on instead, which is a
// documented known limitation rather than a silent one.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
