// Package record holds the on-disk primitives shared by every durable
// component (the KV write-ahead log, snapshots, the Raft log, the Raft
// vote file): CRC-framed records, checksums, and the fsync helpers that
// make file creation and replacement durable. Keeping one implementation
// means the KV store and the consensus layer can never disagree about
// what a valid record looks like.
package record

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
)

// castagnoli is the CRC32-C polynomial: hardware-accelerated on modern
// CPUs and the checksum used by ext4, iSCSI, and most storage systems.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns the CRC32-C of b.
func Checksum(b []byte) uint32 {
	return crc32.Checksum(b, castagnoli)
}

// NewHash returns a streaming CRC32-C, for writers that checksum data
// too large to hold in one buffer (snapshots).
func NewHash() hash.Hash32 {
	return crc32.New(castagnoli)
}

// HeaderSize is the framing overhead per record:
// uint32 payload length + uint32 CRC32-C of the payload.
const HeaderSize = 8

// Frame appends one framed record containing payload to buf and returns
// the extended slice, so callers can batch several records into one
// buffer and write them with a single syscall.
func Frame(buf, payload []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(payload)))
	buf = binary.LittleEndian.AppendUint32(buf, Checksum(payload))
	return append(buf, payload...)
}

// Parse reads one record from the front of b. ok is false when the bytes
// do not form a complete, checksum-valid record with a payload of at most
// maxPayload — the caller decides what an invalid record means (torn tail
// vs. corruption). The returned payload aliases b.
func Parse(b []byte, maxPayload uint32) (payload []byte, recLen int64, ok bool) {
	if len(b) < HeaderSize {
		return nil, 0, false
	}
	payloadLen := binary.LittleEndian.Uint32(b)
	crc := binary.LittleEndian.Uint32(b[4:])
	if payloadLen > maxPayload {
		return nil, 0, false
	}
	end := HeaderSize + int(payloadLen)
	if len(b) < end {
		return nil, 0, false
	}
	payload = b[HeaderSize:end]
	if Checksum(payload) != crc {
		return nil, 0, false
	}
	return payload, int64(end), true
}

// SyncDir fsyncs a directory so that file creations and renames inside
// it are themselves durable. Windows does not support syncing directory
// handles; there the OS metadata journal is relied on instead, which is
// a documented known limitation rather than a silent one.
func SyncDir(dir string) error {
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

// WriteFileAtomic durably replaces path with data: write to a .tmp
// sibling, fsync, rename into place, fsync the directory. A crash at any
// point leaves either the old file or the new one, never a mix.
func WriteFileAtomic(path string, data []byte) (err error) {
	tmp := path + ".tmp"
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

	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(path))
}
