package storage

import (
	"encoding/binary"
	"fmt"
)

// Op identifies a mutation type in encoded commands.
type Op uint8

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

// Command is one mutation of the key-value state. Commands are what the
// WAL persists today and what Raft will replicate as opaque bytes in a
// later milestone, so the encoding is self-contained: no protobuf, no
// references to anything above the storage layer.
//
// Wire form (little-endian):
//
//	uint8 op | uint32 keyLen | key | uint32 valLen | value
type Command struct {
	Op    Op
	Key   string
	Value []byte // ignored for OpDelete
}

// encodeCommand appends the wire form of c to buf and returns the
// extended slice, so callers can reuse one buffer across appends.
func encodeCommand(buf []byte, c Command) []byte {
	buf = append(buf, byte(c.Op))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(c.Key)))
	buf = append(buf, c.Key...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(c.Value)))
	buf = append(buf, c.Value...)
	return buf
}

// decodeCommand parses the wire form. The returned Command's Value
// aliases b; callers that retain it must copy (MemStore.Put does).
//
// Input comes from disk, so every length is treated as untrusted: all
// bounds checks use uint64 arithmetic to survive adversarial or corrupt
// 32-bit lengths without overflow.
func decodeCommand(b []byte) (Command, error) {
	if len(b) < 1+4 {
		return Command{}, fmt.Errorf("command truncated: %d bytes", len(b))
	}
	op := Op(b[0])
	if op != OpPut && op != OpDelete {
		return Command{}, fmt.Errorf("unknown command op %d", op)
	}
	b = b[1:]

	keyLen := binary.LittleEndian.Uint32(b)
	b = b[4:]
	if uint64(len(b)) < uint64(keyLen)+4 {
		return Command{}, fmt.Errorf("command truncated inside key (keyLen=%d, %d bytes left)", keyLen, len(b))
	}
	key := string(b[:keyLen])
	b = b[keyLen:]

	valLen := binary.LittleEndian.Uint32(b)
	b = b[4:]
	if uint64(len(b)) != uint64(valLen) {
		return Command{}, fmt.Errorf("command value length %d does not match %d remaining bytes", valLen, len(b))
	}
	return Command{Op: op, Key: key, Value: b}, nil
}
