package qcow2

import (
	"encoding/binary"
)

type refcountBlock struct {
	entries []refcountBlockEntry
	bits    uint32
}

type refcountBlockEntry uint64

// toBytes convert a refcount block to bytes
func (r refcountBlock) toBytes() []byte {
	var b []byte
	x := r.bits - 1
	for _, e := range r.entries {
		b = append(b, e.toBytes(x)...)
	}
	return b
}

// toBytes convert a refcount block entry to bytes
func (e refcountBlockEntry) toBytes(x uint32) []byte {
	// shift to the left the correct number of bits
	offset := uint64(e) << (63 - x)
	b := make([]byte, 8)
	// straight conversion to bytes
	binary.BigEndian.PutUint64(b, offset)

	return b
}

// parseRefcountBlock read the refcount block from the byte data
func parseRefcountBlock(b []byte, bits uint32) (*refcountBlock, error) {
	block := refcountBlock{
		bits: bits,
	}
	// v2 is locked at order = 4 / bits = 16, i.e. 2 bytes
	// for v3, order may not exceed order = 6 / bits = 64, i.e. 8 bytes
	// since order is a uint32, the only options we have are 4,5,6, or 2/4/8 bytes
	entrySize := (1 << bits) / 8
	for i := 0; i < len(b); i += entrySize {
		entry, err := parseRefcountBlockEntry(b[i : i+entrySize])
		if err != nil {
			return nil, err
		}
		block.entries = append(block.entries, entry)
	}
	return &block, nil
}

func parseRefcountBlockEntry(b []byte) (refcountBlockEntry, error) {
	return refcountBlockEntry(binary.BigEndian.Uint64(b)), nil
}
