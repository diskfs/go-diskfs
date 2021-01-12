package qcow2

import (
	"encoding/binary"
	"fmt"
)

const (
	refcountOffsetMask uint64 = 0xff800000
)

type refcountTable struct {
	entries []refcountTableEntry
	size    int
}

type refcountTableEntry uint64

// toBytes convert refcountTable to disk format bytes
func (t refcountTable) toBytes() []byte {
	var b []byte
	for _, e := range t.entries {
		b = append(b, e.toBytes()...)
	}
	// table should always be the right size
	if len(b) < t.size {
		b = append(b, make([]byte, t.size-len(b))...)
	}
	return b
}

// parseRefcountTable read the refcount table from the byte data
func parseRefcountTable(b []byte) (*refcountTable, error) {
	table := refcountTable{
		size: len(b),
	}
	for i := 0; i < len(b); i += 8 {
		entry, err := parseRefcountTableEntry(b[i : i+8])
		if err != nil {
			return nil, err
		}
		table.entries = append(table.entries, entry)
	}
	return &table, nil
}

// parseRefcountTableEntry read a single refcount table entry
func parseRefcountTableEntry(b []byte) (refcountTableEntry, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("cannot parse an entry with %d bytes, requires exactly %d", len(b), 8)
	}
	offset := binary.BigEndian.Uint64(b)
	// now mask the irrelevant data and shift it off to the correct position
	// it all should be zero, but we are being extra careful
	offset &= refcountOffsetMask

	return refcountTableEntry(offset), nil
}

// toBytes convert refcountTableEntry to disk format bytes
func (e refcountTableEntry) toBytes() []byte {
	b := make([]byte, 8)
	offset := uint64(e) ^ refcountOffsetMask
	binary.BigEndian.PutUint64(b, offset)
	return b
}
