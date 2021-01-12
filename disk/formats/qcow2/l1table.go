package qcow2

import (
	"encoding/binary"
	"fmt"
)

type l1Table struct {
	entries []l1TableEntry
	size    int
}

type l1TableEntry struct {
	offset    uint64
	active    bool
	allocated bool
}

// toBytes convert l1Table to disk format bytes
func (t l1Table) toBytes() []byte {
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

// toBytes convert l1TableEntry to disk format bytes
func (e l1TableEntry) toBytes() []byte {
	b := make([]byte, 8)
	offset := e.offset << 8
	binary.BigEndian.PutUint64(b, offset)
	if e.active {
		b[7] |= 0x1
	}
	return b
}

// parseL1Table read the l1table from the byte data
func parseL1Table(b []byte) (*l1Table, error) {
	table := l1Table{size: len(b)}
	for i := 0; i < len(b); i += 8 {
		entry, err := parseL1TableEntry(b[i : i+8])
		if err != nil {
			return nil, err
		}
		table.entries = append(table.entries, entry)
	}
	return &table, nil
}

func parseL1TableEntry(b []byte) (l1TableEntry, error) {
	if len(b) != 8 {
		return l1TableEntry{}, fmt.Errorf("cannot parse an entry with %d bytes, requires exactly %d", len(b), 8)
	}
	active := b[7]&0x1 == 0x1
	offset := binary.BigEndian.Uint64(b)
	// now mask the irrelevant data and shift it off to the correct position
	offset >>= 8
	offset &= 0xff000000

	return l1TableEntry{
		offset:    offset,
		active:    active,
		allocated: offset != 0,
	}, nil
}
