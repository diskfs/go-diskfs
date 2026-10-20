package fat32

import (
	"encoding/binary"
	"fmt"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/filesystem/fat12"
)

// lazyTable is a read-only FAT that reads each entry from the backend when it
// is asked for, instead of holding the whole table in memory. Nothing is
// cached, so every lookup sees the FAT as it is on the backend at that moment,
// including entries another writer changed after the filesystem was read.
//
// It answers exactly what the in-memory table would for the same bytes:
// entries 0, 1 and maxCluster read as 0, because table.FromBytes never fills
// them, and every other entry is returned as stored.
type lazyTable struct {
	backend    backend.Storage
	offset     int64 // absolute byte offset of the primary FAT on the backend
	size       uint32
	maxCluster uint32
	fatID      uint32
	eocMarker  uint32
}

// Verify the interface is satisfied at compile time.
var _ fat12.FATTable = (*lazyTable)(nil)

// newLazyTable reads the two reserved entries at the start of the FAT, which
// hold the FAT ID and the end-of-chain marker, and nothing else.
func newLazyTable(b backend.Storage, offset int64, size uint32) (*lazyTable, error) {
	head := make([]byte, 8)
	if _, err := b.ReadAt(head, offset); err != nil {
		return nil, fmt.Errorf("could not read the start of the FAT: %w", err)
	}
	return &lazyTable{
		backend:    b,
		offset:     offset,
		size:       size,
		maxCluster: size / 4,
		fatID:      binary.LittleEndian.Uint32(head[0:4]),
		eocMarker:  binary.LittleEndian.Uint32(head[4:8]),
	}, nil
}

// ClusterValue reads entry n from the backend. An entry that cannot be read
// answers a value past MaxCluster, so the chain being followed ends with an
// "invalid cluster chain" error instead of continuing from a made-up value.
func (t *lazyTable) ClusterValue(n uint32) uint32 {
	if n < 2 || n >= t.maxCluster {
		return 0
	}
	b := make([]byte, 4)
	if _, err := t.backend.ReadAt(b, t.offset+int64(n)*4); err != nil {
		return t.maxCluster + 1
	}
	return binary.LittleEndian.Uint32(b)
}

// SetCluster does nothing: the table is read-only. ReadWithOptions only builds
// a lazyTable over a backend that cannot be written, so every write path fails
// at the backend before anything reaches the disk.
func (t *lazyTable) SetCluster(_, _ uint32) {}

func (t *lazyTable) IsEOC(val uint32) bool  { return val&0xFFFFFF8 == 0xFFFFFF8 }
func (t *lazyTable) EOCMarker() uint32      { return t.eocMarker }
func (t *lazyTable) UnusedMarker() uint32   { return 0 }
func (t *lazyTable) MaxCluster() uint32     { return t.maxCluster }
func (t *lazyTable) FATID() uint32          { return t.fatID }
func (t *lazyTable) RootDirCluster() uint32 { return 2 }
func (t *lazyTable) Size() uint32           { return t.size }

// FromBytes does nothing: the entries are always read from the backend.
func (t *lazyTable) FromBytes(_ []byte) {}

// Bytes returns the primary FAT as it is on the backend.
func (t *lazyTable) Bytes() []byte {
	b := make([]byte, t.size)
	n, _ := t.backend.ReadAt(b, t.offset)
	return b[:n]
}
