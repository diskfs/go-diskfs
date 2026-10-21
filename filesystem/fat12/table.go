package fat12

import (
	"slices"
)

// FATTable is the interface that abstracts on-disk FAT entry encoding.
// fat12 provides a 12-bit implementation; fat16 and fat32 each provide
// their own implementation in their respective packages.
// All implementations use uint32 as the in-memory cluster value for uniformity.
type FATTable interface {
	// ClusterValue returns the value stored for cluster n.
	ClusterValue(n uint32) uint32
	// SetCluster sets the value for cluster n.
	SetCluster(n uint32, val uint32)
	// IsEOC reports whether val is an end-of-chain marker.
	IsEOC(val uint32) bool
	// EOCMarker returns the canonical end-of-chain value to write.
	EOCMarker() uint32
	// UnusedMarker returns the value that marks a free cluster.
	UnusedMarker() uint32
	// MaxCluster returns the highest valid cluster index.
	MaxCluster() uint32
	// FATID returns the media-descriptor word stored at FAT[0].
	FATID() uint32
	// RootDirCluster returns the cluster that holds the root directory.
	// For FAT12/16 this is always 2 (though the root dir is stored in
	// a fixed region, not via the cluster chain).
	RootDirCluster() uint32
	// Size returns the on-disk size of one FAT copy in bytes.
	Size() uint32
	// FromBytes populates the table from raw FAT bytes read from disk.
	FromBytes(b []byte)
	// Bytes serialises the table to raw FAT bytes ready to write to disk.
	Bytes() []byte
}

// ChangeTracker is implemented by a FATTable that knows which entries were set
// since it was last written. WriteFat then writes only those, instead of the
// whole table.
type ChangeTracker interface {
	// Changes encodes the entries set since the last ChangesWritten as runs of
	// bytes of one copy of the FAT.
	Changes() ([]FATRun, error)
	// ChangesWritten forgets the entries Changes encoded, once every copy of
	// the FAT holds them.
	ChangesWritten()
}

// FreeClusterFinder is implemented by a FATTable that can find free clusters
// without being asked for each entry in turn.
type FreeClusterFinder interface {
	// FreeClusters returns up to count free clusters, in ascending order from 2.
	FreeClusters(count int) ([]uint32, error)
}

// ── fat12Table ────────────────────────────────────────────────────────────────

// format12 is how FAT12 stores its entries.
var format12 = EntryFormat{
	Bits:      12,
	FATID:     func(head []byte) uint32 { return uint32(head[0]) | 0x0F00 },
	EOCMarker: func([]byte) uint32 { return 0x0FFF },
	IsEOC:     isEOC12,
}

// isEOC12 reports whether val ends a FAT12 cluster chain.
func isEOC12(val uint32) bool { return val >= 0xFF8 && val <= 0xFFF }

// fat12Table implements FATTable for 12-bit FAT entries.
type fat12Table struct {
	fatID    uint32
	eoc      uint32   // canonical EOC to write (0x0FF8–0x0FFF)
	unused   uint32   // 0x000
	clusters []uint32 // in-memory; index == cluster number
	max      uint32   // highest valid cluster index
	size     uint32   // on-disk size in bytes (one copy)
}

func newFat12Table(fatID, sizeBytes uint32) *fat12Table {
	// Each entry is 12 bits = 1.5 bytes; two entries share 3 bytes.
	// maxCluster ≈ sizeBytes * 2/3 but we compute it precisely below.
	maxCluster := sizeBytes * 2 / 3
	return &fat12Table{
		fatID:    fatID,
		eoc:      0x0FFF,
		unused:   0x000,
		clusters: make([]uint32, maxCluster+1),
		max:      maxCluster,
		size:     sizeBytes,
	}
}

func (t *fat12Table) ClusterValue(n uint32) uint32 { return t.clusters[n] }
func (t *fat12Table) SetCluster(n, val uint32)     { t.clusters[n] = val }
func (t *fat12Table) IsEOC(val uint32) bool        { return isEOC12(val) }
func (t *fat12Table) EOCMarker() uint32            { return t.eoc }
func (t *fat12Table) UnusedMarker() uint32         { return t.unused }
func (t *fat12Table) MaxCluster() uint32           { return t.max }
func (t *fat12Table) FATID() uint32                { return t.fatID }
func (t *fat12Table) RootDirCluster() uint32       { return 2 }
func (t *fat12Table) Size() uint32                 { return t.size }

// FromBytes reads a 12-bit FAT from raw bytes.
func (t *fat12Table) FromBytes(b []byte) {
	// FAT[0] and FAT[1] are reserved media-descriptor words; cluster entries start at index 2.
	// Entry i occupies bits [i*12 .. i*12+11] inside the byte stream.
	maxIdx := uint32(len(b)*2/3) - 1
	if maxIdx > t.max {
		maxIdx = t.max
	}
	for i := uint32(2); i <= maxIdx; i++ {
		t.clusters[i] = fat12ReadEntry(b, i)
	}
	// Capture the media byte from FAT[0] for the FATID.
	if len(b) >= 2 {
		t.fatID = uint32(b[0]) | 0x0F00 // FAT12 ID: 0x0Fxx
	}
}

// Bytes serialises a 12-bit FAT to raw bytes.
func (t *fat12Table) Bytes() []byte {
	b := make([]byte, t.size)
	// FAT[0]: media descriptor byte ORed with 0x0F00, then 0xFF for FAT[1].
	mediaByte := byte(t.fatID & 0xFF)
	b[0] = mediaByte
	b[1] = 0xFF
	b[2] = 0x0F // FAT[1] upper nibble; combined with b[1] gives 0xFFF for FAT[1]

	// Actually FAT12 entry layout for FAT[0] and FAT[1]:
	// FAT[0] = 0xF?? where ?? = media type; FAT[1] = 0xFFF (EOC)
	// They share 3 bytes: b[0]=mediaType, b[1]=0xFF, b[2]=0xFF
	b[0] = mediaByte
	b[1] = 0xFF
	b[2] = 0xFF

	for i := uint32(2); i <= t.max; i++ {
		if t.clusters[i] != 0 {
			fat12WriteEntry(b, i, t.clusters[i])
		}
	}
	return b
}

// fat12ReadEntry reads the 12-bit value for cluster i from raw FAT bytes.
func fat12ReadEntry(b []byte, i uint32) uint32 {
	byteOffset := i * 3 / 2
	if byteOffset+1 >= uint32(len(b)) {
		return 0
	}
	word := uint32(b[byteOffset]) | uint32(b[byteOffset+1])<<8
	if i%2 == 0 {
		return word & 0x0FFF
	}
	return word >> 4
}

// fat12WriteEntry writes the 12-bit value v for cluster i into raw FAT bytes.
func fat12WriteEntry(b []byte, i, v uint32) {
	byteOffset := i * 3 / 2
	if byteOffset+1 >= uint32(len(b)) {
		return
	}
	if i%2 == 0 {
		b[byteOffset] = byte(v)
		b[byteOffset+1] = (b[byteOffset+1] & 0xF0) | byte(v>>8)
	} else {
		b[byteOffset] = (b[byteOffset] & 0x0F) | byte(v<<4)
		b[byteOffset+1] = byte(v >> 4)
	}
}

// ── tableEqual ────────────────────────────────────────────────────────────────

// tablesEqual compares two FATTable values for equality (used in tests / Equal()).
// It only works for fat12Table.
func tablesEqual(a, b FATTable) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	at, ok1 := a.(*fat12Table)
	bt, ok2 := b.(*fat12Table)
	if !ok1 || !ok2 {
		return false
	}
	return at.fatID == bt.fatID &&
		at.eoc == bt.eoc &&
		at.max == bt.max &&
		at.size == bt.size &&
		slices.Equal(at.clusters, bt.clusters)
}
