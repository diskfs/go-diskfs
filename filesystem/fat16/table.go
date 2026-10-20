package fat16

import (
	"encoding/binary"
	"slices"

	"github.com/diskfs/go-diskfs/filesystem/fat12"
)

// format16 is how FAT16 stores its entries.
var format16 = fat12.EntryFormat{
	Bits:      16,
	FATID:     func(head []byte) uint32 { return uint32(binary.LittleEndian.Uint16(head[0:2])) },
	EOCMarker: func([]byte) uint32 { return 0xFFFF },
	IsEOC:     isEOC,
}

// isEOC reports whether val ends a FAT16 cluster chain.
func isEOC(val uint32) bool { return val >= 0xFFF8 && val <= 0xFFFF }

// fat16Table implements fat12.FATTable with 16-bit FAT entries.
type fat16Table struct {
	fatID    uint32
	eoc      uint32
	clusters []uint32
	max      uint32
	size     uint32
}

// newFat16Table constructs a 16-bit FAT table of the given on-disk size in bytes.
func newFat16Table(fatID, sizeBytes uint32) *fat16Table {
	maxCluster := sizeBytes / 2
	return &fat16Table{
		fatID:    fatID,
		eoc:      0xFFFF,
		clusters: make([]uint32, maxCluster+1),
		max:      maxCluster,
		size:     sizeBytes,
	}
}

// Compile-time check that fat16Table satisfies the fat12.FATTable interface.
var _ fat12.FATTable = (*fat16Table)(nil)

func (t *fat16Table) ClusterValue(n uint32) uint32 { return t.clusters[n] }
func (t *fat16Table) SetCluster(n, val uint32)     { t.clusters[n] = val }
func (t *fat16Table) IsEOC(val uint32) bool        { return isEOC(val) }
func (t *fat16Table) EOCMarker() uint32            { return t.eoc }
func (t *fat16Table) UnusedMarker() uint32         { return 0x0000 }
func (t *fat16Table) MaxCluster() uint32           { return t.max }
func (t *fat16Table) FATID() uint32                { return t.fatID }
func (t *fat16Table) RootDirCluster() uint32       { return 2 }
func (t *fat16Table) Size() uint32                 { return t.size }

func (t *fat16Table) FromBytes(b []byte) {
	for i := uint32(2); i < t.max && i*2+2 <= uint32(len(b)); i++ {
		val := uint32(binary.LittleEndian.Uint16(b[i*2 : i*2+2]))
		if val != 0 {
			t.clusters[i] = val
		}
	}
	if len(b) >= 2 {
		t.fatID = uint32(binary.LittleEndian.Uint16(b[0:2]))
	}
}

func (t *fat16Table) Bytes() []byte {
	b := make([]byte, t.size)
	binary.LittleEndian.PutUint16(b[0:2], uint16(t.fatID))
	binary.LittleEndian.PutUint16(b[2:4], uint16(t.eoc))
	for i := uint32(2); i < t.max; i++ {
		binary.LittleEndian.PutUint16(b[i*2:i*2+2], uint16(t.clusters[i]))
	}
	return b
}

func (t *fat16Table) equal(a *fat16Table) bool {
	return t.fatID == a.fatID &&
		t.eoc == a.eoc &&
		t.max == a.max &&
		t.size == a.size &&
		slices.Equal(t.clusters, a.clusters)
}
