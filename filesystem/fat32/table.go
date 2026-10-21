package fat32

import (
	"encoding/binary"
	"maps"
	"slices"

	"github.com/diskfs/go-diskfs/filesystem/fat12"
)

// table is fat32's in-memory FAT table. It implements fat12.FATTable so that
// fat12.FileSystem (embedded in fat32.FileSystem) can use it for all cluster-
// chain operations.
type table struct {
	fatID          uint32
	eocMarker      uint32
	unusedMarker   uint32
	clusters       []uint32
	rootDirCluster uint32
	size           uint32
	maxCluster     uint32
	// changed holds the entries set since the table was last written. It is
	// nil for a table that has never been written, as Create builds, so that
	// the first write writes all of it.
	changed map[uint32]struct{}
}

// Verify the interfaces are satisfied at compile time.
var (
	_ fat12.FATTable      = (*table)(nil)
	_ fat12.ChangeTracker = (*table)(nil)
)

// ── fat12.FATTable interface ──────────────────────────────────────────────────

func (t *table) ClusterValue(n uint32) uint32 { return t.clusters[n] }
func (t *table) IsEOC(val uint32) bool        { return isEOC(val) }
func (t *table) EOCMarker() uint32            { return t.eocMarker }
func (t *table) UnusedMarker() uint32         { return t.unusedMarker }
func (t *table) MaxCluster() uint32           { return t.maxCluster }
func (t *table) FATID() uint32                { return t.fatID }
func (t *table) RootDirCluster() uint32       { return t.rootDirCluster }
func (t *table) Size() uint32                 { return t.size }

// SetCluster sets entry n and records it as changed.
func (t *table) SetCluster(n, val uint32) {
	t.clusters[n] = val
	if t.changed != nil && n >= 2 && n < t.maxCluster {
		t.changed[n] = struct{}{}
	}
}

// Changes encodes the entries set since the table was last written, or all of
// it if it has never been written.
func (t *table) Changes() ([]fat12.FATRun, error) {
	if t.changed == nil {
		return []fat12.FATRun{{Bytes: t.Bytes()}}, nil
	}
	return format32.EncodeRuns(t.size, slices.Sorted(maps.Keys(t.changed)), t.stored)
}

// ChangesWritten forgets the entries Changes encoded, which are now on disk.
func (t *table) ChangesWritten() {
	t.changed = map[uint32]struct{}{}
}

// stored returns entry n as held in memory.
func (t *table) stored(n uint32) (uint32, error) {
	return t.clusters[n], nil
}

// FromBytes populates the table from raw FAT bytes read from disk.
func (t *table) FromBytes(b []byte) {
	for i := uint32(2); i < t.maxCluster; i++ {
		bStart := i * 4
		val := binary.LittleEndian.Uint32(b[bStart : bStart+4])
		if val != 0 {
			t.clusters[i] = val
		}
	}
}

// Bytes serialises the table to raw FAT bytes ready to write to disk.
func (t *table) Bytes() []byte {
	b := make([]byte, t.size)
	binary.LittleEndian.PutUint32(b[0:4], t.fatID)
	binary.LittleEndian.PutUint32(b[4:8], t.eocMarker)
	for i := uint32(2); i < t.maxCluster; i++ {
		bStart := i * 4
		binary.LittleEndian.PutUint32(b[bStart:bStart+4], t.clusters[i])
	}
	return b
}

// ── internal helpers (used by fat32 tests and Create/Read) ───────────────────

// isEoc is retained for the table_internal_test.go tests.
func (t *table) isEoc(cluster uint32) bool { return t.IsEOC(cluster) }

func (t *table) equal(a *table) bool {
	if (t == nil && a != nil) || (t != nil && a == nil) {
		return false
	}
	if t == nil && a == nil {
		return true
	}
	return t.fatID == a.fatID &&
		t.eocMarker == a.eocMarker &&
		t.rootDirCluster == a.rootDirCluster &&
		t.size == a.size &&
		t.maxCluster == a.maxCluster &&
		slices.Equal(a.clusters, t.clusters)
}

// tableFromBytes constructs a fat32 table from raw FAT bytes.
func tableFromBytes(b []byte) *table {
	maxCluster := uint32(len(b) / 4)
	t := &table{
		fatID:          binary.LittleEndian.Uint32(b[0:4]),
		eocMarker:      binary.LittleEndian.Uint32(b[4:8]),
		size:           uint32(len(b)),
		clusters:       make([]uint32, maxCluster+1),
		maxCluster:     maxCluster,
		rootDirCluster: 2,
		changed:        map[uint32]struct{}{},
	}
	t.FromBytes(b)
	return t
}

// bytes is retained so existing code that calls t.bytes() still compiles.
// New code should prefer t.Bytes().
func (t *table) bytes() []byte { return t.Bytes() }
