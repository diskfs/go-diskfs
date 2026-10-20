package fat12

import (
	"fmt"
	"maps"
	"slices"

	"github.com/diskfs/go-diskfs/backend"
)

// lazyChunkEntries is how many FAT entries FreeClusters reads at a time.
const lazyChunkEntries = 16 * 1024

// lazyTable is a FAT that reads each entry from the backend when it is asked
// for, instead of holding the whole table in memory. Nothing read is cached, so
// every lookup sees the FAT as it is on the backend at that moment, including
// entries another writer changed after the filesystem was read. Over a
// writable backend, entries that are set are held until WriteFat writes them.
//
// It answers exactly what the in-memory tables would for the same bytes:
// entries 0, 1 and from maxCluster on read as 0, because their FromBytes never
// fills them, and every other entry is returned as stored.
type lazyTable struct {
	format     EntryFormat
	backend    backend.Storage
	offset     int64 // absolute byte offset of the primary FAT on the backend
	size       uint32
	maxCluster uint32
	fatID      uint32
	eocMarker  uint32
	writable   bool
	pending    map[uint32]uint32 // entries set and not yet written
}

// Verify the interfaces are satisfied at compile time.
var (
	_ FATTable          = (*lazyTable)(nil)
	_ ChangeTracker     = (*lazyTable)(nil)
	_ FreeClusterFinder = (*lazyTable)(nil)
)

// NewLazyTable returns a FAT in format, size bytes long, that is read from the
// backend at offset as it is needed. It reads the two reserved entries at the
// start of the table, which hold the FAT ID and the end-of-chain marker, and
// nothing else.
func NewLazyTable(b backend.Storage, offset int64, size uint32, format EntryFormat) (FATTable, error) {
	head := make([]byte, 8)
	if _, err := b.ReadAt(head, offset); err != nil {
		return nil, fmt.Errorf("could not read the start of the FAT: %w", err)
	}
	_, err := b.Writable()
	return &lazyTable{
		format:     format,
		backend:    b,
		offset:     offset,
		size:       size,
		maxCluster: format.Entries(size),
		fatID:      format.FATID(head),
		eocMarker:  format.EOCMarker(head),
		writable:   err == nil,
		pending:    map[uint32]uint32{},
	}, nil
}

// ClusterValue returns entry n. An entry that cannot be read answers a value
// past MaxCluster, so the chain being followed ends with an "invalid cluster
// chain" error instead of continuing from a made-up value.
func (t *lazyTable) ClusterValue(n uint32) uint32 {
	if n < 2 || n >= t.maxCluster {
		return 0
	}
	val, err := t.stored(n)
	if err != nil {
		return t.maxCluster + 1
	}
	return val
}

// stored returns entry n as set and not yet written, or else as read from the
// backend.
func (t *lazyTable) stored(n uint32) (uint32, error) {
	if val, ok := t.pending[n]; ok {
		return val, nil
	}
	b, first, err := t.read(n, n)
	if err != nil {
		return 0, err
	}
	return t.format.get(b, first, n), nil
}

// read returns the bytes holding entries first to last, widened to whole
// groups, and the first entry they hold.
func (t *lazyTable) read(first, last uint32) (b []byte, from uint32, err error) {
	from, to := t.format.widen(first, last)
	offset, length := t.format.span(from, to)
	b = make([]byte, length)
	if _, err := t.backend.ReadAt(b, t.offset+offset); err != nil {
		return nil, 0, fmt.Errorf("could not read FAT entries %d to %d: %w", first, last, err)
	}
	return b, from, nil
}

// SetCluster holds val for entry n until WriteFat writes it. Over a read-only
// backend, and for entries the in-memory tables never write out, it does
// nothing.
func (t *lazyTable) SetCluster(n, val uint32) {
	if !t.writable || n < 2 || n >= t.maxCluster {
		return
	}
	t.pending[n] = val
}

// Changes encodes the entries set and not yet written as runs of bytes.
func (t *lazyTable) Changes() ([]FATRun, error) {
	return t.format.EncodeRuns(t.size, slices.Sorted(maps.Keys(t.pending)), t.stored)
}

// ChangesWritten forgets the entries Changes encoded, which are now on the backend.
func (t *lazyTable) ChangesWritten() {
	clear(t.pending)
}

// FreeClusters reads the primary FAT a chunk at a time and returns up to count
// clusters that hold 0, in ascending order from 2: the clusters the in-memory
// tables would hand out.
func (t *lazyTable) FreeClusters(count int) ([]uint32, error) {
	free := make([]uint32, 0, count)
	for first := uint32(2); first < t.maxCluster && len(free) < count; first += lazyChunkEntries {
		last := min(first+lazyChunkEntries, t.maxCluster) - 1
		b, from, err := t.read(first, last)
		if err != nil {
			return nil, err
		}
		free = append(free, freeEntries(t.format, b, from, first, last, t.pending)...)
	}
	return free[:min(len(free), count)], nil
}

// freeEntries returns the entries first to last that hold 0 once pending is
// applied. b holds whole groups from entry from on.
func freeEntries(format EntryFormat, b []byte, from, first, last uint32, pending map[uint32]uint32) []uint32 {
	var free []uint32
	for n := first; n <= last; n++ {
		val, ok := pending[n]
		if !ok {
			val = format.get(b, from, n)
		}
		if val == 0 {
			free = append(free, n)
		}
	}
	return free
}

func (t *lazyTable) IsEOC(val uint32) bool  { return t.format.IsEOC(val) }
func (t *lazyTable) EOCMarker() uint32      { return t.eocMarker }
func (t *lazyTable) UnusedMarker() uint32   { return 0 }
func (t *lazyTable) MaxCluster() uint32     { return t.maxCluster }
func (t *lazyTable) FATID() uint32          { return t.fatID }
func (t *lazyTable) RootDirCluster() uint32 { return 2 }
func (t *lazyTable) Size() uint32           { return t.size }

// FromBytes does nothing: the entries are always read from the backend.
func (t *lazyTable) FromBytes(_ []byte) {}

// Bytes returns the primary FAT as it is on the backend, with the entries set
// and not yet written applied.
func (t *lazyTable) Bytes() []byte {
	b := make([]byte, t.size)
	n, _ := t.backend.ReadAt(b, t.offset)
	for cluster, val := range t.pending {
		t.format.put(b, 0, cluster, val)
	}
	return b[:n]
}
