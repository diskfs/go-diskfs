package fat12

import (
	"encoding/binary"
	"errors"
	"os"
	"path"
	"slices"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

// testFormats holds a format of each width. 16 and 32 bits are described as
// fat16 and fat32 describe them.
var testFormats = []EntryFormat{
	format12,
	{
		Bits:      16,
		FATID:     func(head []byte) uint32 { return uint32(binary.LittleEndian.Uint16(head)) },
		EOCMarker: func([]byte) uint32 { return 0xFFFF },
		IsEOC:     func(val uint32) bool { return val >= 0xFFF8 && val <= 0xFFFF },
	},
	{
		Bits:      32,
		FATID:     func(head []byte) uint32 { return binary.LittleEndian.Uint32(head) },
		EOCMarker: func(head []byte) uint32 { return binary.LittleEndian.Uint32(head[4:]) },
		IsEOC:     func(val uint32) bool { return val&0xFFFFFF8 == 0xFFFFFF8 },
	},
}

// testEntries returns eight entries for a FAT of the format: FAT ID and EOC
// in the reserved entries, then the given cluster entries, then zeros.
func testEntries(format EntryFormat, clusters ...uint32) []uint32 {
	eoc := uint32(1)<<format.Bits - 1
	entries := make([]uint32, 8)
	entries[0], entries[1] = eoc-7, eoc
	copy(entries[2:], clusters)
	return entries
}

// encodeFAT encodes entries, an even number of them, as the format stores them.
func encodeFAT(format EntryFormat, entries []uint32) []byte {
	_, length := format.span(0, uint32(len(entries)-1))
	b := make([]byte, length)
	for n, val := range entries {
		format.put(b, 0, uint32(n), val)
	}
	return b
}

// decodeFAT decodes count entries from b.
func decodeFAT(format EntryFormat, b []byte, count int) []uint32 {
	entries := make([]uint32, count)
	for n := range entries {
		entries[n] = format.get(b, 0, uint32(n))
	}
	return entries
}

// lazyTableOver writes entries twice, as the primary and the secondary FAT,
// into a file and returns a lazy table over the primary, with the file's name.
func lazyTableOver(t *testing.T, format EntryFormat, entries []uint32, readOnly bool) (table *lazyTable, name string) {
	t.Helper()
	fat := encodeFAT(format, entries)
	name = path.Join(t.TempDir(), "fat.img")
	if err := os.WriteFile(name, append(slices.Clone(fat), fat...), 0o600); err != nil {
		t.Fatal(err)
	}
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	f, err := os.OpenFile(name, flag, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	tbl, err := NewLazyTable(file.New(f, readOnly), 0, uint32(len(fat)), format)
	if err != nil {
		t.Fatal(err)
	}
	table, ok := tbl.(*lazyTable)
	if !ok {
		t.Fatalf("NewLazyTable returned a %T", tbl)
	}
	return table, name
}

func TestEntryFormatEntries(t *testing.T) {
	for _, tt := range []struct {
		bits int
		want uint32
	}{{12, 341}, {16, 256}, {32, 128}} {
		if got := (EntryFormat{Bits: tt.bits}).Entries(512); got != tt.want {
			t.Errorf("%d bits: Entries(512) = %d, want %d", tt.bits, got, tt.want)
		}
	}
}

func TestEntryFormatGetPut(t *testing.T) {
	for _, format := range testFormats {
		mask := uint32(1)<<format.Bits - 1
		entries := make([]uint32, 8)
		for n := range entries {
			entries[n] = (0xABCDE123 * uint32(n+1)) & mask
		}
		b := encodeFAT(format, entries)
		if got := decodeFAT(format, b, len(entries)); !slices.Equal(got, entries) {
			t.Errorf("%d bits: decoded %#x, want %#x", format.Bits, got, entries)
		}
		format.put(b, 0, 5, 0)
		entries[5] = 0
		if got := decodeFAT(format, b, len(entries)); !slices.Equal(got, entries) {
			t.Errorf("%d bits: after putting 0 in entry 5, decoded %#x, want %#x", format.Bits, got, entries)
		}
	}
}

func TestEntryFormatEncodeRuns(t *testing.T) {
	value := func(n uint32) (uint32, error) { return n + 0x100, nil }
	tests := []struct {
		name    string
		format  EntryFormat
		size    uint32
		changed []uint32
		want    []FATRun
	}{
		{
			name: "32 bits, consecutive entries share a run", format: testFormats[2], size: 512,
			changed: []uint32{3, 4, 5, 9},
			want: []FATRun{
				{Offset: 12, Bytes: encodeFAT(testFormats[2], []uint32{0x103, 0x104, 0x105})},
				{Offset: 36, Bytes: binary.LittleEndian.AppendUint32(nil, 0x109)},
			},
		},
		{
			name: "12 bits, a run covers whole groups", format: format12, size: 512,
			changed: []uint32{3, 4},
			want:    []FATRun{{Offset: 3, Bytes: encodeFAT(format12, []uint32{0x102, 0x103, 0x104, 0x105})}},
		},
		{
			name: "12 bits, a run stops where the table ends", format: format12, size: 512,
			changed: []uint32{340},
			want:    []FATRun{{Offset: 510, Bytes: encodeFAT(format12, []uint32{0x254, 0x255})[:2]}},
		},
	}
	for _, tt := range tests {
		runs, err := tt.format.EncodeRuns(tt.size, tt.changed, value)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if !slices.EqualFunc(runs, tt.want, func(a, b FATRun) bool { return a.Offset == b.Offset && slices.Equal(a.Bytes, b.Bytes) }) {
			t.Errorf("%s: EncodeRuns = %v, want %v", tt.name, runs, tt.want)
		}
	}

	failing := func(uint32) (uint32, error) { return 0, errors.New("unreadable") }
	if _, err := format12.EncodeRuns(512, []uint32{3}, failing); err == nil {
		t.Error("EncodeRuns hid an entry that could not be read")
	}
}

func TestNewLazyTable(t *testing.T) {
	for _, format := range testFormats {
		entries := testEntries(format)
		table, _ := lazyTableOver(t, format, entries, true)
		if table.FATID() != entries[0] || table.EOCMarker() != format.EOCMarker(encodeFAT(format, entries)) ||
			table.MaxCluster() != 8 || table.writable {
			t.Errorf("%d bits: FAT ID %#x, EOC %#x, max cluster %d, writable %v",
				format.Bits, table.FATID(), table.EOCMarker(), table.MaxCluster(), table.writable)
		}
		if writable, _ := lazyTableOver(t, format, entries, false); !writable.writable {
			t.Errorf("%d bits: a table over a writable backend is not writable", format.Bits)
		}
	}
}

func TestLazyTableClusterValue(t *testing.T) {
	for _, format := range testFormats {
		table, _ := lazyTableOver(t, format, testEntries(format, 3, 0xFF8, 0, 9, 0, 0), true)
		for n, want := range map[uint32]uint32{0: 0, 1: 0, 2: 3, 3: 0xFF8, 4: 0, 5: 9, 8: 0, 9: 0} {
			if got := table.ClusterValue(n); got != want {
				t.Errorf("%d bits: ClusterValue(%d) = %#x, want %#x", format.Bits, n, got, want)
			}
		}
		table.pending[4] = 7
		if got := table.ClusterValue(4); got != 7 {
			t.Errorf("%d bits: ClusterValue(4) with 7 pending = %d", format.Bits, got)
		}
	}
}

func TestLazyTableSetCluster(t *testing.T) {
	for _, format := range testFormats {
		table, _ := lazyTableOver(t, format, testEntries(format), false)
		for _, n := range []uint32{0, 1, 2, 7, 8, 100} {
			table.SetCluster(n, n+10)
		}
		if want := map[uint32]uint32{2: 12, 7: 17}; len(table.pending) != len(want) || table.pending[2] != 12 || table.pending[7] != 17 {
			t.Errorf("%d bits: pending = %v, want %v", format.Bits, table.pending, want)
		}

		readOnly, _ := lazyTableOver(t, format, testEntries(format), true)
		readOnly.SetCluster(3, 5)
		if len(readOnly.pending) != 0 || readOnly.ClusterValue(3) != 0 {
			t.Errorf("%d bits: a table over a read-only backend held %v", format.Bits, readOnly.pending)
		}
	}
}

func TestLazyTableChanges(t *testing.T) {
	for _, format := range testFormats {
		entries := testEntries(format, 0xA, 0, 0, 0xB, 0, 0)
		table, name := lazyTableOver(t, format, entries, false)
		table.SetCluster(3, 0xC)
		table.SetCluster(4, 0xD)

		runs, err := table.Changes()
		if err != nil {
			t.Fatal(err)
		}
		fat, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		fat = fat[:table.size]
		if got := decodeFAT(format, fat, 8); !slices.Equal(got, entries) {
			t.Fatalf("%d bits: entries reached the backend before they were written: %#x", format.Bits, got)
		}
		for _, run := range runs {
			copy(fat[run.Offset:], run.Bytes)
		}
		want := testEntries(format, 0xA, 0xC, 0xD, 0xB, 0, 0)
		if got := decodeFAT(format, fat, 8); !slices.Equal(got, want) {
			t.Errorf("%d bits: FAT with the runs applied holds %#x, want %#x", format.Bits, got, want)
		}

		table.ChangesWritten()
		if runs, _ := table.Changes(); len(table.pending) != 0 || len(runs) != 0 {
			t.Errorf("%d bits: %d entries still pending after ChangesWritten", format.Bits, len(table.pending))
		}
	}
}

func TestLazyTableFreeClusters(t *testing.T) {
	for _, format := range testFormats {
		count := lazyChunkEntries + 10
		entries := make([]uint32, count)
		for n := range entries {
			entries[n] = 0xFF8
		}
		entries[5] = 0
		entries[lazyChunkEntries+3] = 0
		entries[lazyChunkEntries+7] = 0
		table, _ := lazyTableOver(t, format, entries, true)

		free, err := table.FreeClusters(10)
		if err != nil {
			t.Fatal(err)
		}
		if want := []uint32{5, lazyChunkEntries + 3, lazyChunkEntries + 7}; !slices.Equal(free, want) {
			t.Errorf("%d bits: FreeClusters(10) = %v, want %v", format.Bits, free, want)
		}
		if free, _ = table.FreeClusters(2); !slices.Equal(free, []uint32{5, lazyChunkEntries + 3}) {
			t.Errorf("%d bits: FreeClusters(2) = %v", format.Bits, free)
		}

		table.pending[5] = 0xFF8
		table.pending[9] = 0
		if free, _ = table.FreeClusters(1); !slices.Equal(free, []uint32{9}) {
			t.Errorf("%d bits: FreeClusters(1) with 5 taken and 9 freed pending = %v", format.Bits, free)
		}
	}
}

func TestFreeEntries(t *testing.T) {
	for _, format := range testFormats {
		b := encodeFAT(format, []uint32{0, 0xFF8, 0, 12})
		tests := []struct {
			name    string
			pending map[uint32]uint32
			want    []uint32
		}{
			{"as stored", nil, []uint32{10, 12}},
			{"pending applied", map[uint32]uint32{10: 0xFF8, 11: 0, 20: 0}, []uint32{11, 12}},
		}
		for _, tt := range tests {
			if got := freeEntries(format, b, 10, 10, 13, tt.pending); !slices.Equal(got, tt.want) {
				t.Errorf("%d bits, %s: freeEntries = %v, want %v", format.Bits, tt.name, got, tt.want)
			}
		}
	}
}

func TestLazyTableBytes(t *testing.T) {
	for _, format := range testFormats {
		table, _ := lazyTableOver(t, format, testEntries(format, 0xFF8), true)
		table.pending[3] = 0xFF8
		if got, want := table.Bytes(), encodeFAT(format, testEntries(format, 0xFF8, 0xFF8)); !slices.Equal(got, want) {
			t.Errorf("%d bits: Bytes = %x, want %x", format.Bits, got, want)
		}
	}
}

func TestFirstDifference(t *testing.T) {
	for _, tt := range []struct {
		a, b []byte
		want int
	}{
		{[]byte{1, 2, 3}, []byte{1, 2, 3}, -1},
		{[]byte{1, 2, 3}, []byte{1, 9, 3}, 1},
		{[]byte{}, []byte{}, -1},
	} {
		if got := firstDifference(tt.a, tt.b); got != tt.want {
			t.Errorf("firstDifference(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}
