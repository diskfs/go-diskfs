package fat32

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/diskfs/go-diskfs/filesystem/fat12"
)

const (
	testFATID = 0x0FFFFFF8
	testEOC   = 0x0FFFFFFF
)

// encodeFAT encodes entries as a FAT32 table stores them.
func encodeFAT(entries []uint32) []byte {
	b := make([]byte, 0, len(entries)*4)
	for _, val := range entries {
		b = binary.LittleEndian.AppendUint32(b, val)
	}
	return b
}

// equalRuns reports whether a and b hold the same runs.
func equalRuns(a, b []fat12.FATRun) bool {
	return slices.EqualFunc(a, b, func(x, y fat12.FATRun) bool {
		return x.Offset == y.Offset && slices.Equal(x.Bytes, y.Bytes)
	})
}

func TestFormat32(t *testing.T) {
	head := encodeFAT([]uint32{testFATID, testEOC})
	if got := format32.FATID(head); got != testFATID {
		t.Errorf("FATID = %#x, want %#x", got, testFATID)
	}
	if got := format32.EOCMarker(head); got != testEOC {
		t.Errorf("EOCMarker = %#x, want %#x", got, testEOC)
	}
	for val, want := range map[uint32]bool{0x0FFFFFF8: true, testEOC: true, 0xFFFFFFFF: true, 0x0FFFFFF7: false, 5: false} {
		if got := format32.IsEOC(val); got != want {
			t.Errorf("IsEOC(%#x) = %v, want %v", val, got, want)
		}
	}
}

func TestTableChanges(t *testing.T) {
	tbl := tableFromBytes(encodeFAT([]uint32{testFATID, testEOC, testEOC, 0, 0, 0, 0, 0}))
	if runs, _ := tbl.Changes(); len(runs) != 0 {
		t.Fatalf("a table read from disk has changes: %v", runs)
	}
	tbl.SetCluster(3, 4)
	tbl.SetCluster(4, testEOC)
	tbl.SetCluster(6, testEOC)
	tbl.SetCluster(0, 1)

	runs, err := tbl.Changes()
	if err != nil {
		t.Fatal(err)
	}
	want := []fat12.FATRun{
		{Offset: 12, Bytes: encodeFAT([]uint32{4, testEOC})},
		{Offset: 24, Bytes: encodeFAT([]uint32{testEOC})},
	}
	if !equalRuns(runs, want) {
		t.Errorf("Changes = %v, want %v", runs, want)
	}

	tbl.ChangesWritten()
	if runs, _ := tbl.Changes(); len(runs) != 0 {
		t.Errorf("changes left after ChangesWritten: %v", runs)
	}
}

func TestTableChangesOfANewTable(t *testing.T) {
	tbl := &table{
		fatID:          testFATID,
		eocMarker:      testEOC,
		size:           32,
		maxCluster:     8,
		clusters:       make([]uint32, 9),
		rootDirCluster: 2,
	}
	tbl.SetCluster(2, testEOC)

	runs, err := tbl.Changes()
	if err != nil {
		t.Fatal(err)
	}
	if want := []fat12.FATRun{{Bytes: tbl.Bytes()}}; !equalRuns(runs, want) {
		t.Errorf("a table never written changes %v, want all of it: %v", runs, want)
	}

	tbl.ChangesWritten()
	tbl.SetCluster(3, testEOC)
	runs, _ = tbl.Changes()
	if want := []fat12.FATRun{{Offset: 12, Bytes: encodeFAT([]uint32{testEOC})}}; !equalRuns(runs, want) {
		t.Errorf("once written, Changes = %v, want %v", runs, want)
	}
}
