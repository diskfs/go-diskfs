package fat32

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/diskfs/go-diskfs/util"
	"github.com/google/go-cmp/cmp"
)

const (
	eoc    = uint32(0x0fffffff) // {0xff, 0xff, 0xff, 0x0f})
	eocMin = uint32(0x0ffffff8) // {0xf8, 0xff, 0xff, 0x0f})
)

func getValidFat32Table() *table {
	// make a duplicate, in case someone modifies what we return
	t := &table{}
	*t = *fsInfo.table
	// and because the clusters are copied by reference
	t.clusters = slices.Clone(t.clusters)

	return t
}

func TestFat32TableFromBytes(t *testing.T) {
	t.Run("valid FAT32 Table", func(t *testing.T) {
		input, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		b := input[fsInfo.firstFAT : fsInfo.firstFAT+fsInfo.sectorsPerFAT*fsInfo.bytesPerSector]
		result := tableFromBytes(b)
		if result == nil {
			t.Fatalf("returned FAT32 Table was nil unexpectedly")
		}
		valid := getValidFat32Table()
		if !result.equal(valid) {
			diff := cmp.Diff(result, valid, cmp.AllowUnexported(table{}))
			t.Log(diff)
			t.Fatalf("Mismatched FAT32 Table")
		}
	})
}

func TestFat32TableToBytes(t *testing.T) {
	t.Run("valid FAT32 table", func(t *testing.T) {
		table := getValidFat32Table()
		b := table.bytes()
		if b == nil {
			t.Fatal("b was nil unexpectedly")
		}
		valid, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		validBytes := valid[fsInfo.firstFAT : fsInfo.firstFAT+fsInfo.sectorsPerFAT*fsInfo.bytesPerSector]
		if !bytes.Equal(validBytes, b) {
			_, diffString := util.DumpByteSlicesWithDiffs(validBytes, b, 32, false, true, true)
			t.Errorf("directory.toBytes() mismatched, actual then expected\n%s", diffString)
		}
	})
}

func TestFat32TableIsEoc(t *testing.T) {
	tests := []struct {
		cluster uint32
		eoc     bool
	}{
		{0xa7, false},
		{0x00, false},
		{0xFFFFFF7, false},
		{0xFFFFFF8, true},
		{0xFFFFFF9, true},
		{0xFFFFFFA, true},
		{0xFFFFFFB, true},
		{0xFFFFFFC, true},
		{0xFFFFFFD, true},
		{0xFFFFFFE, true},
		{0xFFFFFFF, true},
		{0xAFFFFFFF, true},
		{0x2FFFFFF8, true},
	}
	tab := table{}
	for _, tt := range tests {
		eoc := tab.isEoc(tt.cluster)
		if eoc != tt.eoc {
			t.Errorf("isEoc(%x): actual %t instead of expected %t", tt.cluster, eoc, tt.eoc)
		}
	}
}
