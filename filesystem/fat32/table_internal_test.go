package fat32

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/diskfs/go-diskfs/util"
	"github.com/google/go-cmp/cmp"
)

func getEoc(fatType int) (uint32, uint32) {
	switch fatType {
	case 12:
		return 0xfff, 0xff8
	case 16:
		return 0xffff, 0xfff8
	default:
		return uint32(0x0fffffff), uint32(0x0ffffff8)
	}
}

func getValidFatTable(fatType int) *table {
	// make a duplicate, in case someone modifies what we return
	t := &table{}
	*t = *GetFsInfo(fatType).table
	// and because the clusters are copied by reference
	t.clusters = slices.Clone(t.clusters)

	return t
}

func TestFat32TableFromBytes(t *testing.T) {
	t.Run("valid FAT32 Table", func(t *testing.T) {
		input, err := os.ReadFile(GetFatDiskImagePath(32))
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", GetFatDiskImagePath(32), err)
		}
		b := input[fsInfo32.firstFAT : fsInfo32.firstFAT+fsInfo32.sectorsPerFAT*fsInfo32.bytesPerSector]
		result := tableFromBytes(b, 32)
		if result == nil {
			t.Fatalf("returned FAT32 Table was nil unexpectedly")
		}
		valid := getValidFatTable(32)
		if !result.equal(valid) {
			diff := cmp.Diff(result, valid, cmp.AllowUnexported(table{}))
			t.Log(diff)
			t.Fatalf("Mismatched FAT32 Table")
		}
	})
}

func TestFat32TableToBytes(t *testing.T) {
	t.Run("valid FAT32 table", func(t *testing.T) {
		table := getValidFatTable(32)
		b := table.bytes(32)
		if b == nil {
			t.Fatal("b was nil unexpectedly")
		}
		valid, err := os.ReadFile(GetFatDiskImagePath(32))
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", GetFatDiskImagePath(32), err)
		}
		validBytes := valid[fsInfo32.firstFAT : fsInfo32.firstFAT+fsInfo32.sectorsPerFAT*fsInfo32.bytesPerSector]
		if !bytes.Equal(validBytes, b) {
			_, diffString := util.DumpByteSlicesWithDiffs(validBytes, b, 32, false, true, true)
			t.Errorf("directory.toBytes() mismatched, actual then expected\n%s", diffString)
		}
	})
}

func TestFatTableIsEoc(t *testing.T) {
	tests := []struct {
		cluster uint32
		eoc     bool
		fatType int
	}{
		{0xa7, false, 12},
		{0x00, false, 12},
		{0xFF7, false, 12},
		{0xFF8, true, 12},
		{0xFF9, true, 12},
		{0xFFA, true, 12},
		{0xFFB, true, 12},
		{0xFFC, true, 12},
		{0xFFD, true, 12},
		{0xFFE, true, 12},
		{0xFFF, true, 12},
		{0xAFFF, true, 12},
		{0x2FF8, true, 12},

		{0xa7, false, 16},
		{0x00, false, 16},
		{0xFFFF7, false, 16},
		{0xFFFF8, true, 16},
		{0xFFFF9, true, 16},
		{0xFFFFA, true, 16},
		{0xFFFFB, true, 16},
		{0xFFFFC, true, 16},
		{0xFFFFD, true, 16},
		{0xFFFFE, true, 16},
		{0xFFFFF, true, 16},
		{0xAFFFFF, true, 16},
		{0x2FFFF8, true, 16},

		{0xa7, false, 32},
		{0x00, false, 32},
		{0xFFFFFF7, false, 32},
		{0xFFFFFF8, true, 32},
		{0xFFFFFF9, true, 32},
		{0xFFFFFFA, true, 32},
		{0xFFFFFFB, true, 32},
		{0xFFFFFFC, true, 32},
		{0xFFFFFFD, true, 32},
		{0xFFFFFFE, true, 32},
		{0xFFFFFFF, true, 32},
		{0xAFFFFFFF, true, 32},
		{0x2FFFFFF8, true, 32},
	}
	tab := table{}
	for _, tt := range tests {
		eoc := tab.isEoc(tt.cluster, tt.fatType)
		if eoc != tt.eoc {
			t.Errorf("isEoc(%x): actual %t instead of expected %t", tt.cluster, eoc, tt.eoc)
		}
	}
}
