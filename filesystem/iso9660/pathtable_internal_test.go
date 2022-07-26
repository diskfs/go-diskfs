package iso9660

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func getBasePathTable(f string) (table *pathTable, lbytes, mbytes []byte, err error) {
	blocksize := 2048
	// sector 27 - L path table
	// sector 28 - M path table
	pathTableLSector := 35
	pathTableMSector := 36
	// read correct bytes off of disk
	input, err := os.ReadFile(f)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error reading data from iso9660 test fixture %s: %v", f, err)
	}

	startL := pathTableLSector * blocksize // start of pathtable in file.iso

	// one block, since we know it is just one block
	LBytes := input[startL : startL+blocksize]

	startM := pathTableMSector * blocksize // start of pathtable in file.iso

	// one block, since we know it is just one block
	MBytes := input[startM : startM+blocksize]

	entries := []*pathTableEntry{
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x12, parentIndex: 0x1, dirname: "\x00"},
		{nameSize: 0x3, size: 0xc, extAttrLength: 0x0, location: 0x13, parentIndex: 0x1, dirname: "ABC"},
		{nameSize: 0x3, size: 0xc, extAttrLength: 0x0, location: 0x14, parentIndex: 0x1, dirname: "BAR"},
		{nameSize: 0x4, size: 0xc, extAttrLength: 0x0, location: 0x15, parentIndex: 0x1, dirname: "DEEP"},
		{nameSize: 0x3, size: 0xc, extAttrLength: 0x0, location: 0x21, parentIndex: 0x1, dirname: "FOO"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x16, parentIndex: 0x4, dirname: "A"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x17, parentIndex: 0x6, dirname: "B"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x18, parentIndex: 0x7, dirname: "C"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x19, parentIndex: 0x8, dirname: "D"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x1a, parentIndex: 0x9, dirname: "E"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x1b, parentIndex: 0xa, dirname: "F"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x1c, parentIndex: 0xb, dirname: "G"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x1d, parentIndex: 0xc, dirname: "H"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x1e, parentIndex: 0xd, dirname: "I"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x1f, parentIndex: 0xe, dirname: "J"},
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x20, parentIndex: 0xf, dirname: "K"},
	}

	return &pathTable{
		records: entries,
	}, LBytes, MBytes, nil
}

func get9660PathTable() (table *pathTable, lbytes, mbytes []byte, err error) {
	return getBasePathTable(ISO9660File)
}

func getRockRidgePathTable() (table *pathTable, lbytes, mbytes []byte, err error) {
	return getBasePathTable(RockRidgeFile)
}

func TestPathTableToLBytes(t *testing.T) {
	// the one on disk is padded to the end of the sector
	b := make([]byte, 2048)
	validTable, validBytes, _, _ := get9660PathTable()
	b2 := validTable.toLBytes()
	copy(b, b2)

	if !bytes.Equal(b, validBytes) {
		t.Errorf("Mismatched path table bytes. Actual then expected")
		t.Logf("%#v", b)
		t.Logf("%#v", validBytes)
	}
}
func TestPathTableToMBytes(t *testing.T) {
	// the one on disk is padded to the end of the sector
	b := make([]byte, 2048)
	validTable, _, validBytes, _ := get9660PathTable()
	b2 := validTable.toMBytes()
	copy(b, b2)

	if !bytes.Equal(b, validBytes) {
		t.Errorf("Mismatched path table bytes. Actual then expected")
		t.Logf("%#v", b)
		t.Logf("%#v", validBytes)
	}
}

func TestPathTableGetLocation(t *testing.T) {
	//nolint:dogsled // we do not care about too many underbar here
	table, _, _, _ := get9660PathTable()
	tests := []struct {
		path     string
		location uint32
		err      error
	}{
		{"/", 0x12, nil},
		{"/FOO", 0x21, nil},
		{"/nothereatall", 0x00, nil},
	}

	for _, tt := range tests {
		location := table.getLocation(tt.path)
		if location != tt.location {
			t.Errorf("Mismatched location, actual: %d vs expected: %d", location, tt.location)
		}
	}
}

func TestParsePathTable(t *testing.T) {
	validTable, b, _, _ := get9660PathTable()
	table := parsePathTable(b)
	if !table.equal(validTable) {
		t.Errorf("Mismatched path tables. Actual then expected")
		t.Logf("%#v", table.records)
		t.Logf("%#v", validTable.records)
	}
}
