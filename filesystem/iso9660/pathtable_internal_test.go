package iso9660

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func getValidPathTable() (*pathTable, []byte, []byte, error) {
	blocksize := 2048
	// sector 27 - L path table
	// sector 28 - M path table
	pathTableLSector := 27
	pathTableMSector := 28
	// read correct bytes off of disk
	input, err := ioutil.ReadFile(ISO9660File)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Error reading data from iso9660 test fixture %s: %v", ISO9660File, err)
	}

	startL := pathTableLSector * blocksize // start of pathtable in file.iso

	// one block, since we know it is just one block
	LBytes := input[startL : startL+blocksize]

	startM := pathTableMSector * blocksize // start of pathtable in file.iso

	// one block, since we know it is just one block
	MBytes := input[startM : startM+blocksize]

	entries := []*pathTableEntry{
		{nameSize: 0x1, size: 0xa, extAttrLength: 0x0, location: 0x12, parentIndex: 0x1, dirname: "\x00"},
		{nameSize: 0x3, size: 0xc, extAttrLength: 0x0, location: 0x14, parentIndex: 0x1, dirname: "ABC"},
		{nameSize: 0x3, size: 0xc, extAttrLength: 0x0, location: 0x15, parentIndex: 0x1, dirname: "BAR"},
		{nameSize: 0x3, size: 0xc, extAttrLength: 0x0, location: 0x16, parentIndex: 0x1, dirname: "FOO"},
	}

	return &pathTable{
		records: entries,
	}, LBytes, MBytes, nil
}

func TestPathTableToLBytes(t *testing.T) {
	// the one on disk is padded to the end of the sector
	b := make([]byte, 2048)
	validTable, validBytes, _, _ := getValidPathTable()
	b2 := validTable.toLBytes()
	copy(b, b2)

	if bytes.Compare(b, validBytes) != 0 {
		t.Errorf("Mismatched path table bytes. Actual then expected")
		t.Logf("%#v", b)
		t.Logf("%#v", validBytes)
	}
}
func TestPathTableToMBytes(t *testing.T) {
	// the one on disk is padded to the end of the sector
	b := make([]byte, 2048)
	validTable, _, validBytes, _ := getValidPathTable()
	b2 := validTable.toMBytes()
	copy(b, b2)

	if bytes.Compare(b, validBytes) != 0 {
		t.Errorf("Mismatched path table bytes. Actual then expected")
		t.Logf("%#v", b)
		t.Logf("%#v", validBytes)
	}
}

func TestPathTableGetLocation(t *testing.T) {
	table, _, _, _ := getValidPathTable()
	tests := []struct {
		path     string
		location uint32
		err      error
	}{
		{"/", 0x12, nil},
		{"/FOO", 0x16, nil},
		{"/nothereatall", 0x00, nil},
	}

	for _, tt := range tests {
		location, err := table.getLocation(tt.path)
		if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) {
			t.Errorf("Mismatched error, actual: %v vs expected: %v", err, tt.err)
		}
		if location != tt.location {
			t.Errorf("Mismatched location, actual: %d vs expected: %d", location, tt.location)
		}
	}
}

func TestParsePathTable(t *testing.T) {
	validTable, b, _, _ := getValidPathTable()
	table, err := parsePathTable(b)
	if err != nil {
		t.Errorf("Unexpected error when parsing path table: %v", err)
	}
	if !table.equal(validTable) {
		t.Errorf("Mismatched path tables. Actual then expected")
		t.Logf("%#v", table.records)
		t.Logf("%#v", validTable.records)
	}
}
