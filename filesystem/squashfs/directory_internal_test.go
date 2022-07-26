package squashfs

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

var (
	// this has convenient linebreaks between header and entries
	testDirectoryTable = []byte{
		0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x64, 0x31,
		0x20, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x64, 0x32,
		0x40, 0x00, 0x02, 0x00, 0x01, 0x00, 0x01, 0x00, 0x64, 0x33,
	}
	testDirectoryHeaderBytes  = testDirectoryTable[:12]
	testDirectoryEntriesBytes = [][]byte{testDirectoryTable[12:22], testDirectoryTable[22:32], testDirectoryTable[32:42]}
	testDirectoryHeader       = directoryHeader{count: 3, startBlock: 0, inode: 1}
	testDirectoryEntries      = []*directoryEntryRaw{
		{offset: 0x0, inodeNumber: 0x1, inodeType: inodeType(0x1), name: "d1", isSubdirectory: true},
		{offset: 0x20, inodeNumber: 0x2, inodeType: inodeType(0x1), name: "d2", isSubdirectory: true},
		{offset: 0x40, inodeNumber: 0x3, inodeType: inodeType(0x1), name: "d3", isSubdirectory: true},
	}
	testDirectory = &directory{
		entries: testDirectoryEntries,
	}
)

func TestParseDirectoryHeader(t *testing.T) {
	tests := []struct {
		b      []byte
		header *directoryHeader
		err    error
	}{
		{testDirectoryHeaderBytes, &testDirectoryHeader, nil},
		{testDirectoryHeaderBytes[:2], nil, fmt.Errorf("header was 2 bytes, less than minimum 12")},
	}
	//nolint:dupl // these tests are not exactly identical, easier to leave as is
	for i, tt := range tests {
		header, err := parseDirectoryHeader(tt.b)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case (header == nil && tt.header != nil) || (header != nil && tt.header == nil) || (header != nil && tt.header != nil && *header != *tt.header):
			t.Errorf("%d: mismatched header, actual then expected", i)
			t.Logf("%v", header)
			t.Logf("%v", tt.header)
		}
	}
}

func TestDirectoryHeaderToBytes(t *testing.T) {
	// func (d *directoryEntryRaw) toBytes() []byte {
	b := testDirectoryHeader.toBytes()
	if !bytes.Equal(testDirectoryHeaderBytes, b) {
		t.Errorf("mismatched header bytes, actual then expected")
		t.Logf("%x", b)
		t.Logf("%x", testDirectoryHeaderBytes)
	}
}

func TestParseDirectoryEntry(t *testing.T) {
	tests := []struct {
		b     []byte
		entry *directoryEntryRaw
		err   error
	}{
		{testDirectoryEntriesBytes[0], testDirectoryEntries[0], nil},
		{testDirectoryEntriesBytes[1], testDirectoryEntries[1], nil},
		{testDirectoryEntriesBytes[2], testDirectoryEntries[2], nil},
		{testDirectoryEntriesBytes[0][:2], nil, fmt.Errorf("directory entry was 2 bytes, less than minimum 8")},
	}
	for i, tt := range tests {
		entry, _, err := parseDirectoryEntry(tt.b, 1)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case (entry == nil && tt.entry != nil) || (entry != nil && tt.entry == nil) || (entry != nil && tt.entry != nil && *entry != *tt.entry):
			t.Errorf("%d: mismatched entry, actual then expected", i)
			t.Logf("%#v", entry)
			t.Logf("%#v", tt.entry)
		}
	}
}

func TestDirectoryEntryToBytes(t *testing.T) {
	tests := []struct {
		b     []byte
		entry *directoryEntryRaw
	}{
		{testDirectoryEntriesBytes[0], testDirectoryEntries[0]},
		{testDirectoryEntriesBytes[1], testDirectoryEntries[1]},
		{testDirectoryEntriesBytes[2], testDirectoryEntries[2]},
	}
	for i, tt := range tests {
		b := tt.entry.toBytes(1)
		if !bytes.Equal(b, tt.b) {
			t.Errorf("%d: mismatched bytes, actual then expected", i)
			t.Logf("% x", b)
			t.Logf("% x", tt.b)
		}
	}
}

func TestParseDirectory(t *testing.T) {
	tests := []struct {
		b   []byte
		dir *directory
		err error
	}{
		{testDirectoryTable, testDirectory, nil},
		{testDirectoryTable[:10], nil, fmt.Errorf("could not parse directory header: header was 10 bytes, less than minimum 12")},
	}
	for i, tt := range tests {
		dir, err := parseDirectory(tt.b)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case !dir.equal(tt.dir):
			t.Errorf("%d: mismatched dir, actual then expected", i)
			t.Logf("%v", dir)
			t.Logf("%v", tt.dir)
		}
	}
}

func TestDirectoryToBytes(t *testing.T) {
	b := testDirectory.toBytes(1)
	if !bytes.Equal(b, testDirectoryTable) {
		t.Errorf("mismatched bytes, actual then expected")
		t.Logf("% x", b)
		t.Logf("% x", testDirectoryTable)
	}
}
