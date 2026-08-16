package ext4

import (
	"encoding/binary"
	"testing"

	"github.com/go-test/deep"
)

func TestDirectoryEntriesFromBytes(t *testing.T) {
	expected, blocksize, b, err := testGetValidRootDirectory()
	if err != nil {
		t.Fatal(err)
	}
	// remove checksums, as we are not testing those here
	b = b[:len(b)-minDirEntryLength]
	entries, err := parseDirEntriesLinear(b, false, blocksize, 2, 0, 0)
	if err != nil {
		t.Fatalf("Failed to parse directory entries: %v", err)
	}
	deep.CompareUnexportedFields = true
	if diff := deep.Equal(expected.entries, entries); diff != nil {
		t.Errorf("directoryFromBytes() = %v", diff)
	}
}

func TestDirectoryEntryFromBytesRejectsNameBeyondRecord(t *testing.T) {
	b := make([]byte, minDirEntryLength)
	binary.LittleEndian.PutUint16(b[0x4:0x6], uint16(len(b)))
	b[0x6] = 250

	if _, err := directoryEntryFromBytes(b); err == nil {
		t.Fatal("expected an error for a name length exceeding the record")
	}
}

func TestParseDirEntriesLinearRejectsInvalidRecordLengths(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "truncated header",
			input: make([]byte, minDirEntryLength-1),
		},
		{
			name: "record beyond remaining data",
			input: func() []byte {
				b := make([]byte, minDirEntryLength)
				binary.LittleEndian.PutUint16(b[0x4:0x6], uint16(minDirEntryLength+4))
				return b
			}(),
		},
		{
			name: "record below minimum",
			input: func() []byte {
				b := make([]byte, minDirEntryLength)
				binary.LittleEndian.PutUint16(b[0x4:0x6], uint16(minDirEntryLength-4))
				return b
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseDirEntriesLinear(tt.input, false, 4096, 2, 0, 0); err == nil {
				t.Fatal("expected malformed directory entry to return an error")
			}
		})
	}
}
