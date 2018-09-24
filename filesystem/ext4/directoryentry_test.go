package ext4

import (
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
