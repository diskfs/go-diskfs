package ext4

import (
	"testing"

	"github.com/diskfs/go-diskfs/testhelper"
)

func TestDirectoryToBytes(t *testing.T) {
	dir, bytesPerBlock, expected, err := testGetValidRootDirectory()
	if err != nil {
		t.Fatal(err)
	}
	//nolint:dogsled // we know and we do not care
	sb, _, _, _, err := testGetValidSuperblockAndGDTs()
	if err != nil {
		t.Fatal(err)
	}
	b := dir.toBytes(bytesPerBlock, directoryChecksumAppender(sb.checksumSeed, 2, 0))

	// read the bytes from the disk
	diff, diffString := testhelper.DumpByteSlicesWithDiffs(b, expected, 32, false, true, true)
	if diff {
		t.Errorf("directory.toBytes() mismatched, actual then expected\n%s", diffString)
	}
}
