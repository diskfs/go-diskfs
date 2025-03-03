package ext4

import (
	"fmt"
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/testhelper"
	"github.com/go-test/deep"
)

func testGetValidRootDirectory() (dir *Directory, bytesPerBlock uint32, contents []byte, err error) {
	// read the root directory file, which was created from debugfs
	rootDirEntries, err := testDirEntriesFromDebugFS(rootDirFile)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("error reading root directory entries from debugfs: %w", err)
	}
	dir = &Directory{
		root:    true,
		entries: rootDirEntries,
	}

	testfile := testRootDirFile
	// read the bytes from the disk
	b, err := os.ReadFile(testfile)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("Failed to read %s", testfile)
	}

	return dir, 1024, b, nil
}

func TestGroupDescriptorFromBytes(t *testing.T) {
	sb, gds, _, b, err := testGetValidSuperblockAndGDTs()
	if err != nil {
		t.Fatalf("Error getting valid superblock: %v", err)
	}
	// we know which one we are reading
	expected := &gds[0]
	if err != nil {
		t.Fatalf("Error getting valid group descriptor: %v", err)
	}
	gd, err := groupDescriptorFromBytes(b, sb.groupDescriptorSize, int(expected.number), sb.gdtChecksumType(), sb.checksumSeed)
	if err != nil {
		t.Errorf("Error parsing group descriptor: %v", err)
	}
	deep.CompareUnexportedFields = true
	if diff := deep.Equal(gd, expected); diff != nil {
		t.Errorf("groupDescriptorFromBytes() = %v", diff)
	}
}

func TestGroupDescriptorToBytes(t *testing.T) {
	sb, gds, _, expected, err := testGetValidSuperblockAndGDTs()
	if err != nil {
		t.Fatalf("Error getting valid superblock: %v", err)
	}
	gd := &gds[0]
	if err != nil {
		t.Fatalf("Error getting valid group descriptor: %v", err)
	}
	b := gd.toBytes(sb.gdtChecksumType(), sb.checksumSeed)
	expected = expected[:64]
	diff, diffString := testhelper.DumpByteSlicesWithDiffs(b, expected, 32, false, true, true)
	if diff {
		t.Errorf("groupdescriptor.toBytes() mismatched, actual then expected\n%s", diffString)
	}
}

func TestGroupDescriptorsFromBytes(t *testing.T) {
	sb, expected, _, b, err := testGetValidSuperblockAndGDTs()
	if err != nil {
		t.Fatalf("Error getting valid superblock: %v", err)
	}
	gds, err := groupDescriptorsFromBytes(b, sb.groupDescriptorSize, sb.checksumSeed, sb.gdtChecksumType())
	if err != nil {
		t.Errorf("Error parsing group descriptor: %v", err)
	}
	expectedGDS := &groupDescriptors{
		descriptors: expected,
	}
	deep.CompareUnexportedFields = true
	if diff := deep.Equal(gds, expectedGDS); diff != nil {
		t.Errorf("groupDescriptorsFromBytes() = %v", diff)
	}
}

func TestGroupDescriptorsToBytes(t *testing.T) {
	sb, groupdescriptors, _, expected, err := testGetValidSuperblockAndGDTs()
	if err != nil {
		t.Fatalf("Error getting valid superblock: %v", err)
	}

	gds := &groupDescriptors{
		descriptors: groupdescriptors,
	}
	b := gds.toBytes(sb.gdtChecksumType(), sb.checksumSeed)
	diff, diffString := testhelper.DumpByteSlicesWithDiffs(b, expected, 32, false, true, true)
	if diff {
		t.Errorf("groupDescriptors.toBytes() mismatched, actual then expected\n%s", diffString)
	}
}
