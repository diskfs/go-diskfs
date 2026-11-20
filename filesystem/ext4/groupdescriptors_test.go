package ext4

import (
	"bytes"
	"encoding/binary"
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

func TestGroupDescriptorChecksumLargeGroupNumbers(t *testing.T) {
	const seed uint32 = 0x12345678

	tests := []struct {
		group    uint32
		checksum uint16
	}{
		{65535, 0x2068},
		{65536, 0x8eba},
		{65537, 0xaa81},
		{0x80000000, 0x29e5},
		{0xfffffffe, 0xc348},
		{0xffffffff, 0xe773},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("group_%d", tt.group), func(t *testing.T) {
			// Synthetic 64-byte EXT4 group descriptor.
			b := make([]byte, groupDescriptorSize64Bit)

			binary.LittleEndian.PutUint32(b[0x00:], 42)
			binary.LittleEndian.PutUint32(b[0x04:], 100)
			binary.LittleEndian.PutUint16(b[0x1e:], tt.checksum)

			gd, err := groupDescriptorFromBytes(
				b,
				groupDescriptorSize64Bit,
				int(tt.group),
				gdtChecksumMetadata,
				seed,
			)
			if err != nil {
				t.Fatalf("failed to parse group %d: %v", tt.group, err)
			}

			if gd.number != tt.group {
				t.Errorf("group number = %d, want %d",
					gd.number, tt.group)
			}

			// Verify that serialization preserves the checksum.
			got := gd.toBytes(gdtChecksumMetadata, seed)
			if !bytes.Equal(got, b) {
				t.Errorf("group %d: serialized descriptor mismatch\n"+
					"got:  %x\nwant: %x", tt.group, got, b)
			}
		})
	}
}
