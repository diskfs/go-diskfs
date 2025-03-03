package ext4

import (
	"reflect"
	"testing"

	"github.com/diskfs/go-diskfs/testhelper"
	"github.com/go-test/deep"
)

func TestSuperblockFromBytes(t *testing.T) {
	expected, _, b, _, err := testGetValidSuperblockAndGDTs()
	if err != nil {
		t.Fatalf("Failed to create valid superblock: %v", err)
	}
	sb, err := superblockFromBytes(b)
	if err != nil {
		t.Fatalf("Failed to parse superblock bytes: %v", err)
	}

	deep.CompareUnexportedFields = true
	if diff := deep.Equal(*expected, *sb); diff != nil {
		t.Errorf("superblockFromBytes() = %v", diff)
	}
}

func TestSuperblockToBytes(t *testing.T) {
	sb, _, expected, _, err := testGetValidSuperblockAndGDTs()
	if err != nil {
		t.Fatalf("Failed to create valid superblock: %v", err)
	}
	b, err := sb.toBytes()
	if err != nil {
		t.Fatalf("Failed to serialize superblock: %v", err)
	}
	diff, diffString := testhelper.DumpByteSlicesWithDiffs(b, expected, 32, false, true, true)
	if diff {
		t.Errorf("superblock.toBytes() mismatched, actual then expected\n%s", diffString)
	}
}

func TestCalculateBackupSuperblocks(t *testing.T) {
	tests := []struct {
		bgs      int64
		expected []int64
	}{
		// Test case 1: Single block group
		{bgs: 2, expected: []int64{1}},

		// Test case 2: Multiple block groups
		{bgs: 119, expected: []int64{1, 3, 5, 7, 9, 25, 27, 49, 81}},

		// Test case 3: Large number of block groups
		{bgs: 746, expected: []int64{1, 3, 5, 7, 9, 25, 27, 49, 81, 125, 243, 343, 625, 729}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := calculateBackupSuperblockGroups(tt.bgs)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("calculateBackupSuperblockGroups(%d) = %v; want %v",
					tt.bgs, result, tt.expected)
			}
		})
	}
}
