package mbr

import (
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"testing"
)

const (
	mbrFile = "./testdata/mbr.img"
	// retrieved via `blkid ./testdata/mbr.img`
	testPartitionTableUUID = "10e9203d"
)

func GetValidTable() *Table {
	table := &Table{
		LogicalSectorSize:  512,
		PhysicalSectorSize: 512,
		partitionTableUUID: testPartitionTableUUID,
	}
	parts := []*Partition{
		{
			Bootable:      false,
			StartHead:     0x20,
			StartSector:   0x21,
			StartCylinder: 0x00,
			Type:          Linux,
			EndHead:       0x31,
			EndSector:     0x18,
			EndCylinder:   0x00,
			Start:         partitionStart,
			Size:          partitionSize,
			partitionUUID: formatPartitionUUID(testPartitionTableUUID, 1),
		},
	}
	// add 3 unused partitions to the table
	for i := 1; i < 4; i++ {
		parts = append(parts, &Partition{Type: Empty, partitionUUID: formatPartitionUUID(testPartitionTableUUID, i+1)})
	}
	table.Partitions = parts
	return table
}

func TestTableFromBytes(t *testing.T) {
	t.Run("Short byte slice", func(t *testing.T) {
		b := make([]byte, 512-1)
		_, _ = rand.Read(b)
		table, err := tableFromBytes(b)
		if table != nil {
			t.Error("should return nil table")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := fmt.Sprintf("data for partition was %d bytes", len(b))
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid MBR Signature", func(t *testing.T) {
		b, err := os.ReadFile(mbrFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", mbrFile, err)
		}
		b[511] = 0x00
		table, err := tableFromBytes(b[:512])
		if table != nil {
			t.Error("should return nil table")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "invalid MBR Signature"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("Valid table", func(t *testing.T) {
		b, err := os.ReadFile(mbrFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", mbrFile, err)
		}
		table, err := tableFromBytes(b[:512])
		if table == nil {
			t.Error("should not return nil table")
		}
		if err != nil {
			t.Errorf("returned non-nil error: %v", err)
		}
		expected := GetValidTable()
		if table == nil && expected != nil || !table.Equal(expected) {
			t.Errorf("actual table was %v instead of expected %v", table, expected)
		}
		if table.partitionTableUUID != testPartitionTableUUID {
			t.Errorf("expected partition table UUID %s, but found %s", testPartitionTableUUID, table.partitionTableUUID)
		}
		for i := range table.Partitions {
			if table.Partitions[i].UUID() != expected.Partitions[i].UUID() {
				t.Errorf(
					"expected partition nr %d to have UUID %s, but found %s",
					i+1,
					expected.Partitions[i].UUID(),
					table.Partitions[i].UUID(),
				)
			}
		}
	})
}
