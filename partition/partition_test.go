package partition_test

/*
 These tests the exported functions
 We want to do full-in tests with files
*/

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/partition"
	"github.com/diskfs/go-diskfs/partition/gpt"
	"github.com/diskfs/go-diskfs/partition/mbr"
)

const (
	logicalBlockSize               = 512
	mbrPartitionEntriesOffset      = 446
	mbrPartitionEntrySize          = 16
	mbrPartitionTypeOffset         = 4
	mbrPartitionStartLBAOffset     = 8
	mbrPartitionSizeInLBAOffset    = 12
	mbrSignatureOffset             = 510
	conventionalPartitionStartLBA  = 2048
	conventionalPartitionSizeInLBA = 1024
	gptTestImage                   = "./gpt/testdata/gpt.img"
	mbrTestImage                   = "./mbr/testdata/mbr.img"
)

func TestRead(t *testing.T) {
	tests := []struct {
		path      string
		tableType string
		err       error
	}{
		{"./mbr/testdata/mbr.img", "mbr", nil},
		{"./gpt/testdata/gpt.img", "gpt", nil},
		{"", "", fmt.Errorf("unknown disk partition type")},
	}
	for _, t2 := range tests {
		tt := t2
		t.Run(tt.tableType, func(t *testing.T) {
			// create a blank file if we did not provide a path to a test file
			var f *os.File
			var err error
			if tt.path == "" {
				filename := "partition_test"
				f, err = os.CreateTemp("", filename)
				// make it a 10MB file
				_ = f.Truncate(10 * 1024 * 1024)
				defer f.Close()
				defer os.Remove(f.Name())
				if err != nil {
					t.Errorf("Failed to create tempfile %s :%v", filename, err)
					return
				}
			} else {
				f, err = os.Open(tt.path)
				if err != nil {
					t.Errorf("Failed to open file %s :%v", tt.path, err)
					return
				}
			}

			// be sure to close the file
			defer f.Close()

			table, err := partition.Read(f, 512, 512)

			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("read(%s): mismatched errors, actual %v expected %v", f.Name(), err, tt.err)
			case (table == nil && tt.tableType != "") || (table != nil && tt.tableType == "") || (table != nil && table.Type() != tt.tableType):
				t.Errorf("Create(%s): mismatched table, actual then expected", f.Name())
				t.Logf("%v", table.Type())
				t.Logf("%v", tt.tableType)
			}
		})
	}
}

func TestReadGPTPolicy(t *testing.T) {
	t.Run("default prefers conventional MBR over valid GPT", func(t *testing.T) {
		f := openGPTTestImage(t, replaceLBA0WithConventionalMBR)
		table, err := partition.Read(f, logicalBlockSize, logicalBlockSize)
		assertTableType(t, table, err, "mbr")
	})

	t.Run("GPTWithMBR prefers conventional MBR over valid GPT", func(t *testing.T) {
		f := openGPTTestImage(t, replaceLBA0WithConventionalMBR)
		table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, partition.GPTWithMBR)
		assertTableType(t, table, err, "mbr")
	})

	t.Run("GPTIgnoreMBR preserves GPT-first behavior", func(t *testing.T) {
		f := openGPTTestImage(t, replaceLBA0WithConventionalMBR)
		table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, partition.GPTIgnoreMBR)
		assertTableType(t, table, err, "gpt")
	})

	t.Run("protective MBR selects GPT", func(t *testing.T) {
		f := openGPTTestImage(t, nil)
		table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, partition.GPTWithMBR)
		assertTableType(t, table, err, "gpt")
	})

	t.Run("hybrid MBR selects GPT", func(t *testing.T) {
		f := openGPTTestImage(t, func(sector []byte) {
			writeMBRPartitionEntry(sector, 1, byte(mbr.Linux), conventionalPartitionStartLBA, conventionalPartitionSizeInLBA)
		})
		table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, partition.GPTWithMBR)
		assertTableType(t, table, err, "gpt")
		gptTable, ok := table.(*gpt.Table)
		if !ok {
			t.Fatalf("ReadWithPolicy returned %T instead of *gpt.Table", table)
		}
		if gptTable.ProtectiveMBR {
			t.Fatal("hybrid MBR unexpectedly reported as a pure protective MBR")
		}
	})

	t.Run("no valid MBR selects GPT", func(t *testing.T) {
		f := openGPTTestImage(t, func(sector []byte) {
			clear(sector)
		})
		table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, partition.GPTWithMBR)
		assertTableType(t, table, err, "gpt")
	})

	t.Run("invalid protective entry selects MBR", func(t *testing.T) {
		f := openGPTTestImage(t, func(sector []byte) {
			start := mbrPartitionEntriesOffset + mbrPartitionStartLBAOffset
			binary.LittleEndian.PutUint32(sector[start:start+4], 2)
		})
		table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, partition.GPTWithMBR)
		assertTableType(t, table, err, "mbr")
	})

	t.Run("signed empty MBR selects MBR", func(t *testing.T) {
		f := openGPTTestImage(t, func(sector []byte) {
			clear(sector[mbrPartitionEntriesOffset:mbrSignatureOffset])
		})
		table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, partition.GPTWithMBR)
		assertTableType(t, table, err, "mbr")
	})

	t.Run("invalid GPT falls back to MBR", func(t *testing.T) {
		for _, policy := range []partition.GPTPolicy{partition.GPTWithMBR, partition.GPTIgnoreMBR} {
			f, err := os.Open(mbrTestImage)
			if err != nil {
				t.Fatalf("unable to open MBR test image: %v", err)
			}
			t.Cleanup(func() { _ = f.Close() })
			table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, policy)
			assertTableType(t, table, err, "mbr")
		}
	})

	t.Run("invalid policy", func(t *testing.T) {
		f := openGPTTestImage(t, nil)
		table, err := partition.ReadWithPolicy(f, logicalBlockSize, logicalBlockSize, partition.GPTPolicy(99))
		if table != nil {
			t.Fatalf("ReadWithPolicy returned %T instead of nil", table)
		}
		if err == nil || !strings.Contains(err.Error(), "unknown GPT policy") {
			t.Fatalf("ReadWithPolicy returned error %v instead of an unknown policy error", err)
		}
	})
}

func replaceLBA0WithConventionalMBR(sector []byte) {
	clear(sector[mbrPartitionEntriesOffset:mbrSignatureOffset])
	writeMBRPartitionEntry(sector, 0, byte(mbr.Linux), conventionalPartitionStartLBA, conventionalPartitionSizeInLBA)
}

func writeMBRPartitionEntry(sector []byte, index int, partitionType byte, startLBA, sizeInLBA uint32) {
	entryStart := mbrPartitionEntriesOffset + index*mbrPartitionEntrySize
	entry := sector[entryStart : entryStart+mbrPartitionEntrySize]
	clear(entry)
	entry[mbrPartitionTypeOffset] = partitionType
	binary.LittleEndian.PutUint32(entry[mbrPartitionStartLBAOffset:], startLBA)
	binary.LittleEndian.PutUint32(entry[mbrPartitionSizeInLBAOffset:], sizeInLBA)
}

func openGPTTestImage(t *testing.T, mutateLBA0 func([]byte)) *os.File {
	t.Helper()
	b, err := os.ReadFile(gptTestImage)
	if err != nil {
		t.Fatalf("unable to read GPT test image: %v", err)
	}
	if mutateLBA0 != nil {
		mutateLBA0(b[:logicalBlockSize])
	}
	f, err := os.CreateTemp(t.TempDir(), "partition-*.img")
	if err != nil {
		t.Fatalf("unable to create temporary test image: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.Write(b); err != nil {
		t.Fatalf("unable to write temporary test image: %v", err)
	}
	return f
}

func assertTableType(t *testing.T, table partition.Table, err error, expected string) {
	t.Helper()
	if err != nil {
		t.Fatalf("partition read returned unexpected error: %v", err)
	}
	if table == nil {
		t.Fatal("partition read returned a nil table")
	}
	if table.Type() != expected {
		t.Fatalf("partition read returned %s table instead of %s", table.Type(), expected)
	}
}
