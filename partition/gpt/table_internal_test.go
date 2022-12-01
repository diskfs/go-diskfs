package gpt

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

const (
	gptFile = "./testdata/gpt.img"
)

func GetValidTable() *Table {
	// check out data
	table := Table{
		LogicalSectorSize:  512,
		PhysicalSectorSize: 512,
		partitionEntrySize: 128,
		primaryHeader:      1,
		secondaryHeader:    20479,
		firstDataSector:    34,
		lastDataSector:     20446,
		partitionArraySize: 128,
		ProtectiveMBR:      true,
		GUID:               "43E51892-3273-42F7-BCDA-B43B80CDFC48",
	}
	parts := []*Partition{
		{
			Start:              2048,
			End:                3048,
			Size:               (3048 - 2048 + 1) * 512,
			Name:               "EFI System",
			GUID:               "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes:         0,
			Type:               EFISystemPartition,
			logicalSectorSize:  512,
			physicalSectorSize: 512,
		},
	}
	// add 127 Unused partitions to the table
	for i := 1; i < 128; i++ {
		parts = append(parts, &Partition{Type: Unused})
	}
	table.Partitions = parts
	return &table
}

func TestTableFromBytes(t *testing.T) {
	t.Run("Short byte slice", func(t *testing.T) {
		b := make([]byte, 512+512-1)
		_, _ = rand.Read(b)
		table, err := tableFromBytes(b, 512, 512)
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
	t.Run("invalid EFI Signature", func(t *testing.T) {
		b, err := os.ReadFile(gptFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptFile, err)
		}
		b[512] = 0x00
		table, err := tableFromBytes(b, 512, 512)
		if table != nil {
			t.Error("should return nil table")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "invalid EFI Signature"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid EFI Revision", func(t *testing.T) {
		b, err := os.ReadFile(gptFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptFile, err)
		}
		b[512+10] = 0xff
		table, err := tableFromBytes(b, 512, 512)
		if table != nil {
			t.Error("should return nil table")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "invalid EFI Revision"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid EFI Header Size", func(t *testing.T) {
		b, err := os.ReadFile(gptFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptFile, err)
		}
		b[512+12]++
		table, err := tableFromBytes(b, 512, 512)
		if table != nil {
			t.Error("should return nil table")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "invalid EFI Header size"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid EFI Zeroes", func(t *testing.T) {
		b, err := os.ReadFile(gptFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptFile, err)
		}
		b[512+20] = 0x01
		table, err := tableFromBytes(b, 512, 512)
		if table != nil {
			t.Error("should return nil table")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "invalid EFI Header, expected zeroes"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid EFI Header Checksum", func(t *testing.T) {
		b, err := os.ReadFile(gptFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptFile, err)
		}
		b[512+16]++
		table, err := tableFromBytes(b, 512, 512)
		if table != nil {
			t.Error("should return nil table")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "invalid EFI Header Checksum"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
}

type byteBufferReader struct {
	b   []byte
	pos int
}

func (b *byteBufferReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(b.b)) {
		return 0, io.EOF
	}
	if off < 0 {
		return 0, fmt.Errorf("invalid offset %d", off)
	}
	n = copy(p, b.b[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (b *byteBufferReader) WriteAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(b.b)) {
		return 0, io.EOF
	}
	if off < 0 {
		return 0, fmt.Errorf("invalid offset %d", off)
	}
	n = copy(b.b[off:], p)
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (b *byteBufferReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.pos = int(offset)
	case io.SeekCurrent:
		b.pos += int(offset)
	case io.SeekEnd:
		b.pos = len(b.b) + int(offset)
	}
	return int64(b.pos), nil
}

func TestRead(t *testing.T) {
	t.Run("invalid EFI Partition Checksum", func(t *testing.T) {
		b, err := os.ReadFile(gptFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptFile, err)
		}
		// change a single byte in a partition entry
		b[512+512+400]++
		buf := &byteBufferReader{b: b}
		table, err := Read(buf, 512, 512)
		if table != nil {
			t.Error("should return nil table")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "invalid EFI Partition Entry Checksum"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("Valid table", func(t *testing.T) {
		b, err := os.ReadFile(gptFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptFile, err)
		}
		buf := &byteBufferReader{b: b}
		table, err := Read(buf, 512, 512)
		if table == nil {
			t.Error("should not return nil table")
		}
		if err != nil {
			t.Errorf("returned non-nil error: %v", err)
		}
		expected := GetValidTable()
		if table == nil || !table.Equal(expected) {
			t.Errorf("mismatched\nactual: %#v\nexpected %#v", table, expected)
		}
	})
}
