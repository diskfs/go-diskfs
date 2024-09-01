package fat32

import (
	"bytes"
	"encoding/binary"
	"os"
	"strings"
	"testing"
)

func getValidDos71EBPB() *dos71EBPB {
	return &dos71EBPB{
		dos331BPB:             getValidDos331BPB(),
		sectorsPerFat:         fsInfo.sectorsPerFAT,
		mirrorFlags:           0,
		version:               0,
		rootDirectoryCluster:  2,
		fsInformationSector:   1,
		backupBootSector:      6,
		bootFileName:          [12]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		driveNumber:           128,
		reservedFlags:         0x00,
		extendedBootSignature: 0x29,
		volumeSerialNumber:    fsInfo.serial,
		volumeLabel:           fsInfo.label,
		fileSystemType:        "FAT32",
	}
}

func TestDos71EBPBFromBytes(t *testing.T) {
	t.Run("mismatched length less than 60", func(t *testing.T) {
		b := make([]byte, 59, 60)
		bpb, size, err := dos71EBPBFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if bpb != nil {
			t.Fatalf("returned bpb was non-nil")
		}
		if size > 0 {
			t.Errorf("read %d bytes instead of 0", size)
		}
		expected := "cannot read DOS 7.1 EBPB from invalid byte slice"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("mismatched length less than 79", func(t *testing.T) {
		b := make([]byte, 78, 79)
		bpb, size, err := dos71EBPBFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if bpb != nil {
			t.Fatalf("returned bpb was non-nil")
		}
		if size > 0 {
			t.Errorf("read %d bytes instead of 0", size)
		}
		expected := "cannot read DOS 7.1 EBPB from invalid byte slice"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("mismatched length greater than 79", func(t *testing.T) {
		b := make([]byte, 80)
		bpb, size, err := dos71EBPBFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if bpb != nil {
			t.Fatalf("returned bpb was non-nil")
		}
		if size > 0 {
			t.Errorf("read %d bytes instead of 0", size)
		}
		expected := "cannot read DOS 7.1 EBPB from invalid byte slice"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid Dos331BPB", func(t *testing.T) {
		size := uint16(511)
		b := make([]byte, 25)
		binary.LittleEndian.PutUint16(b[0:2], size)
		bpb, err := dos331BPBFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if bpb != nil {
			t.Fatalf("returned bpb was non-nil")
		}
		expected := "error reading embedded DOS 2.0 BPB"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("valid short ebpb", func(t *testing.T) {
		input, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		inputBytes := input[11:71]
		inputBytes[55] = 0x28
		bpb, size, err := dos71EBPBFromBytes(inputBytes)
		if err != nil {
			t.Errorf("returned unexpected non-nil error: %v", err)
		}
		if bpb == nil {
			t.Fatalf("returned bpb was nil")
		}
		if size != len(inputBytes) {
			t.Errorf("read %d bytes instead of %d", size, len(inputBytes))
		}
		// get a valid one - valid short has no VolumeLabel or FileSystemType
		valid := getValidDos71EBPB()
		valid.extendedBootSignature = 0x28
		valid.volumeLabel = ""
		valid.fileSystemType = ""
		if !bpb.equal(valid) {
			t.Logf("%#v", bpb)
			t.Logf("%#v", valid)
			t.Fatalf("Mismatched BPB")
		}
	})
	t.Run("valid long ebpb", func(t *testing.T) {
		input, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		inputBytes := input[11:90]
		bpb, size, err := dos71EBPBFromBytes(inputBytes)
		if err != nil {
			t.Errorf("returned unexpected non-nil error: %v", err)
		}
		if bpb == nil {
			t.Fatalf("returned bpb was nil")
		}
		if size != len(inputBytes) {
			t.Errorf("read %d bytes instead of %d", size, len(inputBytes))
		}
		valid := getValidDos71EBPB()
		if !bpb.equal(valid) {
			t.Log(bpb)
			t.Log(valid)
			t.Fatalf("Mismatched BPB")
		}
	})
}

func TestDos71EBPBToBytes(t *testing.T) {
	t.Run("short Volume Label", func(t *testing.T) {
		label := "abc"
		bpb := getValidDos71EBPB()
		bpb.volumeLabel = label
		b, err := bpb.toBytes()
		if err != nil {
			t.Errorf("error was not nil, instead %v", err)
		}
		if b == nil {
			t.Fatal("b was nil unexpectedly")
		}
		// it should have passed it
		calculatedLabel := b[60:71]
		expectedLabel := []byte{97, 98, 99, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20}
		if !bytes.Equal(calculatedLabel, expectedLabel) {
			t.Log(calculatedLabel)
			t.Log(expectedLabel)
			t.Fatal("did not fill short label properly")
		}
	})
	t.Run("long Volume Label", func(t *testing.T) {
		bpb := getValidDos71EBPB()
		bpb.volumeLabel = "abcdefghijklmnopqrst"
		b, err := bpb.toBytes()
		if err == nil {
			t.Error("error was nil unexpectedly")
		}
		if b != nil {
			t.Fatal("b was not nil")
		}
		expected := "invalid volume label: too long"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("non-ascii Volume Label", func(t *testing.T) {
		bpb := getValidDos71EBPB()
		bpb.volumeLabel = "\u0061\u6785"
		b, err := bpb.toBytes()
		if err == nil {
			t.Error("error was nil unexpectedly")
		}
		if b != nil {
			t.Fatal("b was not nil")
		}
		expected := "invalid volume label: non-ascii characters"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("short FileSystem Type", func(t *testing.T) {
		fstype := "fat"
		bpb := getValidDos71EBPB()
		bpb.fileSystemType = fstype
		b, err := bpb.toBytes()
		if err != nil {
			t.Errorf("error was not nil, instead %v", err)
		}
		if b == nil {
			t.Fatal("b was nil unexpectedly")
		}
		// it should have passed it
		calculatedType := b[71:79]
		expectedType := []byte{102, 97, 116, 0x20, 0x20, 0x20, 0x20, 0x20}
		if !bytes.Equal(calculatedType, expectedType) {
			t.Log(calculatedType)
			t.Log(expectedType)
			t.Fatal("did not fill short FileSystem Type properly")
		}
	})
	t.Run("long FileSystem Type", func(t *testing.T) {
		bpb := getValidDos71EBPB()
		bpb.fileSystemType = "abcdefghijklmnopqrst"
		b, err := bpb.toBytes()
		if err == nil {
			t.Error("error was nil unexpectedly")
		}
		if b != nil {
			t.Fatal("b was not nil")
		}
		expected := "invalid filesystem type: too long"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("non-ascii FileSystem Type", func(t *testing.T) {
		bpb := getValidDos71EBPB()
		bpb.fileSystemType = "\u0061\u6785"
		b, err := bpb.toBytes()
		if err == nil {
			t.Error("error was nil unexpectedly")
		}
		if b != nil {
			t.Fatal("b was not nil")
		}
		expected := "invalid filesystem type: non-ascii characters"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("valid EBPB", func(t *testing.T) {
		bpb := getValidDos71EBPB()
		b, err := bpb.toBytes()
		if err != nil {
			t.Errorf("error was not nil, instead %v", err)
		}
		if b == nil {
			t.Fatal("b was nil unexpectedly")
		}
		valid, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		validBytes := valid[11:90]
		if !bytes.Equal(validBytes, b) {
			t.Log(validBytes)
			t.Log(b)
			t.Error("Mismatched bytes")
		}
	})
}
