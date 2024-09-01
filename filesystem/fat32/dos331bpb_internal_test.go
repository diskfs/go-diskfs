package fat32

import (
	"bytes"
	"encoding/binary"
	"os"
	"strings"
	"testing"
)

func getValidDos331BPB() *dos331BPB {
	return &dos331BPB{
		dos20BPB:        getValidDos20BPB(),
		sectorsPerTrack: uint16(fsInfo.sectorsPerTrack),
		heads:           uint16(fsInfo.heads),
		hiddenSectors:   fsInfo.hiddenSectors,
		totalSectors:    0,
	}
}

func TestDos331BPBFromBytes(t *testing.T) {
	t.Run("mismatched length", func(t *testing.T) {
		b := make([]byte, 24, 25)
		bpb, err := dos331BPBFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if bpb != nil {
			t.Fatalf("returned bpb was non-nil")
		}
		expected := "cannot read DOS 3.31 BPB from invalid byte slice"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid Dos20BPB", func(t *testing.T) {
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
	t.Run("valid data", func(t *testing.T) {
		input, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		inputBytes := input[11:36]
		bpb, err := dos331BPBFromBytes(inputBytes)
		if err != nil {
			t.Errorf("returned unexpected non-nil error: %v", err)
		}
		if bpb == nil {
			t.Fatalf("returned bpb was nil")
		}
		valid := getValidDos331BPB()
		if !bpb.equal(valid) {
			t.Log(bpb)
			t.Log(valid)
			t.Fatalf("Mismatched BPB")
		}
	})
}

func TestDos331BPBToBytes(t *testing.T) {
	bpb := getValidDos331BPB()
	b := bpb.toBytes()
	if b == nil {
		t.Fatal("b was nil unexpectedly")
	}
	valid, err := os.ReadFile(Fat32File)
	if err != nil {
		t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
	}
	validBytes := valid[11:36]
	if !bytes.Equal(validBytes, b) {
		t.Log(validBytes)
		t.Log(b)
		t.Error("Mismatched bytes")
	}
}
