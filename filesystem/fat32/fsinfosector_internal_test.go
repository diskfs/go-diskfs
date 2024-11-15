package fat32

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

func getValidFSInfoSector() *FSInformationSector {
	return &FSInformationSector{
		freeDataClustersCount: fsInfo.freeSectorCount,
		lastAllocatedCluster:  fsInfo.nextFreeSector,
	}
}

func TestFsInformationSectorFromBytes(t *testing.T) {
	t.Run("mismatched length less than 512", func(t *testing.T) {
		b := make([]byte, 511, 512)
		fsis, err := fsInformationSectorFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if fsis != nil {
			t.Fatalf("returned FSInformationSector was non-nil")
		}
		expected := fmt.Sprintf("cannot read FAT32 FS Information Sector from %d bytes", len(b))
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("mismatched length greater than 512", func(t *testing.T) {
		b := make([]byte, 513)
		fsis, err := fsInformationSectorFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if fsis != nil {
			t.Fatalf("returned FSInformationSector was non-nil")
		}
		expected := fmt.Sprintf("cannot read FAT32 FS Information Sector from %d bytes", len(b))
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid start signature", func(t *testing.T) {
		input, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		b := input[512:1024]
		// now to pervert one key byte
		b[0] = 0xff
		fsis, err := fsInformationSectorFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if fsis != nil {
			t.Fatalf("returned FSInformationSector was non-nil")
		}
		expected := "invalid signature at beginning of FAT 32 Filesystem Information Sector"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid middle signature", func(t *testing.T) {
		input, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		b := input[512:1024]
		// now to pervert one key byte
		b[484] = 0xff
		fsis, err := fsInformationSectorFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if fsis != nil {
			t.Fatalf("returned FSInformationSector was non-nil")
		}
		expected := "invalid signature at middle of FAT 32 Filesystem Information Sector"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid end signature", func(t *testing.T) {
		input, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		b := input[512:1024]
		// now to pervert one key byte
		b[510] = 0xff
		fsis, err := fsInformationSectorFromBytes(b)
		if err == nil {
			t.Errorf("Did not return expected error")
		}
		if fsis != nil {
			t.Fatalf("returned FSInformationSector was non-nil")
		}
		expected := "invalid signature at end of FAT 32 Filesystem Information Sector"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("valid FS Information Sector", func(t *testing.T) {
		input, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		b := input[512:1024]
		fsis, err := fsInformationSectorFromBytes(b)
		if err != nil {
			t.Errorf("return unexpected error: %v", err)
		}
		if fsis == nil {
			t.Fatalf("returned FSInformationSector was nil unexpectedly")
		}
		valid := getValidFSInfoSector()
		if *valid != *fsis {
			t.Log(fsis)
			t.Log(valid)
			t.Fatalf("Mismatched FSInformationSector")
		}
	})
}

func TestInformationSectorToBytes(t *testing.T) {
	t.Run("valid FSInformationSector", func(t *testing.T) {
		fsis := getValidFSInfoSector()
		b := fsis.toBytes()
		if b == nil {
			t.Fatal("b was nil unexpectedly")
		}
		valid, err := os.ReadFile(Fat32File)
		if err != nil {
			t.Fatalf("error reading test fixture data from %s: %v", Fat32File, err)
		}
		validBytes := valid[512:1024]
		if !bytes.Equal(validBytes, b) {
			t.Error("Mismatched bytes")
		}
	})
}
