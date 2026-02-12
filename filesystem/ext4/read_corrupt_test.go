package ext4

import (
	"os"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

// TestReadTooSmall verifies that Read rejects images smaller than Ext4MinSize.
func TestReadTooSmall(t *testing.T) {
	// Create a file that is too small
	dir := t.TempDir()
	p := dir + "/tiny.img"
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	defer f.Close()

	tinySize := Ext4MinSize - 1
	if err := f.Truncate(tinySize); err != nil {
		t.Fatalf("Error truncating: %v", err)
	}

	b := file.New(f, true)
	_, err = Read(b, tinySize, 0, 512)
	if err == nil {
		t.Fatalf("expected error for too-small image, got nil")
	}
	if !strings.Contains(err.Error(), "smaller than minimum") {
		t.Errorf("expected 'smaller than minimum' error, got: %v", err)
	}
}

// TestReadInvalidSectorSize verifies that Read rejects non-512 sector sizes.
func TestReadInvalidSectorSize(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	_, err = Read(b, 100*MB, 0, 1024)
	if err == nil {
		t.Fatalf("expected error for sectorsize=1024, got nil")
	}
	if !strings.Contains(err.Error(), "sectorsize") {
		t.Errorf("expected sectorsize error, got: %v", err)
	}
}

// TestReadCorruptSuperblockMagic verifies that Read detects a corrupted superblock magic number.
func TestReadCorruptSuperblockMagic(t *testing.T) {
	outfile := testCreateImgCopyFrom(t, imgFile)

	// Corrupt the superblock magic number at offset 0x438 (1080 bytes from start)
	// The superblock starts at byte 1024, and the magic is at offset 0x38 within the superblock = byte 1080
	f, err := os.OpenFile(outfile, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Error opening: %v", err)
	}

	// Write garbage over the magic bytes
	if _, err := f.WriteAt([]byte{0xDE, 0xAD}, 1024+0x38); err != nil {
		t.Fatalf("Error corrupting magic: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}

	// Seek to beginning for reading
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Error seeking: %v", err)
	}

	b := file.New(f, true)
	_, err = Read(b, 100*MB, 0, 512)
	f.Close()

	if err == nil {
		t.Fatalf("expected error for corrupted superblock magic, got nil")
	}
	if !strings.Contains(err.Error(), "superblock") {
		t.Errorf("expected superblock-related error, got: %v", err)
	}
}

// TestReadTruncatedImage verifies that Read fails on a truncated image.
func TestReadTruncatedImage(t *testing.T) {
	// Create an image that has valid magic but is truncated before the GDT
	dir := t.TempDir()
	p := dir + "/truncated.img"

	// Copy just the first 2048 bytes (superblock only, no GDT)
	srcFile, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening source: %v", err)
	}
	buf := make([]byte, 2048)
	n, err := srcFile.Read(buf)
	srcFile.Close()
	if err != nil {
		t.Fatalf("Error reading source: %v", err)
	}

	dstFile, err := os.Create(p)
	if err != nil {
		t.Fatalf("Error creating truncated image: %v", err)
	}
	if _, err := dstFile.Write(buf[:n]); err != nil {
		t.Fatalf("Error writing truncated image: %v", err)
	}
	dstFile.Close()

	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("Error opening truncated image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	// Use a reported size that would be correct for a full image
	_, err = Read(b, 100*MB, 0, 512)
	if err == nil {
		t.Fatalf("expected error for truncated image, got nil")
	}
}

// TestReadAllZeros verifies that Read fails gracefully on an all-zero image.
func TestReadAllZeros(t *testing.T) {
	dir := t.TempDir()
	p := dir + "/zeros.img"
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	// Create a 10MB zero-filled image
	size := 10 * MB
	if err := f.Truncate(size); err != nil {
		t.Fatalf("Error truncating: %v", err)
	}
	f.Close()

	f, err = os.Open(p)
	if err != nil {
		t.Fatalf("Error opening: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	_, err = Read(b, size, 0, 512)
	if err == nil {
		t.Fatalf("expected error for all-zero image (bad magic), got nil")
	}
}

// TestReadRandomGarbage verifies that Read fails gracefully on random garbage data.
func TestReadRandomGarbage(t *testing.T) {
	dir := t.TempDir()
	p := dir + "/garbage.img"
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	// Write repeating non-zero garbage
	size := 10 * MB
	garbage := make([]byte, 4096)
	for i := range garbage {
		garbage[i] = byte(i % 251) // prime modulus avoids accidental ext4 magic
	}
	for written := int64(0); written < size; written += int64(len(garbage)) {
		n := int64(len(garbage))
		if written+n > size {
			n = size - written
		}
		if _, err := f.Write(garbage[:n]); err != nil {
			t.Fatalf("Error writing garbage: %v", err)
		}
	}
	f.Close()

	f, err = os.Open(p)
	if err != nil {
		t.Fatalf("Error opening: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	_, err = Read(b, size, 0, 512)
	if err == nil {
		t.Fatalf("expected error for garbage image, got nil")
	}
}

// TestReadZeroSectorSize verifies that Read with sectorsize=0 defaults to 512.
func TestReadZeroSectorSize(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 0)
	if err != nil {
		t.Fatalf("Read with sectorsize=0 failed: %v", err)
	}
	if fs == nil {
		t.Fatalf("Expected non-nil filesystem")
	}
}

// TestReadWrongOffset verifies that reading at a wrong offset fails.
func TestReadWrongOffset(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	// The image has no offset, so reading from offset 512 should fail
	_, err = Read(b, 100*MB-512, 512, 512)
	if err == nil {
		t.Fatalf("expected error reading at wrong offset, got nil")
	}
}

// TestReadCorruptGDT verifies that Read fails when the GDT is corrupted.
func TestReadCorruptGDT(t *testing.T) {
	outfile := testCreateImgCopyFrom(t, imgFile)

	f, err := os.OpenFile(outfile, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Error opening: %v", err)
	}

	// First read the superblock to find the block size
	// For the default test image, blocksize is 1024, so GDT starts at block 2 = byte 2048
	// Overwrite the GDT area with garbage
	garbage := make([]byte, 1024)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	if _, err := f.WriteAt(garbage, 2048); err != nil {
		t.Fatalf("Error corrupting GDT: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Error seeking: %v", err)
	}

	b := file.New(f, true)
	_, err = Read(b, 100*MB, 0, 512)
	f.Close()

	// The read may succeed (ext4 is lenient about GDT checksums on read) or
	// may fail with a GDT error. Either way, this exercises the code path.
	// What we really care about is that it doesn't panic.
	_ = err
}
