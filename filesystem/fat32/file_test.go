package fat32_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat32"
)

//nolint:unused,revive // keep for future when we implement it and will need t
func TestFileRead(t *testing.T) {

}

//nolint:unused,revive // keep for future when we implement it and will need t
func TestFileWrite(t *testing.T) {

}

func TestFileSizeLimitWrite(t *testing.T) {
	tmpDir := t.TempDir()
	diskPath := filepath.Join(tmpDir, "test.img")

	// Create a 5GB disk
	diskSize := int64(5 * 1024 * 1024 * 1024)

	bk, err := file.CreateFromPath(diskPath, diskSize)
	if err != nil {
		t.Fatalf("creating backend failed: %v", err)
	}

	d, err := diskfs.OpenBackend(bk)
	if err != nil {
		t.Fatalf("opening disk failed: %v", err)
	}
	defer d.Close()

	spec := disk.FilesystemSpec{
		Partition: 0,
		FSType:    filesystem.TypeFat32,
	}

	fs, err := d.CreateFilesystem(spec)
	if err != nil {
		t.Fatalf("creating filesystem failed: %v", err)
	}
	defer fs.Close()

	// Open a file for writing
	f, err := fs.OpenFile("/testfile.dat", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("opening file failed: %v", err)
	}
	defer f.Close()

	// Try to write just under the 4GB limit - should succeed
	const maxSize = (1 << 32) - 1
	smallData := []byte("test")
	_, err = f.Write(smallData)
	if err != nil {
		t.Fatalf("writing small data failed: %v", err)
	}

	// Seek to just before the 4GB boundary
	_, err = f.Seek(maxSize-100, io.SeekStart)
	if err != nil {
		t.Fatalf("seeking failed: %v", err)
	}

	// Write data that would fit within the limit
	_, err = f.Write([]byte("within limit"))
	if err != nil {
		t.Fatalf("writing within limit failed: %v", err)
	}

	// Try to write data that exceeds the 4GB limit
	_, err = f.Seek(maxSize-10, io.SeekStart)
	if err != nil {
		t.Fatalf("seeking failed: %v", err)
	}

	bigData := make([]byte, 100)
	_, err = f.Write(bigData)
	if !errors.Is(err, fat32.ErrFileTooLarge) {
		t.Errorf("expected ErrFileTooLarge, got: %v", err)
	}
}

func TestFileSizeLimitReadFrom(t *testing.T) {
	tmpDir := t.TempDir()
	diskPath := filepath.Join(tmpDir, "test.img")

	// Create a 5GB disk
	diskSize := int64(5 * 1024 * 1024 * 1024)

	bk, err := file.CreateFromPath(diskPath, diskSize)
	if err != nil {
		t.Fatalf("creating backend failed: %v", err)
	}

	d, err := diskfs.OpenBackend(bk)
	if err != nil {
		t.Fatalf("opening disk failed: %v", err)
	}
	defer d.Close()

	spec := disk.FilesystemSpec{
		Partition: 0,
		FSType:    filesystem.TypeFat32,
	}

	fs, err := d.CreateFilesystem(spec)
	if err != nil {
		t.Fatalf("creating filesystem failed: %v", err)
	}
	defer fs.Close()

	// Open a file for writing
	f, err := fs.OpenFile("/testfile.dat", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("opening file failed: %v", err)
	}
	defer f.Close()

	// Try to copy data that would exceed 4GB using io.Copy (which uses ReadFrom)
	const maxSize = (1 << 32) - 1
	const dataSize = maxSize + 1000 // Slightly over 4GB

	// Create an infinite zero reader limited to dataSize bytes
	reader := io.LimitReader(zeroReader{}, dataSize)

	// This should fail with ErrFileTooLarge
	_, err = io.Copy(f, reader)
	if !errors.Is(err, fat32.ErrFileTooLarge) {
		t.Errorf("expected ErrFileTooLarge, got: %v", err)
	}
}

// zeroReader is a reader that always returns zeros
type zeroReader struct{}

func (zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
