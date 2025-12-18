package fat32_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/stretchr/testify/require"
)

const (
	benchmarkFileSize = (4 * 1024 * 1024 * 1024) - 1 // 4GB
)

// createTestFile creates a temporary file with specified size using random data
func createTestFile(b *testing.B, size int64) string {
	b.Helper()

	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "source.dat")

	f, err := os.Create(path)
	require.NoError(b, err, "creating test file failed")

	defer f.Close()

	require.NoError(b, f.Truncate(size), "truncating test file failed")

	return path
}

// setupFAT32Disk creates a temporary FAT32 filesystem
func setupFAT32Disk(b *testing.B) (fs filesystem.FileSystem, cleanup func()) {
	b.Helper()

	tmpDir := b.TempDir()
	diskPath := filepath.Join(tmpDir, "test.img")

	// Create a 5GB disk to have plenty of space for 4GB file
	diskSize := int64(5 * 1024 * 1024 * 1024)

	bk, err := file.CreateFromPath(diskPath, diskSize)
	require.NoError(b, err, "creating backend failed")

	d, err := diskfs.OpenBackend(bk)
	require.NoError(b, err, "opening disk failed")

	spec := disk.FilesystemSpec{
		Partition: 0,
		FSType:    filesystem.TypeFat32,
	}

	fs, err = d.CreateFilesystem(spec)
	require.NoError(b, err, "creating filesystem failed")

	return fs, func() {
		require.NoError(b, fs.Close())
		require.NoError(b, d.Close())
	}
}

// BenchmarkFAT32WriteWithReadFile benchmarks writing using os.ReadFile + Write
func BenchmarkFAT32WriteWithReadFile(b *testing.B) {
	// Create source file once for all iterations
	sourceFile := createTestFile(b, benchmarkFileSize)

	for b.Loop() {
		// Create new filesystem for each iteration
		fs, cleanupFunc := setupFAT32Disk(b)

		// Read entire file into memory
		data, err := os.ReadFile(sourceFile)
		require.NoError(b, err, "reading source file failed")

		// Open destination file
		destFile, err := fs.OpenFile("/testfile.dat", os.O_CREATE|os.O_RDWR)
		require.NoError(b, err, "opening destination file failed")

		// Write all at once
		_, err = destFile.Write(data)
		require.NoError(b, err, "writing to destination file failed")
		require.NoError(b, destFile.Close())

		cleanupFunc()
	}
}

// BenchmarkFAT32WriteWithIOCopy benchmarks writing using io.Copy
func BenchmarkFAT32WriteWithIOCopy(b *testing.B) {
	// Create source file once for all iterations
	sourceFile := createTestFile(b, benchmarkFileSize)

	for b.Loop() {
		// Create new filesystem for each iteration
		fs, cleanupFunc := setupFAT32Disk(b)

		// Open source file
		srcFile, err := os.Open(sourceFile)
		require.NoError(b, err, "opening source file failed")

		// Open destination file
		destFile, err := fs.OpenFile("/testfile.dat", os.O_CREATE|os.O_RDWR)
		require.NoError(b, err, "opening destination file failed")

		_, err = io.Copy(destFile, srcFile)
		require.NoError(b, err, "copying to destination file failed")
		require.NoError(b, srcFile.Close())
		require.NoError(b, destFile.Close())

		cleanupFunc()
	}
}
