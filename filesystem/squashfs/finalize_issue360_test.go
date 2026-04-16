package squashfs_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
)

// TestFinalizeInodesAcrossMetadataBlocks demonstrates the root cause of
// https://github.com/diskfs/go-diskfs/issues/360.
//
// When compressed inode metadata spans multiple metadata blocks,
// updateInodeLocations (finalize.go) computes each inode's block byte offset
// as `logicalBlock * (metadataBlockSize + 2)` = `logicalBlock * 8194`.
// That assumes every compressed metadata block is exactly 8194 bytes on disk,
// which is only true when compression is effectively off. With real gzip
// compression the blocks are typically much smaller, so directory entries
// and the superblock root-inode ref point past the real block positions.
// Readers then fail with "could not read directory entries" or
// "failed to read inode X:Y" for every inode that isn't in block 0.
//
// Reproducer: enough small files (~600) to push inodes past the 8 KB
// metadata-block boundary, plus CompressionLevel: 9 so gzip actually
// reduces block size. (The default CompressorGzip{} uses level 0 = no
// compression, which is why existing tests don't catch this.)
//
// Expected current behaviour: this test FAILS. Every file lookup returns
// an error because even the root directory inode is past block 0.
// After the fix in finalize.go, this test passes.
func TestFinalizeInodesAcrossMetadataBlocks(t *testing.T) {
	f, err := os.CreateTemp("", "squashfs_issue360_*.sqs")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())

	b := file.New(f, false)
	fs, err := squashfs.Create(b, 0, 0, 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	const numFiles = 600
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("f%04d.txt", i)
		fh, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("OpenFile %s: %v", name, err)
		}
		if _, err := fmt.Fprintf(fh, "file %d content\n", i); err != nil {
			t.Fatalf("Write %s: %v", name, err)
		}
		fh.Close()
	}

	// CompressionLevel 9 guarantees gzip actually compresses — default 0 means
	// zlib.NoCompression and the bug doesn't reproduce.
	if err := fs.Finalize(squashfs.FinalizeOptions{
		Compression: &squashfs.CompressorGzip{CompressionLevel: 9},
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	fs2, err := squashfs.Read(b, 0, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	failed := 0
	firstFail := -1
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("f%04d.txt", i)
		fh, err := fs2.OpenFile(name, os.O_RDONLY)
		if err != nil {
			if firstFail < 0 {
				firstFail = i
				t.Logf("FIRST FAIL at i=%d: OpenFile: %v", i, err)
			}
			failed++
			continue
		}
		buf := make([]byte, 100)
		n, _ := fh.Read(buf)
		want := fmt.Sprintf("file %d content\n", i)
		if string(buf[:n]) != want {
			if firstFail < 0 {
				firstFail = i
				t.Logf("FIRST FAIL at i=%d: got %q want %q", i, buf[:n], want)
			}
			failed++
		}
		fh.Close()
	}
	if failed > 0 {
		t.Errorf("%d of %d files failed (first failure at index %d)", failed, numFiles, firstFail)
	}
}
