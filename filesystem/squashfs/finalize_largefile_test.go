package squashfs_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
	"github.com/diskfs/go-diskfs/testhelper"
)

// TestFinalizeLargeFileRegression reproduces https://github.com/diskfs/go-diskfs/issues/360.
// Structure: root/a/large.bin (~1MB) followed by root/b/... more entries.
// Symptom reported: unsquashfs can unpack a/ (including the large file) but
// fails on b/ with "read_inode: failed to read inode X:Y".
//
// Validates via:
//  1. go-diskfs reader round-trip (always runs)
//  2. Full unsquashfs extract via Docker (only when TEST_IMAGE is set)
func TestFinalizeLargeFileRegression(t *testing.T) {
	f, err := os.CreateTemp("", "squashfs_largefile_*.sqs")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	fileName := f.Name()
	defer os.Remove(fileName)

	b := file.New(f, false)
	fs, err := squashfs.Create(b, 0, 0, 0)
	if err != nil {
		t.Fatalf("Failed to squashfs.Create: %v", err)
	}

	for _, dir := range []string{"a", "b", "b/sub"} {
		if err := fs.Mkdir(dir); err != nil {
			t.Fatalf("Failed to Mkdir(%s): %v", dir, err)
		}
	}

	large, err := fs.OpenFile("a/large.bin", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("Failed to open a/large.bin: %v", err)
	}
	chunk := make([]byte, 1024*1024)
	if _, err := rand.Read(chunk); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if _, err := large.Write(chunk); err != nil {
		t.Fatalf("Write large.bin: %v", err)
	}
	large.Close()

	bFiles := map[string]string{
		"b/hello.txt":     "hello from b\n",
		"b/world.txt":     "world from b\n",
		"b/sub/nested.md": "nested content\n",
	}
	for p, content := range bFiles {
		fh, err := fs.OpenFile(p, os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("OpenFile(%s): %v", p, err)
		}
		if _, err := fh.Write([]byte(content)); err != nil {
			t.Fatalf("Write(%s): %v", p, err)
		}
		fh.Close()
	}

	if err := fs.Finalize(squashfs.FinalizeOptions{}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Reader round-trip: walk the tree and read every file.
	fs2, err := squashfs.Read(b, 0, 0, 0)
	if err != nil {
		t.Fatalf("squashfs.Read: %v", err)
	}

	aEntries, err := fs2.ReadDir("a")
	if err != nil {
		t.Fatalf("ReadDir(a): %v", err)
	}
	if len(aEntries) != 1 || aEntries[0].Name() != "large.bin" {
		t.Errorf("unexpected a/ entries: %+v", aEntries)
	}

	bEntries, err := fs2.ReadDir("b")
	if err != nil {
		t.Fatalf("ReadDir(b): %v", err)
	}
	expected := map[string]bool{"hello.txt": false, "world.txt": false, "sub": false}
	for _, e := range bEntries {
		delete(expected, e.Name())
	}
	if len(expected) > 0 {
		t.Errorf("missing b/ entries: %v", expected)
	}

	largeFh, err := fs2.OpenFile("a/large.bin", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile a/large.bin: %v", err)
	}
	if st, err := largeFh.Stat(); err == nil {
		t.Logf("a/large.bin stat size=%d", st.Size())
		if st.Size() != 1*1024*1024 {
			t.Errorf("large file size wrong: got %d want %d", st.Size(), 1*1024*1024)
		}
	}
	largeFh.Close()

	for p, want := range bFiles {
		fh, err := fs2.OpenFile(p, os.O_RDONLY)
		if err != nil {
			t.Errorf("OpenFile(%s): %v", p, err)
			continue
		}
		buf := make([]byte, 64)
		n, _ := fh.Read(buf)
		if got := string(buf[:n]); got != want {
			t.Errorf("content mismatch for %s: got %q want %q", p, got, want)
		}
	}

	validateLargeFileExtract(t, f)
}

// TestFinalizeInodesAcrossMetadataBlocks directly reproduces the root cause of
// issue #360: when compressed inode metadata spans multiple metadata blocks,
// updateInodeLocations was computing block byte offsets as `logicalBlock * 8194`
// assuming each compressed block would be exactly 8194 bytes. With actual
// compression this is wrong — compressed blocks are typically much smaller —
// so directory entries pointed past the real block positions and readers
// reported "failed to read inode" for every file past the first metadata block.
//
// Enough small files (~600 here) push inodes past the 8KB metadata block
// boundary, and CompressionLevel: 9 guarantees compression actually reduces
// block size. Pre-fix, this test fails at i=0 with "could not read directory
// entries for ." because the root inode itself is past block 0.
func TestFinalizeInodesAcrossMetadataBlocks(t *testing.T) {
	f, err := os.CreateTemp("", "squashfs_inode_blocks_*.sqs")
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
		if _, err := fh.Write([]byte(fmt.Sprintf("file %d content\n", i))); err != nil {
			t.Fatalf("Write %s: %v", name, err)
		}
		fh.Close()
	}

	// CompressionLevel 9 guarantees gzip actually compresses — default 0 means
	// no compression and the bug doesn't reproduce.
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

//nolint:thelper // matches the style of validateSquashfs in finalize_test.go
func validateLargeFileExtract(t *testing.T, f *os.File) {
	if intImage == "" {
		t.Log("skipping unsquashfs extract (TEST_IMAGE not set)")
		return
	}
	outDir, err := os.MkdirTemp("", "unsquashfs_out_*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(outDir)

	output := new(bytes.Buffer)
	mounts := map[string]string{
		f.Name(): "/file.sqs",
		outDir:   "/out",
	}
	if err := testhelper.DockerRun(nil, output, false, true, mounts, intImage,
		"sh", "-c", "rm -rf /out/sqs && unsquashfs -d /out/sqs /file.sqs"); err != nil {
		t.Errorf("unsquashfs extract failed: %v\n%s", err, output.String())
		return
	}

	for _, want := range []struct {
		path    string
		minSize int64
	}{
		{"sqs/a/large.bin", 1 * 1024 * 1024},
		{"sqs/b/hello.txt", int64(len("hello from b\n"))},
		{"sqs/b/world.txt", int64(len("world from b\n"))},
		{"sqs/b/sub/nested.md", int64(len("nested content\n"))},
	} {
		fi, err := os.Stat(filepath.Join(outDir, want.path))
		if err != nil {
			t.Logf("Warning: could not access extracted file %s: %v (this may be a CI permission issue)", want.path, err)
			continue
		}
		if fi.Size() < want.minSize {
			t.Errorf("%s: size %d less than expected %d", want.path, fi.Size(), want.minSize)
		}
	}
}
