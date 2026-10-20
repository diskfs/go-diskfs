package fat32_test

import (
	"bytes"
	"io"
	"os"
	"path"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat32"
)

const lazyTestImageSize = 64 * 1024 * 1024

// lazyTestFiles are written into the image: long names, nested directories,
// and sizes on both sides of a 4 KiB cluster boundary.
var lazyTestFiles = map[string]int{
	"/empty.bin":    0,
	"/one-byte.bin": 1,
	"/dir/sub/a-long-file-name-that-needs-lfn-entries-a.bin": 4095,
	"/dir/sub/a-long-file-name-that-needs-lfn-entries-b.bin": 4096,
	"/dir/sub/a-long-file-name-that-needs-lfn-entries-c.bin": 4097,
	"/dir/other/medium.bin":                                  22000,
	"/large.bin":                                             100000,
}

// lazyTestContent is the content written for a file: deterministic per path.
func lazyTestContent(p string, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(len(p) + i*7)
	}
	return b
}

// writeFile writes content to p through a writable filesystem.
func writeFile(t *testing.T, fs *fat32.FileSystem, p string, content []byte) {
	t.Helper()
	if dir := path.Dir(p); dir != "/" {
		if err := fs.Mkdir(dir); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	f, err := fs.OpenFile(p, os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("create %s: %v", p, err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", p, err)
	}
}

// lazyTestImage creates a FAT32 image holding lazyTestFiles and returns its path.
func lazyTestImage(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "lazy-fat-*.img")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(lazyTestImageSize); err != nil {
		t.Fatal(err)
	}
	fs, err := fat32.Create(file.New(f, false), lazyTestImageSize, 0, 512, "LAZYFAT", false)
	if err != nil {
		t.Fatalf("create filesystem: %v", err)
	}
	for p, size := range lazyTestFiles {
		writeFile(t, fs, p, lazyTestContent(p, size))
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

// openImage opens the image file read-only or writable.
func openImage(t *testing.T, name string, readOnly bool) *os.File {
	t.Helper()
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	f, err := os.OpenFile(name, flag, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// readTree returns every file under dir with its content, keyed by its path
// from the root with a leading slash. ReadDir takes io/fs paths, where the
// root is "." and nothing starts with a slash.
func readTree(t *testing.T, fs filesystem.FileSystem, dir string, into map[string][]byte) {
	t.Helper()
	entries, err := fs.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir %s: %v", dir, err)
	}
	for _, entry := range entries {
		p := path.Join(dir, entry.Name())
		if entry.IsDir() {
			readTree(t, fs, p, into)
			continue
		}
		p = "/" + p
		f, err := fs.OpenFile(p, os.O_RDONLY)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		content, err := io.ReadAll(f)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		into[p] = content
	}
}

// TestLazyFATReadsWhatTheFullTableReads: the same image read with the whole
// table in memory and with entries read on demand lists the same files with
// the same content, and that content is what was written.
func TestLazyFATReadsWhatTheFullTableReads(t *testing.T) {
	name := lazyTestImage(t)

	full, err := fat32.Read(file.New(openImage(t, name, true), true), lazyTestImageSize, 0, 512)
	if err != nil {
		t.Fatalf("read with the full table: %v", err)
	}
	lazy, err := fat32.ReadWithOptions(file.New(openImage(t, name, true), true), lazyTestImageSize, 0, 512, fat32.WithLazyFAT())
	if err != nil {
		t.Fatalf("read with a lazy table: %v", err)
	}

	fullTree := map[string][]byte{}
	readTree(t, full, ".", fullTree)
	lazyTree := map[string][]byte{}
	readTree(t, lazy, ".", lazyTree)

	if len(lazyTree) != len(lazyTestFiles) {
		t.Fatalf("lazy table lists %d files, wrote %d", len(lazyTree), len(lazyTestFiles))
	}
	for p, size := range lazyTestFiles {
		want := lazyTestContent(p, size)
		if !bytes.Equal(fullTree[p], want) {
			t.Errorf("%s through the full table: %d bytes, not the %d written", p, len(fullTree[p]), size)
		}
		if !bytes.Equal(lazyTree[p], want) {
			t.Errorf("%s through the lazy table: %d bytes, not the %d written", p, len(lazyTree[p]), size)
		}
	}
	if fullLabel, lazyLabel := full.Label(), lazy.Label(); fullLabel != lazyLabel {
		t.Errorf("label %q through the lazy table, %q through the full table", lazyLabel, fullLabel)
	}
}

// TestLazyFATFindsAFileWrittenAfterItWasRead: another writer adds a file after
// the filesystem was read. The full table read before the write does not know
// the new file's clusters; the lazy table reads them as they are now.
func TestLazyFATFindsAFileWrittenAfterItWasRead(t *testing.T) {
	name := lazyTestImage(t)

	full, err := fat32.Read(file.New(openImage(t, name, true), true), lazyTestImageSize, 0, 512)
	if err != nil {
		t.Fatalf("read with the full table: %v", err)
	}
	lazy, err := fat32.ReadWithOptions(file.New(openImage(t, name, true), true), lazyTestImageSize, 0, 512, fat32.WithLazyFAT())
	if err != nil {
		t.Fatalf("read with a lazy table: %v", err)
	}

	writer, err := fat32.Read(file.New(openImage(t, name, false), false), lazyTestImageSize, 0, 512)
	if err != nil {
		t.Fatalf("read for writing: %v", err)
	}
	const late = "/dir/later/written-after-read.bin"
	content := lazyTestContent(late, 9000)
	writeFile(t, writer, late, content)

	f, err := lazy.OpenFile(late, os.O_RDONLY)
	if err != nil {
		t.Fatalf("lazy table: open %s: %v", late, err)
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("lazy table: read %s: %v", late, err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("lazy table: %s read %d bytes, not the %d written", late, len(got), len(content))
	}

	if f, err := full.OpenFile(late, os.O_RDONLY); err == nil {
		if got, err := io.ReadAll(f); err == nil && bytes.Equal(got, content) {
			t.Errorf("the full table read before the write found %s; this test no longer shows the difference", late)
		}
	}
}

// TestLazyFATRefusesAWritableBackend: the lazy table cannot be written, so it
// is only built over a backend that cannot be written either.
func TestLazyFATRefusesAWritableBackend(t *testing.T) {
	name := lazyTestImage(t)

	_, err := fat32.ReadWithOptions(file.New(openImage(t, name, false), false), lazyTestImageSize, 0, 512, fat32.WithLazyFAT())
	if err == nil {
		t.Fatal("a lazy table was built over a writable backend")
	}
}
