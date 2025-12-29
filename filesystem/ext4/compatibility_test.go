package ext4

import (
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/internal/testutil"
)

func TestFSCompatibility(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Failed to read iso9660 testfile: %v", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Failed to stat iso9660 testfile: %v", err)
	}

	b := file.New(f, true)
	fs, err := Read(b, info.Size(), 0, 512)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}
	rootDirEntries, err := testDirEntriesFromDebugFS(rootDirFile)
	if err != nil {
		t.Fatalf("Error reading root directory entries from debugfs: %v", err)
	}

	if _, err := fs.ReadDir("/"); err == nil {
		t.Fatalf("should have given error with ReadDir(/): %s", err)
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		t.Fatalf("should not have given error with ReadDir(.): %s", err)
	}
	expectedEntries := len(rootDirEntries) - 2 // remove . and ..
	if len(entries) != expectedEntries {
		t.Fatalf("found %d root entries in fs instead of expected %d", len(entries), expectedEntries)
	}
	// take a random file from the root dir
	direntry := rootDirEntries[5]
	if _, err := fs.Open("/" + direntry.filename); err == nil {
		t.Fatalf("should have given an error with Open(/%s)", direntry.filename)
	}
	testfile, err := fs.Open(direntry.filename)
	if err != nil {
		t.Fatalf("test file: %s", err)
	}
	if _, err := testfile.Stat(); err != nil {
		t.Fatalf("stat: %s", err)
	}
	testutil.TestFSTree(t, fs)
}
