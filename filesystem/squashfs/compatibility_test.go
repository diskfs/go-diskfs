package squashfs

import (
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/internal/testutil"
)

func TestFSCompatibility(t *testing.T) {
	f, err := os.Open(Squashfsfile)
	if err != nil {
		t.Fatalf("Failed to read iso9660 testfile: %v", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Failed to stat iso9660 testfile: %v", err)
	}

	b := file.New(f, true)
	fs, err := Read(b, info.Size(), 0, 0)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}
	rootDirEntries := testGetFilesystemRoot()

	if _, err := fs.ReadDir("/"); err == nil {
		t.Fatalf("should have given error with ReadDir(/)")
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		t.Fatalf("should not have given error with ReadDir(.): %s", err)
	}
	expectedEntries := len(rootDirEntries) // rootDirEntries already excludes . and ..
	if len(entries) != expectedEntries {
		t.Fatalf("found %d root entries in fs instead of expected %d", len(entries), expectedEntries)
	}
	// take a random file from the root dir
	direntry := rootDirEntries[5]
	if _, err := fs.Open("/" + direntry.name); err == nil {
		t.Fatalf("should have given an error with Open(/%s)", direntry.name)
	}
	testfile, err := fs.Open(direntry.name)
	if err != nil {
		t.Fatalf("test file: %s", err)
	}
	if _, err := testfile.Stat(); err != nil {
		t.Fatalf("stat: %s", err)
	}
	testutil.TestFSTree(t, fs)
}
