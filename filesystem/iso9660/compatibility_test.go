package iso9660

import (
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/internal/testutil"
)

func TestFSCompatibility(t *testing.T) {
	f, err := os.Open(ISO9660File)
	if err != nil {
		t.Fatalf("Failed to read iso9660 testfile: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 0, 0, 2048)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}

	if _, err := fs.ReadDir("/"); err == nil {
		t.Fatalf("should have given error with ReadDir(/): %s", err)
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		t.Fatalf("should not have given error with ReadDir(.): %s", err)
	}
	if len(entries) != 5 {
		t.Fatalf("should be 5 entries in iso fs")
	}
	if _, err := fs.Open("/README.MD"); err == nil {
		t.Fatalf("should have given an error with Open(/README.MD)")
	}
	testfile, err := fs.Open("README.MD")
	if err != nil {
		t.Fatalf("test file: %s", err)
	}
	stat, err := testfile.Stat()
	if err != nil {
		t.Fatalf("stat: %s", err)
	}
	if stat.Size() != 7 {
		t.Fatalf("size bad: %d", stat.Size())
	}

	testutil.TestFSTree(t, fs)
}
