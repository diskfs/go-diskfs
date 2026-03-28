package iso9660

import (
	"io/fs"
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
	fsys, err := Read(b, 0, 0, 2048)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}

	if _, err := fsys.ReadDir("/"); err == nil {
		t.Fatalf("should have given error with ReadDir(/): %s", err)
	}
	entries, err := fsys.ReadDir(".")
	if err != nil {
		t.Fatalf("should not have given error with ReadDir(.): %s", err)
	}
	if len(entries) != 5 {
		t.Fatalf("should be 5 entries in iso fs")
	}
	if _, err := fsys.Open("/README.MD"); err == nil {
		t.Fatalf("should have given an error with Open(/README.MD)")
	}
	testfile, err := fsys.Open("README.MD")
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

	testutil.TestFSTree(t, fsys)
}

func TestOpenRootDirectory(t *testing.T) {
	f, err := os.Open(ISO9660File)
	if err != nil {
		t.Fatalf("Failed to read iso9660 testfile: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fsys, err := Read(b, 0, 0, 2048)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}

	// Open root directory
	root, err := fsys.Open(".")
	if err != nil {
		t.Fatalf("Open(\".\") should succeed: %v", err)
	}
	defer root.Close()

	// Should implement ReadDirFile
	rdf, ok := root.(fs.ReadDirFile)
	if !ok {
		t.Fatal("Open(\".\") should return a ReadDirFile")
	}

	// Stat should return "." as name
	info, err := root.Stat()
	if err != nil {
		t.Fatalf("Stat on root: %v", err)
	}
	if info.Name() != "." {
		t.Fatalf("root Stat().Name() = %q, want \".\"", info.Name())
	}
	if !info.IsDir() {
		t.Fatal("root Stat().IsDir() should be true")
	}

	// ReadDir should list entries
	dirEntries, err := rdf.ReadDir(-1)
	if err != nil {
		t.Fatalf("ReadDir(-1) on root: %v", err)
	}
	if len(dirEntries) != 5 {
		t.Fatalf("expected 5 root entries, got %d", len(dirEntries))
	}

	// Read should return ErrInvalid
	buf := make([]byte, 10)
	if _, err := root.Read(buf); err != fs.ErrInvalid {
		t.Fatalf("Read on directory should return fs.ErrInvalid, got %v", err)
	}
}

func TestOpenSubdirectory(t *testing.T) {
	f, err := os.Open(ISO9660File)
	if err != nil {
		t.Fatalf("Failed to read iso9660 testfile: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fsys, err := Read(b, 0, 0, 2048)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}

	// Open the FOO subdirectory
	dir, err := fsys.Open("FOO")
	if err != nil {
		t.Fatalf("Open(\"FOO\") should succeed: %v", err)
	}
	defer dir.Close()

	// Should implement ReadDirFile
	rdf, ok := dir.(fs.ReadDirFile)
	if !ok {
		t.Fatal("Open(\"FOO\") should return a ReadDirFile")
	}

	// Stat should return directory info
	info, err := dir.Stat()
	if err != nil {
		t.Fatalf("Stat on FOO: %v", err)
	}
	if info.Name() != "FOO" {
		t.Fatalf("FOO Stat().Name() = %q, want \"FOO\"", info.Name())
	}
	if !info.IsDir() {
		t.Fatal("FOO Stat().IsDir() should be true")
	}

	// ReadDir with cursor: read 3 at a time
	first, err := rdf.ReadDir(3)
	if err != nil {
		t.Fatalf("ReadDir(3) first call: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("expected 3 entries from first ReadDir(3), got %d", len(first))
	}

	// Continue reading
	rest, err := rdf.ReadDir(-1)
	if err != nil {
		t.Fatalf("ReadDir(-1) after partial read: %v", err)
	}
	// FOO directory has 76 files (FILENA00..FILENA75)
	totalEntries := len(first) + len(rest)
	if totalEntries != 76 {
		t.Fatalf("expected 76 total entries in FOO, got %d", totalEntries)
	}
}

func TestStatRootDirectory(t *testing.T) {
	f, err := os.Open(ISO9660File)
	if err != nil {
		t.Fatalf("Failed to read iso9660 testfile: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fsys, err := Read(b, 0, 0, 2048)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}

	info, err := fsys.Stat(".")
	if err != nil {
		t.Fatalf("Stat(\".\") should succeed: %v", err)
	}
	if info.Name() != "." {
		t.Fatalf("Stat(\".\").Name() = %q, want \".\"", info.Name())
	}
	if !info.IsDir() {
		t.Fatal("Stat(\".\").IsDir() should be true")
	}
}

func TestOpenDirectoryRockRidge(t *testing.T) {
	f, err := os.Open(RockRidgeFile)
	if err != nil {
		t.Fatalf("Failed to read rock ridge testfile: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fsys, err := Read(b, 0, 0, 2048)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}

	// Open root
	root, err := fsys.Open(".")
	if err != nil {
		t.Fatalf("Open(\".\") should succeed: %v", err)
	}
	defer root.Close()

	rdf, ok := root.(fs.ReadDirFile)
	if !ok {
		t.Fatal("Open(\".\") should return a ReadDirFile")
	}

	entries, err := rdf.ReadDir(-1)
	if err != nil {
		t.Fatalf("ReadDir(-1) on root: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one entry in rock ridge root")
	}

	// Find the "foo" subdirectory (Rock Ridge uses lowercase)
	found := false
	for _, e := range entries {
		if e.IsDir() && e.Name() == "foo" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected to find 'foo' subdirectory in rock ridge root")
	}

	// Open the foo subdirectory
	dir, err := fsys.Open("foo")
	if err != nil {
		t.Fatalf("Open(\"foo\") should succeed: %v", err)
	}
	defer dir.Close()

	rdf2, ok := dir.(fs.ReadDirFile)
	if !ok {
		t.Fatal("Open(\"foo\") should return a ReadDirFile")
	}
	dirEntries, err := rdf2.ReadDir(-1)
	if err != nil {
		t.Fatalf("ReadDir(-1) on foo: %v", err)
	}
	if len(dirEntries) == 0 {
		t.Fatal("expected entries in rock ridge foo directory")
	}
}
