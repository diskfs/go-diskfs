package iso9660

import (
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/filesystem"
)

func TestISO9660FSCompatibility(t *testing.T) {
	f, err := os.Open(ISO9660File)
	if err != nil {
		t.Fatalf("Failed to read iso9660 testfile: %v", err)
	}
	defer f.Close()
	isofs, err := Read(f, 0, 0, 2048)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}

	fs := filesystem.FS(isofs)
	entries, err := fs.ReadDir("/")
	if err != nil {
		t.Fatalf("cannot read /: %s", err)
	}
	if len(entries) != 5 {
		t.Fatalf("should be 5 entries in iso fs")
	}
	testfile, err := fs.Open("/README.MD")
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
}
