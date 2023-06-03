package converter

import (
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/filesystem/iso9660"
)

func TestFat32(t *testing.T) {
	f, err := os.Open("../filesystem/iso9660/testdata/9660.iso")
	if err != nil {
		t.Fatalf("Failed to read iso9660 testfile: %v", err)
	}
	isofs, err := iso9660.Read(f, 0, 0, 2048)
	if err != nil {
		t.Fatalf("iso read: %s", err)
	}

	fs := FS(isofs)
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
