package ext4

import (
	"os"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

func TestOpenFileSpecialFileReturnsError(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	_, err = fs.OpenFile("/chardev", os.O_RDONLY)
	if err == nil {
		t.Fatal("OpenFile on character device should return an error, not succeed")
	}
	if !strings.Contains(err.Error(), "cannot open special file") {
		t.Errorf("expected 'cannot open special file' error, got: %v", err)
	}
}
