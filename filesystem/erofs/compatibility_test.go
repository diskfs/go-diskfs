package erofs

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

func TestReadMkfsErofsImage(t *testing.T) {
	mkfs, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skip("mkfs.erofs is not available")
	}
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	if err := os.Mkdir(source, 0o755); err != nil {
		t.Fatalf("os.Mkdir() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "hello.txt"), []byte("hello world\n"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	imagePath := filepath.Join(directory, "image.erofs")
	command := exec.Command(mkfs, "-b4096", "-x-1", "-T1700000000", imagePath, source)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("mkfs.erofs failed: %v: %s", err, output)
	}
	f, err := os.Open(imagePath)
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("f.Stat() error = %v", err)
	}
	filesystem, err := Read(file.New(f, true), info.Size(), 0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	data, err := filesystem.ReadFile("hello.txt")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "hello world\n" {
		t.Fatalf("ReadFile() = %q", data)
	}
}
