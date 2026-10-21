package erofs

import (
	"crypto/sha256"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

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

func TestReadCompressedMkfsErofsImage(t *testing.T) {
	mkfs, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skip("mkfs.erofs is not available")
	}
	directory := t.TempDir()
	if err := os.MkdirAll(filepath.Join(directory, "test", "path"), 0o755); err != nil {
		t.Fatalf("os.MkdirAll() error = %v", err)
	}
	payload := make([]byte, 5*1024*1024)
	seed := uint32(0x45524f46)
	for offset := 0; offset < len(payload); offset += 4 {
		seed = seed*1664525 + 1013904223
		binary.LittleEndian.PutUint32(payload[offset:offset+4], seed)
	}
	copy(payload[len(payload)-48*1024:], make([]byte, 48*1024))
	sourcePath := filepath.Join(directory, "test", "path", "file")
	if err := os.WriteFile(sourcePath, payload, 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	mtime := time.Unix(1577934245, 0).UTC()
	if err := os.Chtimes(sourcePath, mtime, mtime); err != nil {
		t.Fatalf("os.Chtimes() error = %v", err)
	}
	imagePath := filepath.Join(directory, "root.erofs")
	command := exec.Command(mkfs, "-zlzma", "-C", "1048576", "-E", "fragments", imagePath, directory)
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
	data, err := filesystem.ReadFile("test/path/file")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	sourceHash := sha256.Sum256(payload)
	readHash := sha256.Sum256(data)
	if sourceHash != readHash {
		t.Fatalf("ReadFile() SHA-256 = %x, want %x", readHash, sourceHash)
	}
}
