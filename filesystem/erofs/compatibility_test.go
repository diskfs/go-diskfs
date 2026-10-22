package erofs

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/diskfs/go-diskfs/backend/file"
)

const (
	uncompressedFixture = "uncompressed.erofs"
	compressedFixture   = "compressed.erofs"
)

func testPayload() []byte {
	payload := make([]byte, 96*1024)
	seed := uint32(0x45524f46)
	for offset := 0; offset < len(payload); offset += 4 {
		seed = seed*1664525 + 1013904223
		binary.LittleEndian.PutUint32(payload[offset:offset+4], seed)
	}
	return payload
}

func readTestImage(t *testing.T, name string) *FileSystem {
	t.Helper()
	path := filepath.Join("testdata", name)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open(%q) error = %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("f.Stat() error = %v", err)
	}
	filesystem, err := Read(file.New(f, true), info.Size(), 0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	return filesystem
}

func TestReadUncompressedFixture(t *testing.T) {
	filesystem := readTestImage(t, uncompressedFixture)
	entries, err := filesystem.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("ReadDir() returned %d entries, want 5", len(entries))
	}
	if target, err := filesystem.ReadFile("link"); err != nil || string(target) != "hello.txt" {
		t.Fatalf("ReadFile(link) = %q, %v; want hello.txt, <nil>", target, err)
	}
	data, err := filesystem.ReadFile("sub/data.bin")
	if err != nil {
		t.Fatalf("ReadFile(sub/data.bin) error = %v", err)
	}
	expected := testPayload()
	expectedHash := sha256.Sum256(expected)
	dataHash := sha256.Sum256(data)
	if dataHash != expectedHash {
		t.Fatalf("ReadFile(sub/data.bin) SHA-256 = %x, want %x", dataHash, expectedHash)
	}
}

func TestReadCompressedFixture(t *testing.T) {
	filesystem := readTestImage(t, compressedFixture)
	data, err := filesystem.ReadFile("sub/data.bin")
	if err != nil {
		t.Fatalf("ReadFile(sub/data.bin) error = %v", err)
	}
	expected := testPayload()
	if len(data) != len(expected) {
		t.Fatalf("ReadFile(sub/data.bin) length = %d, want %d", len(data), len(expected))
	}
	if !bytes.Equal(data, expected) {
		t.Fatal("ReadFile(sub/data.bin) does not match the source payload")
	}
}

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
