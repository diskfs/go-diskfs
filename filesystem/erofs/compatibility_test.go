package erofs

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/diskfs/go-diskfs/backend/file"
)

const (
	uncompressedFixture = "uncompressed.erofs"
	compressedFixture   = "compressed.erofs"
	fullFixture         = "full.erofs"
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

func readFixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	image, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", name, err)
	}
	return image
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

func fullFixturePayload() []byte {
	pattern := []byte("EROFS deterministic fixture payload\n")
	payload := bytes.Repeat(pattern, 2622)
	return append(payload, make([]byte, 96*1024-len(payload))...)
}

func updateSuperblockChecksum(image []byte) {
	binary.LittleEndian.PutUint32(image[superOffset+4:superOffset+8], superblockChecksum(image[superOffset:4096]))
}

func TestReadFullCompressedFixture(t *testing.T) {
	filesystem := readTestImage(t, fullFixture)
	data, err := filesystem.ReadFile("data.bin")
	if err != nil {
		t.Fatalf("ReadFile(data.bin) error = %v", err)
	}
	if !bytes.Equal(data, fullFixturePayload()) {
		t.Fatal("ReadFile(data.bin) does not match the source payload")
	}
}

func compressedMetadata(t *testing.T, filesystem *FileSystem, path string) (node *inode, headerOffset int64) {
	t.Helper()
	node, err := filesystem.stat(path)
	if err != nil {
		t.Fatalf("stat(%q) error = %v", path, err)
	}
	inodeOffset := int64(filesystem.superblock.metaBlockAddress)*filesystem.blockSize() + int64(node.nid)*inodeSlotSize
	headerOffset = roundUp64(inodeOffset+int64(node.inodeSize+node.xattrSize), 8)
	return node, headerOffset
}

func TestCompressedFixtureCorruption(t *testing.T) {
	filesystem := readTestImage(t, compressedFixture)
	_, headerOffset := compressedMetadata(t, filesystem, "sub/data.bin")
	indexOffset := headerOffset + 8

	tests := []struct {
		name     string
		mutate   func([]byte)
		expected string
	}{
		{
			name: "invalid compact head type",
			mutate: func(image []byte) {
				binary.LittleEndian.PutUint16(image[indexOffset:], 0x3000)
			},
			expected: "unsupported compression algorithm 0",
		},
		{
			name: "extent outside image",
			mutate: func(image []byte) {
				binary.LittleEndian.PutUint32(image[indexOffset+4:], math.MaxUint32)
			},
			expected: "outside the image",
		},
		{
			name: "fragment outside packed inode",
			mutate: func(image []byte) {
				binary.LittleEndian.PutUint32(image[headerOffset:], math.MaxUint32)
			},
			expected: "outside packed inode",
		},
		{
			name: "invalid packed inode",
			mutate: func(image []byte) {
				binary.LittleEndian.PutUint64(image[superOffset+96:], 999999)
				updateSuperblockChecksum(image)
			},
			expected: "inode 999999 offset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			image := readFixtureBytes(t, compressedFixture)
			tt.mutate(image)
			updateSuperblockChecksum(image)
			corrupted, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			_, err = corrupted.ReadFile("sub/data.bin")
			if err == nil || !strings.Contains(err.Error(), tt.expected) {
				t.Fatalf("ReadFile() error = %v, want containing %q", err, tt.expected)
			}
		})
	}
}

func TestFullCompressedFixtureCorruption(t *testing.T) {
	filesystem := readTestImage(t, fullFixture)
	_, headerOffset := compressedMetadata(t, filesystem, "data.bin")
	indexOffset := headerOffset + 16

	tests := []struct {
		name     string
		mutate   func([]byte)
		expected string
	}{
		{
			name:     "unsupported algorithm",
			mutate:   func(image []byte) { image[headerOffset+6] = compressionLZ4 },
			expected: "unsupported compression algorithm 0",
		},
		{
			name: "invalid full head type",
			mutate: func(image []byte) {
				binary.LittleEndian.PutUint16(image[indexOffset:], uint16(zClusterTypeHead2))
			},
			expected: "unsupported compression algorithm 0",
		},
		{
			name: "extent outside image",
			mutate: func(image []byte) {
				binary.LittleEndian.PutUint32(image[indexOffset+4:], math.MaxUint32)
			},
			expected: "outside the image",
		},
		{
			name: "malformed LZMA",
			mutate: func(image []byte) {
				physicalStart := int64(binary.LittleEndian.Uint32(image[indexOffset+4:])) * filesystem.blockSize()
				for offset := physicalStart; offset < physicalStart+filesystem.blockSize(); offset++ {
					image[offset] ^= 0xff
				}
			},
			expected: "unable to initialize LZMA decoder",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			image := readFixtureBytes(t, fullFixture)
			tt.mutate(image)
			updateSuperblockChecksum(image)
			corrupted, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			_, err = corrupted.ReadFile("data.bin")
			if err == nil || !strings.Contains(err.Error(), tt.expected) {
				t.Fatalf("ReadFile() error = %v, want containing %q", err, tt.expected)
			}
		})
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
