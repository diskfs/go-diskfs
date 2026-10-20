package erofs

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/backend/file"
)

func testBackend(t *testing.T, image []byte) backend.Storage {
	t.Helper()
	path := filepath.Join(t.TempDir(), "image.erofs")
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return file.New(f, true)
}

func setCompactInode(b []byte, offset int, format, mode uint16, size, startBlock uint32) {
	binary.LittleEndian.PutUint16(b[offset:], format)
	binary.LittleEndian.PutUint16(b[offset+2:], 0)
	binary.LittleEndian.PutUint16(b[offset+4:], mode)
	binary.LittleEndian.PutUint16(b[offset+6:], 0)
	binary.LittleEndian.PutUint32(b[offset+8:], size)
	binary.LittleEndian.PutUint32(b[offset+12:], 1)
	binary.LittleEndian.PutUint32(b[offset+16:], startBlock)
	binary.LittleEndian.PutUint32(b[offset+20:], uint32(offset/inodeSlotSize))
	binary.LittleEndian.PutUint16(b[offset+24:], 0)
	binary.LittleEndian.PutUint16(b[offset+26:], 0)
}

func setDirectoryEntry(b []byte, offset int, nid uint64, name string, fileType byte) {
	binary.LittleEndian.PutUint64(b[offset:], nid)
	binary.LittleEndian.PutUint16(b[offset+8:], 12)
	b[offset+10] = fileType
	b[offset+11] = 0
}

func buildImage(inline bool) []byte {
	image := make([]byte, 16384)
	copy(image[superOffset:], validSuperblockBytes())
	binary.LittleEndian.PutUint32(image[superOffset+36:], 3)
	if !inline {
		binary.LittleEndian.PutUint32(image[superOffset+36:], 4)
	}

	setCompactInode(image, 4096, 1<<4, linuxModeDir, 17, 2)
	setCompactInode(image, 4128, 1<<4|(uint16(dataLayoutFlatInline)<<1), linuxModeRegular, 11, 0)
	if !inline {
		format := 1<<4 | uint16(dataLayoutFlatPlain)<<1
		setCompactInode(image, 4128, format, linuxModeRegular, 11, 3)
		copy(image[12288:], []byte("hello world"))
	} else {
		copy(image[4160:], []byte("hello world"))
	}

	setDirectoryEntry(image, 8192, 1, "hello", fileTypeRegular)
	copy(image[8204:], []byte("hello"))
	return image
}

func TestReadCoreFilesystem(t *testing.T) {
	image := buildImage(false)
	filesystem, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	entries, err := filesystem.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "hello" {
		t.Fatalf("ReadDir() entries = %#v, want hello", entries)
	}
	info, err := filesystem.Stat("hello")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() != 11 || !info.Mode().IsRegular() {
		t.Fatalf("Stat() = size %d mode %v", info.Size(), info.Mode())
	}
	data, err := filesystem.ReadFile("hello")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("ReadFile() = %q", data)
	}
}

func TestReadInlineFilesystem(t *testing.T) {
	image := buildImage(true)
	filesystem, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	data, err := filesystem.ReadFile("hello")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("ReadFile() = %q", data)
	}
}

func TestReadPartitionOffset(t *testing.T) {
	image := buildImage(false)
	partitioned := append(make([]byte, 4096), image...)
	filesystem, err := Read(testBackend(t, partitioned), int64(len(image)), 4096, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	data, err := filesystem.ReadFile("hello")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("ReadFile() = %q", data)
	}
}

func TestFileReadAndSeek(t *testing.T) {
	filesystem, err := Read(testBackend(t, buildImage(false)), 16384, 0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	f, err := filesystem.OpenFile("hello", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer f.Close()
	b := make([]byte, 5)
	if _, err := io.ReadFull(f, b); err != nil {
		t.Fatalf("io.ReadFull() error = %v", err)
	}
	if string(b) != "hello" {
		t.Fatalf("first read = %q", b)
	}
	if _, err := f.Seek(6, io.SeekStart); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	if _, err := io.ReadFull(f, b); err != nil {
		t.Fatalf("second io.ReadFull() error = %v", err)
	}
	if string(b) != "world" {
		t.Fatalf("second read = %q", b)
	}
}
