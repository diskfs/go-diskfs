package erofs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/backend/file"
	fsapi "github.com/diskfs/go-diskfs/filesystem"
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

func TestReadInlineFileSpanningTailBoundary(t *testing.T) {
	image := buildImage(false)
	setCompactInode(image, 4128, 1<<4|(uint16(dataLayoutFlatInline)<<1), linuxModeRegular, 4097, 3)
	copy(image[12288:], bytes.Repeat([]byte("a"), 4096))
	image[4160] = 'z'

	filesystem, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	data, err := filesystem.ReadFile("hello")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if len(data) != 4097 || data[0] != 'a' || data[4095] != 'a' || data[4096] != 'z' {
		t.Fatalf("ReadFile() length = %d, boundary = %q..%q", len(data), data[:1], data[4096:])
	}
}

func TestReadVerifiesSuperblockChecksum(t *testing.T) {
	image := buildImage(false)
	binary.LittleEndian.PutUint32(image[superOffset+8:superOffset+12], featureCompatChecksum)
	binary.LittleEndian.PutUint32(image[superOffset+4:superOffset+8], superblockChecksum(image[superOffset:4096]))

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

	image[1500] ^= 1
	if _, err := Read(testBackend(t, image), int64(len(image)), 0, 0); err == nil {
		t.Fatal("Read() error = nil after checksum-relevant corruption")
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

func TestFSConformance(t *testing.T) {
	filesystem := readTestImage(t, uncompressedFixture)
	if err := fstest.TestFS(filesystem, "empty.txt", "hello.txt", "sub/data.bin"); err != nil {
		t.Fatalf("fstest.TestFS() error = %v", err)
	}
}

func TestInvalidPathsAndModes(t *testing.T) {
	filesystem := readTestImage(t, uncompressedFixture)
	if _, err := filesystem.Open("../hello.txt"); !errors.Is(err, iofs.ErrInvalid) {
		t.Fatalf("Open(../hello.txt) error = %v, want %v", err, iofs.ErrInvalid)
	}
	if _, err := filesystem.Stat("missing"); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("Stat(missing) error = %v, want %v", err, iofs.ErrNotExist)
	}
	if _, err := filesystem.ReadFile("empty"); err == nil {
		t.Fatal("ReadFile(empty) error = nil, want directory error")
	}
	if _, err := filesystem.OpenFile("hello.txt", os.O_WRONLY); !errors.Is(err, fsapi.ErrReadonlyFilesystem) {
		t.Fatalf("OpenFile(O_WRONLY) error = %v, want %v", err, fsapi.ErrReadonlyFilesystem)
	}
}

func TestCompressedReadAtBoundaries(t *testing.T) {
	filesystem := readTestImage(t, compressedFixture)
	file, err := filesystem.Open("sub/data.bin")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer file.Close()
	expected := testPayload()

	tests := []struct {
		offset int64
		length int
	}{
		{offset: 0, length: 7},
		{offset: 4095, length: 2},
		{offset: 61439, length: 2},
		{offset: 65535, length: 2},
		{offset: 98303, length: 2},
		{offset: 98304, length: 1},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d/%d", tt.offset, tt.length), func(t *testing.T) {
			b := make([]byte, tt.length)
			read, err := file.(*File).ReadAt(b, tt.offset)
			want := expected[min(tt.offset, int64(len(expected))):]
			want = want[:min(int64(len(want)), int64(tt.length))]
			if !bytes.Equal(b[:read], want) {
				t.Fatalf("ReadAt() = %q, want %q", b[:read], want)
			}
			if tt.offset+int64(tt.length) > int64(len(expected)) && !errors.Is(err, io.EOF) {
				t.Fatalf("ReadAt() error = %v, want %v", err, io.EOF)
			}
			if tt.offset+int64(tt.length) <= int64(len(expected)) && err != nil {
				t.Fatalf("ReadAt() error = %v", err)
			}
		})
	}

	if _, err := file.(*File).Seek(61439, io.SeekStart); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	b := make([]byte, 2)
	if _, err := io.ReadFull(file, b); err != nil {
		t.Fatalf("io.ReadFull() error = %v", err)
	}
	if !bytes.Equal(b, expected[61439:61441]) {
		t.Fatalf("read after seek = %q, want %q", b, expected[61439:61441])
	}
}

func buildMetadataImage() []byte {
	image := make([]byte, 16384)
	copy(image[superOffset:], validSuperblockBytes())
	binary.LittleEndian.PutUint32(image[superOffset+36:], 4)

	setCompactInode(image, 4096, 1<<4, linuxModeDir, 127, 2)

	type metadata struct {
		name     string
		format   uint16
		mode     uint16
		size     uint32
		start    uint32
		fileType byte
	}
	nodes := []metadata{
		{name: "block", format: uint16(dataLayoutFlatPlain) << 1, mode: linuxModeBlock | 0o644, start: 0x0802, fileType: fileTypeBlockDevice},
		{name: "char", format: uint16(dataLayoutFlatPlain) << 1, mode: linuxModeChar | 0o644, start: 0x0301, fileType: fileTypeCharDevice},
		{name: "fifo", format: uint16(dataLayoutFlatPlain) << 1, mode: linuxModeFIFO | 0o644, fileType: fileTypeFIFO},
		{name: "hardlink", format: uint16(dataLayoutFlatPlain) << 1, mode: linuxModeRegular | 0o644, size: 11, start: 3, fileType: fileTypeRegular},
		{name: "socket", format: uint16(dataLayoutFlatPlain) << 1, mode: linuxModeSocket | 0o644, fileType: fileTypeSocket},
		{name: "symlink", format: 1<<4 | uint16(dataLayoutFlatInline)<<1, mode: linuxModeSymlink | 0o777, size: 9, fileType: fileTypeSymlink},
	}
	for index, node := range nodes {
		inodeOffset := 4128 + index*32
		setCompactInode(image, inodeOffset, node.format, node.mode, node.size, node.start)
		if node.name == "hardlink" {
			binary.LittleEndian.PutUint16(image[inodeOffset+6:], 2)
		}
	}
	copy(image[4320:], "hello.txt")
	copy(image[12288:], "hello world")

	directory := make([]byte, 127)
	names := []struct {
		name string
		nid  uint64
	}{
		{name: "block", nid: 1},
		{name: "char", nid: 2},
		{name: "fifo", nid: 3},
		{name: "hardlink", nid: 4},
		{name: "hardlink2", nid: 4},
		{name: "socket", nid: 5},
		{name: "symlink", nid: 6},
	}
	nameOffset := 84
	for index, entry := range names {
		fileType := byte(fileTypeRegular)
		switch entry.name {
		case "block":
			fileType = fileTypeBlockDevice
		case "char":
			fileType = fileTypeCharDevice
		case "fifo":
			fileType = fileTypeFIFO
		case "socket":
			fileType = fileTypeSocket
		case "symlink":
			fileType = fileTypeSymlink
		}
		setDirectoryEntry(directory, index*direntSize, entry.nid, entry.name, fileType)
		binary.LittleEndian.PutUint16(directory[index*direntSize+8:], uint16(nameOffset))
		copy(directory[nameOffset:], entry.name)
		nameOffset += len(entry.name)
	}
	binary.LittleEndian.PutUint16(directory[8:], 84)
	copy(image[8192:], directory)
	return image
}

func TestSpecialFileMetadataAndHardLinks(t *testing.T) {
	filesystem, err := Read(testBackend(t, buildMetadataImage()), 16384, 0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	expectedModes := map[string]iofs.FileMode{
		"block":   iofs.ModeDevice | 0o644,
		"char":    iofs.ModeDevice | iofs.ModeCharDevice | 0o644,
		"fifo":    iofs.ModeNamedPipe | 0o644,
		"socket":  iofs.ModeSocket | 0o644,
		"symlink": iofs.ModeSymlink | 0o777,
	}
	for name, mode := range expectedModes {
		info, err := filesystem.Stat(name)
		if err != nil {
			t.Fatalf("Stat(%q) error = %v", name, err)
		}
		if info.Mode() != mode {
			t.Fatalf("Stat(%q) mode = %v, want %v", name, info.Mode(), mode)
		}
	}
	blockInfo, err := filesystem.Stat("block")
	if err != nil {
		t.Fatalf("Stat(block) error = %v", err)
	}
	blockStat, ok := blockInfo.Sys().(*StatT)
	if !ok || blockStat.DeviceID != 0x0802 {
		t.Fatalf("Stat(block).Sys() = %#v, want device ID 0x0802", blockInfo.Sys())
	}
	charInfo, err := filesystem.Stat("char")
	if err != nil {
		t.Fatalf("Stat(char) error = %v", err)
	}
	charStat, ok := charInfo.Sys().(*StatT)
	if !ok || charStat.DeviceID != 0x0301 {
		t.Fatalf("Stat(char).Sys() = %#v, want device ID 0x0301", charInfo.Sys())
	}
	for _, name := range []string{"hardlink", "hardlink2"} {
		info, err := filesystem.Stat(name)
		if err != nil {
			t.Fatalf("Stat(%q) error = %v", name, err)
		}
		stat, ok := info.Sys().(*StatT)
		if !ok || stat.Inode != 4 || stat.LinkCount != 2 {
			t.Fatalf("Stat(%q).Sys() = %#v, want inode 4 with two links", name, info.Sys())
		}
		data, err := filesystem.ReadFile(name)
		if err != nil || string(data) != "hello world" {
			t.Fatalf("ReadFile(%q) = %q, %v", name, data, err)
		}
	}
	if target, err := filesystem.ReadFile("symlink"); err != nil || string(target) != "hello.txt" {
		t.Fatalf("ReadFile(symlink) = %q, %v", target, err)
	}
}
