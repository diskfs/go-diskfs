package squashfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

// Tests guarding GHSA-6jcg-q3wp-x2f4 (CVE-2026-79775): denial of service when
// parsing a maliciously crafted SquashFS image. The parser trusted
// attacker-controlled superblock and metadata values in three places:
//
//  1. a zero block size passes the superblock block-log check
//     (uint16(math.Log2(0)) is 0) and then divides by zero when the
//     filesystem is set up;
//  2. an inode/directory metadata offset can point past the end of the
//     uncompressed metadata block it indexes, panicking on the slice;
//  3. a truncated metadata stream made up of zero-size blocks never advances
//     the read loop, spinning the CPU forever.
//
// All three are now rejected with an error instead of panicking or hanging.

// testSuperblockWithBlocksize returns a valid superblock with its block size
// and block log replaced by the given values.
func testSuperblockWithBlocksize(b []byte, blocksize uint32, blocklog uint16) []byte {
	out := append([]byte{}, b...)
	binary.LittleEndian.PutUint32(out[12:16], blocksize)
	binary.LittleEndian.PutUint16(out[22:24], blocklog)
	return out
}

func TestParseSuperblockRejectsInvalidBlocksize(t *testing.T) {
	b, _, err := testGetValidSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		blocksize uint32
		blocklog  uint16
	}{
		// uint16(math.Log2(0)) is 0, so the block-log check alone accepts these
		{"zero blocksize", 0, 0},
		// too small for squashfs (minimum is 4 KiB)
		{"too small", 2048, 11},
		// too large for squashfs (maximum is 1 MiB)
		{"too large", 2 * 1024 * 1024, 21},
		// not a power of two; the log check rounds it, so it must be caught here
		{"not a power of two", 4097, 12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := parseSuperblock(testSuperblockWithBlocksize(b, tt.blocksize, tt.blocklog))
			if err == nil {
				t.Fatalf("expected error for blocksize %d, got superblock %v", tt.blocksize, s)
			}
			if !strings.Contains(err.Error(), "block size invalid") {
				t.Errorf("mismatched error, actual %v", err)
			}
			if s != nil {
				t.Errorf("expected nil superblock, got %v", s)
			}
		})
	}
	// the untouched superblock must still parse
	s, err := parseSuperblock(b)
	if err != nil {
		t.Fatalf("unexpected error parsing valid superblock: %v", err)
	}
	if s == nil {
		t.Fatal("expected valid superblock, got nil")
	}
}

func TestReadMetadataByteOffsetBeyondBlock(t *testing.T) {
	// one uncompressed metadata block holding 5 bytes
	data := []byte{0x05, 0x80, 1, 2, 3, 4, 5}
	fs := &FileSystem{}
	out, err := fs.readMetadata(bytes.NewReader(data), nil, 0, 0, 100, 64)
	if err == nil {
		t.Fatalf("expected error for byte offset 100 in a 5-byte block, got %v", out)
	}
	if !strings.Contains(err.Error(), "cannot read at offset 100") {
		t.Errorf("mismatched error, actual %v", err)
	}
}

func TestReadMetadataZeroSizeBlocks(t *testing.T) {
	// 512 bytes of zero-size uncompressed blocks. Every 2-byte header reads as
	// size 0, so the read loop never accumulates the requested bytes and, with a
	// compressor that tolerates empty input, spins forever reading past the end
	// of the data. It must fail instead.
	data := bytes.Repeat([]byte{0x00, 0x80}, 256)
	fs := &FileSystem{}
	out, err := fs.readMetadata(bytes.NewReader(data), &testCompressorAddBytes{}, 0, 0, 0, 64)
	if err == nil {
		t.Fatalf("expected error for zero-size metadata blocks, got %v", out)
	}
	if !strings.Contains(err.Error(), "is empty") {
		t.Errorf("mismatched error, actual %v", err)
	}
}

func TestReadMetadata(t *testing.T) {
	// two uncompressed metadata blocks of 5 bytes each
	b := []byte{0x05, 0x80, 1, 2, 3, 4, 5, 0x05, 0x80, 9, 8, 7, 6, 5}
	fs := &FileSystem{}
	tests := []struct {
		name          string
		b             []byte
		c             Compressor
		initialOffset uint32
		byteOffset    uint16
		size          int
		out           []byte
		err           error
	}{
		// single block, exact size
		{"single block", b, nil, 0, 0, 5, []byte{1, 2, 3, 4, 5}, nil},
		// fewer bytes than the block holds; readMetadata returns the whole
		// block, leaving the trimming to the caller
		{"short read", b, nil, 0, 0, 3, []byte{1, 2, 3, 4, 5}, nil},
		// offset into the block
		{"offset into block", b, nil, 0, 2, 3, []byte{3, 4, 5}, nil},
		// spanning the boundary between the two blocks
		{"crossing blocks", b, nil, 0, 3, 7, []byte{4, 5, 9, 8, 7, 6, 5}, nil},
		// starting in the second metadata block, via the block offset
		{"second block", b, nil, 7, 0, 2, []byte{9, 8, 7, 6, 5}, nil},
		// a compressed block is decompressed by the supplied compressor
		{"compressed", []byte{0x05, 0x00, 1, 2, 3, 4, 5}, &testCompressorAddBytes{b: []byte{0x25}}, 0, 0, 6, []byte{1, 2, 3, 4, 5, 0x25}, nil},
		// header claims more data than the block holds
		{"truncated block", []byte{0x05, 0x80, 1, 2}, nil, 0, 0, 5, nil, fmt.Errorf("read %d instead of expected %d bytes for metadata block at location %d", 2, 5, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := fs.readMetadata(bytes.NewReader(tt.b), tt.c, 0, tt.initialOffset, tt.byteOffset, tt.size)
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("mismatched error, actual %v expected %v", err, tt.err)
			case !bytes.Equal(out, tt.out):
				t.Errorf("mismatched output, actual % x expected % x", out, tt.out)
			}
		})
	}
}

// TestReadRejectsZeroBlocksizeSuperblock is the end-to-end regression for the
// divide by zero: a superblock with a zero block size used to pass the
// block-log check and panic when the filesystem cache was sized.
func TestReadRejectsZeroBlocksizeSuperblock(t *testing.T) {
	b := make([]byte, superblockSize)
	binary.LittleEndian.PutUint32(b[0:4], superblockMagic)
	binary.LittleEndian.PutUint32(b[4:8], 1) // inodes
	binary.LittleEndian.PutUint32(b[12:16], 0)
	binary.LittleEndian.PutUint16(b[20:22], uint16(compressionNone))
	binary.LittleEndian.PutUint16(b[22:24], 0) // log2(0) == 0, so this matches
	binary.LittleEndian.PutUint16(b[24:26], 0x0200)
	binary.LittleEndian.PutUint16(b[26:28], 0) // no ids
	binary.LittleEndian.PutUint16(b[28:30], superblockMajorVersion)
	binary.LittleEndian.PutUint16(b[30:32], superblockMinorVersion)
	binary.LittleEndian.PutUint64(b[56:64], 0xffffffffffffffff) // no xattr table

	path := filepath.Join(t.TempDir(), "zero-blocksize.sqfs")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	storage, err := file.OpenFromPath(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	fs, err := Read(storage, 4096, 0, 0)
	if err == nil {
		t.Fatalf("expected error for zero block size, got filesystem %v", fs != nil)
	}
	if !strings.Contains(err.Error(), "block size invalid") {
		t.Errorf("mismatched error, actual %v", err)
	}
	if fs != nil {
		t.Errorf("expected nil filesystem, got %v", fs)
	}
}
