package erofs

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/diskfs/go-diskfs/backend"
)

// The backing file deliberately extends past the filesystem. Recording reads
// catches boundary violations even if a later validation rejects the image.
type checkedReadBackend struct {
	backend.Storage
	t           *testing.T
	start, size int64
}

func (b checkedReadBackend) ReadAt(p []byte, offset int64) (int, error) {
	if offset < b.start || offset > b.start+b.size || int64(len(p)) > b.start+b.size-offset {
		b.t.Errorf("backend read at %d, length %d exceeds filesystem [%d, %d)", offset, len(p), b.start, b.start+b.size)
	}
	return b.Storage.ReadAt(p, offset)
}

func TestReadSuperblockBounds(t *testing.T) {
	tests := []struct {
		name       string
		size       int64
		extensions byte
	}{
		{name: "header", size: superOffset + 13},
		{name: "base", size: superOffset + 64},
		{name: "extension", size: superOffset + superblockBaseSize, extensions: 1},
	}
	for _, start := range []int64{0, 4096} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s/start=%d", tt.name, start), func(t *testing.T) {
				image := buildImage(false)
				image[superOffset+13] = tt.extensions
				whole := append(make([]byte, start), image...)
				b := checkedReadBackend{Storage: testBackend(t, whole), t: t, start: start, size: tt.size}
				if _, err := Read(b, tt.size, start, 0); err == nil {
					t.Fatal("Read() accepted an out-of-bounds superblock")
				}
			})
		}
	}
}

func TestReadCompressionMetadataBounds(t *testing.T) {
	const size = int64(12288)
	tests := []struct {
		name         string
		nid          uint64
		layout       uint8
		xattrs       uint16
		headerOffset int
		header       uint64
	}{
		{name: "header", nid: 255, layout: dataLayoutCompressedFull, headerOffset: 12288, header: uint64(1) << 63},
		{name: "full index", nid: 254, layout: dataLayoutCompressedFull, xattrs: 1, headerOffset: 12272, header: uint64(zAdviseFragment) << 32},
		{name: "compact index", nid: 254, layout: dataLayoutCompressedCompact, xattrs: 3, headerOffset: 12280, header: uint64(zAdviseFragment) << 32},
	}
	for _, start := range []int64{0, 4096} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s/start=%d", tt.name, start), func(t *testing.T) {
				image := buildImage(true)
				binary.LittleEndian.PutUint64(image[superOffset+96:], 1)
				binary.LittleEndian.PutUint64(image[8192:], tt.nid)
				inodeOffset := 4096 + int(tt.nid)*inodeSlotSize
				setCompactInode(image, inodeOffset, uint16(tt.layout)<<1, linuxModeRegular, 11, 0)
				binary.LittleEndian.PutUint16(image[inodeOffset+2:], tt.xattrs)
				binary.LittleEndian.PutUint64(image[tt.headerOffset:], tt.header)
				// Zeroed indexes encode a plain cluster at logical offset zero.
				// Without bounds checks these indexes resolve to the packed file.
				whole := append(make([]byte, start), image...)
				b := checkedReadBackend{Storage: testBackend(t, whole), t: t, start: start, size: size}
				filesystem, err := Read(b, size, start, 0)
				if err != nil {
					t.Fatalf("Read() error = %v", err)
				}
				if data, err := filesystem.ReadFile("hello"); err == nil {
					t.Fatalf("ReadFile() = %q, nil; accepted out-of-bounds metadata", data)
				}
			})
		}
	}
}

func TestReadCompressionConfigBounds(t *testing.T) {
	const size = int64(12288)
	for _, start := range []int64{0, 4096} {
		for _, lengthOutside := range []bool{false, true} {
			t.Run(fmt.Sprintf("lengthOutside=%t/start=%d", lengthOutside, start), func(t *testing.T) {
				image := buildImage(true)
				binary.LittleEndian.PutUint32(image[superOffset+80:], featureIncompatComprCfgs)
				binary.LittleEndian.PutUint16(image[superOffset+84:], 1<<compressionLZ4)
				configOffset := superOffset + superblockBaseSize
				if lengthOutside {
					// The first config ends exactly at the filesystem boundary;
					// the next record's length and body are in the backing file only.
					binary.LittleEndian.PutUint16(image[superOffset+84:], 1<<compressionLZ4|1<<compressionLZMA)
					binary.LittleEndian.PutUint16(image[configOffset:], uint16(size-int64(configOffset)-2))
					binary.LittleEndian.PutUint16(image[size:], 14)
					binary.LittleEndian.PutUint32(image[size+2:], 4096)
				} else {
					binary.LittleEndian.PutUint16(image[configOffset:], uint16(size))
				}
				whole := append(make([]byte, start), image...)
				b := checkedReadBackend{Storage: testBackend(t, whole), t: t, start: start, size: size}
				if _, err := Read(b, size, start, 0); err == nil {
					t.Fatal("Read() accepted an out-of-bounds compression configuration")
				}
			})
		}
	}
}

func TestReadRejectsOverflowingPartitionRange(t *testing.T) {
	if _, err := Read(nil, 4096, math.MaxInt64-4095, 0); err == nil {
		t.Fatal("Read() accepted a partition range exceeding MaxInt64")
	}
}

func TestReadAtBounds(t *testing.T) {
	tests := []struct {
		name         string
		size, offset int64
		length       int
		wantError    bool
	}{
		{name: "first byte", size: 16, offset: 0, length: 1},
		{name: "last byte", size: 16, offset: 15, length: 1},
		{name: "empty at end", size: 16, offset: 16},
		{name: "past end", size: 16, offset: 16, length: 1, wantError: true},
		{name: "crosses end", size: 16, offset: 15, length: 2, wantError: true},
		{name: "negative offset", size: 16, offset: -1, length: 1, wantError: true},
		{name: "overflowing end", size: math.MaxInt64, offset: math.MaxInt64 - 1, length: 2, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := checkedReadBackend{Storage: testBackend(t, make([]byte, 32)), t: t, size: tt.size}
			filesystem := &FileSystem{backend: b, size: tt.size}
			err := filesystem.readAt(make([]byte, tt.length), tt.offset)
			if (err != nil) != tt.wantError {
				t.Fatalf("readAt() error = %v, wantError = %t", err, tt.wantError)
			}
		})
	}
}
