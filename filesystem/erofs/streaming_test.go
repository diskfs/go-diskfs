package erofs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/backend"
)

type extentReadBackend struct {
	backend.Storage
	start, length int64
	reads         int
}

func (b *extentReadBackend) ReadAt(p []byte, offset int64) (int, error) {
	if offset < b.start+b.length && offset+int64(len(p)) > b.start {
		b.reads++
	}
	return b.Storage.ReadAt(p, offset)
}

func TestAdjacentReadsReuseDecodedExtent(t *testing.T) {
	for _, fragment := range []bool{false, true} {
		t.Run(fmt.Sprintf("fragment=%t", fragment), func(t *testing.T) {
			image := readFixtureBytes(t, fullFixture)
			if fragment {
				// Keep the existing file's directory entry, but turn its inode
				// into an all-fragment file. Move the original full-index inode
				// and indexes to a free metadata slot as the packed inode. This
				// exercises a compressed packed inode without requiring mkfs.
				const originalOffset, packedOffset, packedNID = 40 * inodeSlotSize, 2048, 64
				const metadataLength = inodeCompactSize + 16 + 24*8
				copy(image[packedOffset:], image[originalOffset:originalOffset+metadataLength])
				binary.LittleEndian.PutUint64(image[originalOffset+inodeCompactSize:], uint64(1)<<63)
				binary.LittleEndian.PutUint64(image[superOffset+96:], packedNID)
				updateSuperblockChecksum(image)
			}
			filesystem, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			counter := &extentReadBackend{Storage: filesystem.backend, start: 4096, length: 4096}
			filesystem.backend = counter
			opened, err := filesystem.OpenFile("data.bin", os.O_RDONLY)
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			reader, ok := opened.(io.ReaderAt)
			if !ok {
				t.Fatalf("OpenFile() returned %T, want io.ReaderAt", opened)
			}
			payload := fullFixturePayload()
			buffer := make([]byte, 4096)
			for offset := 0; offset < len(payload); offset += len(buffer) {
				// Alternate Read and ReadAt so both public APIs share the cache.
				var n int
				if offset%8192 == 0 {
					_, err = opened.Seek(int64(offset), io.SeekStart)
					if err != nil {
						t.Fatal(err)
					}
					n, err = opened.Read(buffer)
				} else {
					n, err = reader.ReadAt(buffer, int64(offset))
				}
				if (err != nil && !errors.Is(err, io.EOF)) || n != len(buffer) || !bytes.Equal(buffer, payload[offset:offset+n]) {
					t.Fatalf("read at %d = %d, %v or incorrect data", offset, n, err)
				}
			}
			if _, err := reader.ReadAt(buffer, 0); err != nil || !bytes.Equal(buffer, payload[:len(buffer)]) {
				t.Fatalf("read back into cached extent: %v or incorrect data", err)
			}
			if counter.reads != 1 {
				t.Fatalf("compressed data fetched %d times, want once", counter.reads)
			}
		})
	}
}

// BenchmarkLargeExtentStreaming measures a fresh open/extraction each time,
// including first decode, so it cannot benchmark an already warmed cache. The
// image uses 1 MiB physical clusters and multi-megabyte decoded extents, with a
// fragment tail. The image and expected payload are loaded outside the timed region.
func BenchmarkLargeExtentStreaming(b *testing.B) {
	filesystem := readTestImage(b, largeFixture)
	payload := largeFixturePayload()
	for _, bufferSize := range []int{32 << 10, len(payload)} {
		b.Run(fmt.Sprintf("buffer=%d", bufferSize), func(b *testing.B) {
			buffer := make([]byte, bufferSize)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for b.Loop() {
				opened, err := filesystem.Open("test/path/file")
				if err != nil {
					b.Fatal(err)
				}
				total := 0
				for {
					n, err := opened.Read(buffer)
					if !bytes.Equal(buffer[:n], payload[total:total+n]) {
						b.Fatal("incorrect payload")
					}
					total += n
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if total != len(payload) {
					b.Fatalf("read %d bytes, want %d", total, len(payload))
				}
				if err := opened.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
