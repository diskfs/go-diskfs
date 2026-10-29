package erofs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/backend/file"
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
			reader := opened.(io.ReaderAt)
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
// fragment tail. Generation requires mkfs.erofs and is outside the timed region.
func BenchmarkLargeExtentStreaming(b *testing.B) {
	mkfs, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		b.Skip("mkfs.erofs is not available")
	}
	directory := b.TempDir()
	source := filepath.Join(directory, "source")
	if err := os.Mkdir(source, 0o755); err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 15<<20)
	seed := uint32(0x45524f46)
	for record := 0; record < len(payload); record += 64 << 10 {
		for offset := record; offset < record+(16<<10); offset += 4 {
			seed = seed*1664525 + 1013904223
			binary.LittleEndian.PutUint32(payload[offset:], seed)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), payload, 0o644); err != nil {
		b.Fatal(err)
	}
	imagePath := filepath.Join(directory, "image.erofs")
	command := exec.Command(mkfs, "-zlzma", "-C1048576", "-Efragments", imagePath, source)
	if output, err := command.CombinedOutput(); err != nil {
		b.Fatalf("mkfs.erofs: %v: %s", err, output)
	}
	image, err := os.Open(imagePath)
	if err != nil {
		b.Fatal(err)
	}
	defer image.Close()
	info, err := image.Stat()
	if err != nil {
		b.Fatal(err)
	}
	filesystem, err := Read(file.New(image, true), info.Size(), 0, 0)
	if err != nil {
		b.Fatal(err)
	}
	for _, bufferSize := range []int{32 << 10, len(payload)} {
		b.Run(fmt.Sprintf("buffer=%d", bufferSize), func(b *testing.B) {
			buffer := make([]byte, bufferSize)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for b.Loop() {
				opened, err := filesystem.Open("payload")
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
