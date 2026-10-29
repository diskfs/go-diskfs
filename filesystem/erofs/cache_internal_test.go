package erofs

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"
)

func TestConcurrentCompressedReadAt(t *testing.T) {
	filesystem := readTestImage(t, fullFixture)
	opened, err := filesystem.OpenFile("data.bin", os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	reader := opened.(io.ReaderAt)
	payload := fullFixturePayload()
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Go(func() {
			buffer := make([]byte, 1024)
			for i := 0; i < 32; i++ {
				offset := (worker*7919 + i*4093) % (len(payload) - len(buffer))
				n, err := reader.ReadAt(buffer, int64(offset))
				if err != nil || n != len(buffer) || !bytes.Equal(buffer, payload[offset:offset+n]) {
					t.Errorf("ReadAt(%d) = %d, %v or incorrect data", offset, n, err)
					return
				}
			}
		})
	}
	workers.Wait()
	// ReaderAt must not change the sequential cursor.
	buffer := make([]byte, 32)
	if _, err := io.ReadFull(opened, buffer); err != nil || !bytes.Equal(buffer, payload[:len(buffer)]) {
		t.Fatalf("Read after ReadAt = %x, %v", buffer, err)
	}
}
