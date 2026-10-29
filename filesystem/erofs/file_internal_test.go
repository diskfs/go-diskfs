package erofs

import (
	"errors"
	"io"
	"math"
	"os"
	"testing"
)

func TestFileReadAtEOF(t *testing.T) {
	filesystem := readTestImage(t, uncompressedFixture)
	for _, path := range []string{"hello.txt", "empty.txt"} {
		t.Run(path, func(t *testing.T) {
			opened, err := filesystem.OpenFile(path, os.O_RDONLY)
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			info, err := opened.Stat()
			if err != nil {
				t.Fatal(err)
			}
			reader := opened.(io.ReaderAt)
			for _, offset := range []int64{info.Size(), info.Size() + 1, math.MaxInt64} {
				for _, length := range []int{0, 1} {
					n, err := reader.ReadAt(make([]byte, length), offset)
					var want error
					if length != 0 {
						want = io.EOF
					}
					if n != 0 || !errors.Is(err, want) {
						t.Errorf("ReadAt(length=%d, offset=%d) = %d, %v; want 0, %v", length, offset, n, err, want)
					}
				}
			}
			for _, length := range []int{0, 1} {
				if _, err := reader.ReadAt(make([]byte, length), -1); err == nil || errors.Is(err, io.EOF) {
					t.Errorf("ReadAt(length=%d, -1) error = %v; want invalid offset", length, err)
				}
			}
			if info.Size() > 0 {
				if n, err := reader.ReadAt(make([]byte, 2), info.Size()-1); n != 1 || !errors.Is(err, io.EOF) {
					t.Errorf("ReadAt crossing EOF = %d, %v; want 1, EOF", n, err)
				}
			}
		})
	}
}
