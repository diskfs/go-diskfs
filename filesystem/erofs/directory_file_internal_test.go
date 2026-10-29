package erofs

import (
	"errors"
	"io"
	"io/fs"
	"math"
	"testing"
)

func TestReadDirLargeCount(t *testing.T) {
	filesystem := readTestImage(t, uncompressedFixture)
	opened, err := filesystem.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	directory := opened.(fs.ReadDirFile)
	first, err := directory.ReadDir(1)
	if err != nil || len(first) != 1 {
		t.Fatalf("ReadDir(1) = %v, %v", first, err)
	}
	rest, err := directory.ReadDir(math.MaxInt)
	if err != nil || len(rest) != 4 {
		t.Fatalf("ReadDir(MaxInt) returned %d entries, %v; want 4, nil", len(rest), err)
	}
	if entries, err := directory.ReadDir(math.MaxInt); len(entries) != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("exhausted ReadDir(MaxInt) = %v, %v", entries, err)
	}
}
