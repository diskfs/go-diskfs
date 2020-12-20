package squashfs_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/filesystem/squashfs"
	"github.com/diskfs/go-diskfs/testhelper"
)

func testRandomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(65 + rand.Intn(25)) //A=65 and Z = 65+25
	}
	return string(bytes)
}

func TestFileRead(t *testing.T) {
	blocksize := 0x20000
	size := blocksize + 5
	contentLong := []byte(testRandomString(int(size)))
	contentShort := []byte("README\n")

	fileImpl := &testhelper.FileImpl{}
	fileImpl.Reader = func(b []byte, offset int64) (int, error) {
		var b2 []byte
		switch offset {
		case 96: // regular block
			b2 = contentLong[:blocksize]
		case 200000: // fragment block
			b2 = append(contentShort, contentLong[blocksize:]...)
		}
		copy(b, b2)
		count := len(b2)
		if len(b) < len(b2) {
			count = len(b)
		}
		return count, io.EOF
	}

	t.Run("fragment only", func(t *testing.T) {
		// stub the file reader
		f, err := squashfs.GetTestFileSmall(fileImpl, nil)
		if err != nil {
			t.Fatalf("Unable to get small test file: %v", err)
		}

		b := make([]byte, 20, 20)
		read, err := f.Read(b)
		if err != nil && err != io.EOF {
			t.Errorf("received unexpected error when reading: %v", err)
		}
		if read != len(contentShort) {
			t.Errorf("read %d bytes instead of expected %d", read, len(contentShort))
		}
		bString := string(b[:read])
		if bytes.Compare(b[:read], contentShort) != 0 {
			t.Errorf("Mismatched content:\nActual: '%s'\nExpected: '%s'", bString, contentShort)
		}
	})
	t.Run("blocks", func(t *testing.T) {
		// stub the file reader
		f, err := squashfs.GetTestFileBig(fileImpl, nil)
		if err != nil {
			t.Fatalf("Unable to get small test file: %v", err)
		}

		filesize := blocksize + 5
		b := make([]byte, filesize, filesize)
		read, err := f.Read(b)
		if err != nil && err != io.EOF {
			t.Errorf("received unexpected error when reading: %v", err)
		}
		if read != len(contentLong) {
			t.Errorf("read %d bytes instead of expected %d", read, len(contentLong))
		}
		bString := string(b[:read])
		if bytes.Compare(b[:read], contentLong) != 0 {
			t.Errorf("Mismatched content:\nActual: '%s...'\nExpected: '%s...'", bString[:20], contentLong[:20])
		}
	})
}

func TestFileWrite(t *testing.T) {
	// pretty simple: never should be able to write as it is a read-only filesystem
	f := &squashfs.File{}
	b := make([]byte, 8, 8)
	written, err := f.Write(b)
	if err == nil {
		t.Errorf("received no error when should have been prevented from writing")
	}
	if written != 0 {
		t.Errorf("wrote %d bytes instead of expected %d", written, 0)
	}
}

func TestFileSeek(t *testing.T) {
	tests := []struct {
		offset   int64
		whence   int
		expected int64
		err      error
	}{
		{100, io.SeekStart, 100, nil},
		{100, io.SeekCurrent, 100, nil},
		{50, io.SeekEnd, 150, nil},
		{250, io.SeekEnd, 0, fmt.Errorf("Cannot set offset %d before start of file", 250)},
	}

	for i, tt := range tests {
		f := squashfs.MakeTestFile(200)
		offset, err := f.Seek(tt.offset, tt.whence)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case offset != tt.expected:
			t.Errorf("%d: mismatched resulting offset, actual %d, expected %d", i, offset, tt.expected)
		}
	}
}
