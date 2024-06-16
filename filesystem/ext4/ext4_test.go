package ext4

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/go-test/deep"
)

const (
	randomDataFile = "testdata/dist/random.dat"
)

func TestReadDirectory(t *testing.T) {
	// read the foo directory file, which was created from debugfs
	fooDirEntries, err := testDirEntriesFromDebugFS(fooDirFile)
	if err != nil {
		t.Fatalf("Error reading foo directory entries from debugfs: %v", err)
	}

	// read the root directory file, which was created from debugfs
	rootDirEntries, err := testDirEntriesFromDebugFS(rootDirFile)
	if err != nil {
		t.Fatalf("Error reading root directory entries from debugfs: %v", err)
	}

	tests := []struct {
		name    string
		inode   uint32
		entries []*directoryEntry
		err     error
	}{
		{"invalid inode", 0, nil, errors.New("could not read inode")},
		{"root", 2, rootDirEntries, nil},
		{"foo dir", 13, fooDirEntries, nil},
	}
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()
	fs, err := Read(f, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries, err := fs.readDirectory(tt.inode)
			switch {
			case err != nil && tt.err == nil:
				t.Fatalf("unexpected error reading directory: %v", err)
			case err == nil && tt.err != nil:
				t.Fatalf("expected error reading directory: %v", tt.err)
			case err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error()):
				t.Fatalf("mismatched error reading directory, expected '%v' got '%v'", tt.err, err)
			default:
				sortFunc := func(a, b *directoryEntry) int {
					return cmp.Compare(a.filename, b.filename)
				}
				slices.SortFunc(entries, sortFunc)
				slices.SortFunc(tt.entries, sortFunc)
				if diff := deep.Equal(entries, tt.entries); diff != nil {
					t.Errorf("directory entries mismatch: %v", diff)
				}
			}
		})
	}
}

func TestReadFile(t *testing.T) {
	randomFileData, err := os.ReadFile(randomDataFile)
	if err != nil {
		t.Fatalf("Error opening random data file %s: %v", randomDataFile, err)
	}
	tests := []struct {
		name     string
		path     string
		offset   int64
		size     int
		readAll  bool
		expected []byte
		err      error
	}{
		{"invalid path", "/do/not/exist/any/where", 0, 0, false, nil, errors.New("could not read directory entries")},
		{"large file", "/random.dat", 0, len(randomFileData), false, randomFileData, nil},
		{"offset in file", "/random.dat", 5000, 1000, false, randomFileData[5000:6000], nil},
		{"readall", "/random.dat", 0, 0, true, randomFileData, nil},
		{"hard link", "/hardlink.dat", 0, 0, true, randomFileData, nil},
		{"valid symlink", "/symlink.dat", 0, 0, true, randomFileData, nil},
		{"absolute symlink", "/absolutesymlink", 0, 0, true, randomFileData, nil},
		{"dead symlink", "/deadlink", 0, 0, true, nil, fmt.Errorf("target file %s does not exist", "/nonexistent")},
		{"dead long symlink", "/deadlonglink", 0, 0, true, nil, errors.New("could not read directory entries")},
	}
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()
	fs, err := Read(f, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsFile, err := fs.OpenFile(tt.path, 0o600)
			switch {
			case err != nil && tt.err == nil:
				t.Fatalf("unexpected error opening file: %v", err)
			case err == nil && tt.err != nil:
				t.Fatalf("expected error opening file: %v", tt.err)
			case err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error()):
				t.Fatalf("mismatched error opening file, expected '%v' got '%v'", tt.err, err)
			case err == nil:
				var b []byte
				if tt.readAll {
					tt.size = len(tt.expected)
					b, err = io.ReadAll(fsFile)
					if err != nil {
						t.Fatalf("Error reading file: %v", err)
					}
				} else {
					if _, err := fsFile.Seek(tt.offset, io.SeekStart); err != nil {
						t.Fatalf("Error seeking file: %v", err)
					}
					b = make([]byte, tt.size)
					var n int
					n, err = fsFile.Read(b)
					if n != len(b) {
						t.Fatalf("short read, expected %d bytes got %d", len(b), n)
					}
				}
				if err != nil && !errors.Is(err, io.EOF) {
					t.Fatalf("Error reading file: %v", err)
				}
				if !bytes.Equal(b, tt.expected) {
					t.Errorf("file data mismatch")
				}
			}
		})
	}
}
