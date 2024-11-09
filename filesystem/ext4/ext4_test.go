package ext4

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
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

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
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

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
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

// copy infile to outfile
func testCopyFile(infile, outfile string) error {
	in, err := os.Open(infile)
	if err != nil {
		return fmt.Errorf("Error opening input file: %w", err)
	}
	defer in.Close()
	out, err := os.Create(outfile)
	if err != nil {
		return fmt.Errorf("Error opening output file: %w", err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("Error copying file contents: %w", err)
	}
	return nil
}

// creates a copy of the ready-to-run ext4 img file, so we can manipulate it as desired
// without affecting the original
func testCreateImgCopy(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	outfile := filepath.Join(dir, path.Base(imgFile))
	if err := testCopyFile(imgFile, outfile); err != nil {
		t.Fatalf("Error copying image file: %v", err)
	}
	return outfile
}

func TestWriteFile(t *testing.T) {
	var newFile = "newlygeneratedfile.dat"
	tests := []struct {
		name     string
		path     string
		flag     int
		offset   int64
		size     int
		readAll  bool
		expected []byte
		err      error
	}{
		{"create invalid path", "/do/not/exist/any/where", os.O_CREATE, 0, 0, false, nil, errors.New("could not read directory entries")},
		{"create in root", "/" + newFile, os.O_CREATE | os.O_RDWR, 0, 0, false, []byte("hello world"), nil},
		{"create in valid subdirectory", "/foo/" + newFile, os.O_CREATE | os.O_RDWR, 0, 0, false, []byte("hello world"), nil},
		{"create exists as directory", "/foo", os.O_CREATE, 0, 0, false, nil, errors.New("cannot open directory /foo as file")},
		{"create exists as file", "/random.dat", os.O_CREATE | os.O_RDWR, 0, 0, false, nil, nil},
		{"append invalid path", "/do/not/exist/any/where", os.O_APPEND, 0, 0, false, nil, errors.New("could not read directory entries")},
		{"append exists as directory", "/foo", os.O_APPEND, 0, 0, false, nil, errors.New("cannot open directory /foo as file")},
		{"append exists as file", "/random.dat", os.O_APPEND | os.O_RDWR, 0, 0, false, nil, nil},
		{"overwrite invalid path", "/do/not/exist/any/where", os.O_RDWR, 0, 0, false, nil, errors.New("could not read directory entries")},
		{"overwrite exists as directory", "/foo", os.O_RDWR, 0, 0, false, nil, errors.New("cannot open directory /foo as file")},
		{"overwrite exists as file", "/random.dat", os.O_RDWR, 0, 0, false, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outfile := testCreateImgCopy(t)
			f, err := os.OpenFile(outfile, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("Error opening test image: %v", err)
			}
			defer f.Close()

			b := file.New(f, false)
			fs, err := Read(b, 100*MB, 0, 512)
			if err != nil {
				t.Fatalf("Error reading filesystem: %v", err)
			}
			ext4File, err := fs.OpenFile(tt.path, tt.flag)
			switch {
			case err != nil && tt.err == nil:
				t.Fatalf("unexpected error opening file: %v", err)
			case err == nil && tt.err != nil:
				t.Fatalf("missing expected error opening file: %v", tt.err)
			case err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error()):
				t.Fatalf("mismatched error opening file, expected '%v' got '%v'", tt.err, err)
			case err == nil:
				if _, err := ext4File.Seek(tt.offset, io.SeekStart); err != nil {
					t.Fatalf("Error seeking file for write: %v", err)
				}
				n, err := ext4File.Write(tt.expected)
				if err != nil && err != io.EOF {
					t.Fatalf("Error writing file: %v", err)
				}
				if n != len(tt.expected) {
					t.Fatalf("short write, expected %d bytes got %d", len(tt.expected), n)
				}
				// now read from the file and see that it matches what we wrote
				if _, err := ext4File.Seek(tt.offset, io.SeekStart); err != nil {
					t.Fatalf("Error seeking file for read: %v", err)
				}
				b := make([]byte, len(tt.expected))
				n, err = ext4File.Read(b)
				if err != nil && err != io.EOF {
					t.Fatalf("Error reading file: %v", err)
				}
				if n != len(tt.expected) {
					t.Fatalf("short read, expected %d bytes got %d", len(tt.expected), n)
				}
				if !bytes.Equal(b, tt.expected) {
					t.Errorf("file data mismatch")
				}
			}
		})
	}
}

func TestRm(t *testing.T) {
	tests := []struct {
		name string
		path string
		err  error
	}{
		{"invalid path", "/do/not/exist/any/where", errors.New("could not read directory entries")},
		{"root dir", "/", errors.New("cannot remove root directory")},
		{"root file", "/random.dat", nil},
		{"subdir file", "/foo/subdirfile.txt", nil},
		{"nonexistent file", "/foo/nonexistent.dat", errors.New("file does not exist")},
		{"non-empty dir", "/foo", errors.New("directory not empty")},
		{"empty dir", "/foo/dir1", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outfile := testCreateImgCopy(t)
			f, err := os.OpenFile(outfile, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("Error opening test image: %v", err)
			}
			defer f.Close()

			b := file.New(f, false)
			fs, err := Read(b, 100*MB, 0, 512)
			if err != nil {
				t.Fatalf("Error reading filesystem: %v", err)
			}
			err = fs.Rm(tt.path)
			switch {
			case err != nil && tt.err == nil:
				t.Fatalf("unexpected error removing file: %v", err)
			case err == nil && tt.err != nil:
				t.Fatalf("missing expected error removing file: %v", tt.err)
			case err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error()):
				t.Fatalf("mismatched error removing file, expected '%v' got '%v'", tt.err, err)
			case err == nil:
				// make sure the file no longer exists
				_, err := fs.OpenFile(tt.path, 0)
				if err == nil {
					t.Fatalf("expected error opening file after removal")
				}
			}
		})
	}
}

func TestTruncateFile(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		exists bool // if the path is supposed to exist before or not
		err    error
	}{
		{"invalid path", "/do/not/exist/any/where", false, errors.New("could not read directory entries")},
		{"root dir", "/", true, errors.New("cannot truncate directory")},
		{"sub dir", "/foo", true, errors.New("cannot truncate directory")},
		{"valid file", "/random.dat", true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outfile := testCreateImgCopy(t)
			f, err := os.OpenFile(outfile, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("Error opening test image: %v", err)
			}
			defer f.Close()

			b := file.New(f, false)
			fs, err := Read(b, 100*MB, 0, 512)
			if err != nil {
				t.Fatalf("Error reading filesystem: %v", err)
			}
			// get the original size of the file
			var origSize int64
			if tt.exists {
				fi, err := fs.Stat(tt.path)
				if err != nil {
					t.Fatalf("Error getting file info before truncate: %v", err)
				}
				origSize = fi.Size()
			}

			// truncate the file to a random number of bytes
			targetSize := int64(1000)
			if origSize == targetSize {
				targetSize = 2000
			}
			err = fs.Truncate(tt.path, targetSize)
			switch {
			case err != nil && tt.err == nil:
				t.Fatalf("unexpected error truncating file: %v", err)
			case err == nil && tt.err != nil:
				t.Fatalf("missing expected error truncating file: %v", tt.err)
			case err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error()):
				t.Fatalf("mismatched error truncating file, expected '%v' got '%v'", tt.err, err)
			case err == nil:
				// make sure the file size is now the target size
				fi, err := fs.Stat(tt.path)
				if err != nil {
					t.Fatalf("Error getting file info after truncate: %v", err)
				}
				if fi.Size() != targetSize {
					t.Errorf("expected file size to be %d, got %d", targetSize, fi.Size())
				}
			}
		})
	}
}

func TestMkdir(t *testing.T) {
	tests := []struct {
		name string
		path string
		err  error
	}{
		{"parent exists", "/foo/bar", nil},
		{"parent does not exist", "/baz/bar", nil},
		{"parent is file", "/random.dat/bar", errors.New("cannot create directory at")},
		{"path exists", "/foo", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outfile := testCreateImgCopy(t)
			f, err := os.OpenFile(outfile, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("Error opening test image: %v", err)
			}
			defer f.Close()

			b := file.New(f, false)
			fs, err := Read(b, 100*MB, 0, 512)
			if err != nil {
				t.Fatalf("Error reading filesystem: %v", err)
			}
			err = fs.Mkdir(tt.path)
			switch {
			case err != nil && tt.err == nil:
				t.Fatalf("unexpected error creating directory: %v", err)
			case err == nil && tt.err != nil:
				t.Fatalf("missing expected error creating directory: %v", tt.err)
			case err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error()):
				t.Fatalf("mismatched error creating directory, expected '%v' got '%v'", tt.err, err)
			case err == nil:
				// make sure the directory exists
				entries, err := fs.ReadDir(tt.path)
				if err != nil {
					t.Fatalf("Error reading directory: %v", err)
				}
				if len(entries) < 2 {
					t.Fatalf("expected at least 2 entries in directory, for . and .. , got %d", len(entries))
				}
				if entries[0].Name() != "." {
					t.Errorf("expected . entry in directory")
				}
				if entries[1].Name() != ".." {
					t.Errorf("expected .. entry in directory")
				}
				if !entries[0].IsDir() {
					t.Errorf("expected . entry to be a directory")
				}
				if !entries[1].IsDir() {
					t.Errorf("expected .. entry to be a directory")
				}
			}
		})
	}
}
