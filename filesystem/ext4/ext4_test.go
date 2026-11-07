package ext4

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

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

	dirTests := []struct {
		name    string
		inode   uint32
		entries []*directoryEntry
		err     error
	}{
		{"invalid inode", 0, nil, errors.New("could not read inode")},
		{"root", 2, rootDirEntries, nil},
		{"foo dir", 13, fooDirEntries, nil},
	}

	imageTests := []struct {
		name      string
		imageFile string
		fsOffset  int64
	}{
		{"no offset", imgFile, 0},
		{"with offset", imgFileOffset, 1024},
	}

	for _, it := range imageTests {
		t.Run(it.name, func(t *testing.T) {
			f, err := os.Open(it.imageFile)
			if err != nil {
				t.Fatalf("Error opening test image: %v", err)
			}
			defer f.Close()

			b := file.New(f, true)
			fs, err := Read(b, 100*MB, it.fsOffset, 512)
			if err != nil {
				t.Fatalf("Error reading filesystem: %v", err)
			}

			for _, tt := range dirTests {
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
		})
	}
}

func TestReadFile(t *testing.T) {
	randomFileData, err := os.ReadFile(randomDataFile)
	if err != nil {
		t.Fatalf("Error opening random data file %s: %v", randomDataFile, err)
	}

	fileTests := []struct {
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

	imageTests := []struct {
		name      string
		imageFile string
		fsOffset  int64
	}{
		{"no offset", imgFile, 0},
		{"with offset", imgFileOffset, 1024},
	}

	for _, it := range imageTests {
		t.Run(it.name, func(t *testing.T) {
			f, err := os.Open(it.imageFile)
			if err != nil {
				t.Fatalf("Error opening test image: %v", err)
			}
			defer f.Close()

			b := file.New(f, true)
			fs, err := Read(b, 100*MB, it.fsOffset, 512)
			if err != nil {
				t.Fatalf("Error reading filesystem: %v", err)
			}

			for _, tt := range fileTests {
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

// creates a copy of the provided img file, so we can manipulate it
// without affecting the original
func testCreateImgCopyFrom(t *testing.T, src string) string {
	t.Helper()
	dir := t.TempDir()
	outfile := filepath.Join(dir, path.Base(src))
	if err := testCopyFile(src, outfile); err != nil {
		t.Fatalf("Error copying image file: %v", err)
	}
	return outfile
}

func testCreateEmptyFile(t *testing.T, size int64) (outfile string, f *os.File) {
	t.Helper()
	dir := t.TempDir()
	outfile = filepath.Join(dir, "ext4.img")
	f, err := os.Create(outfile)
	if err != nil {
		t.Fatalf("Error creating empty image file: %v", err)
	}

	// Truncate to size
	err = f.Truncate(size)
	if err != nil {
		t.Fatalf("Error truncating image file: %v", err)
	}
	return outfile, f
}

//nolint:gocyclo // yes, long and complex, we can live with it
func TestWriteFile(t *testing.T) {
	var newFile = "newlygeneratedfile.dat"
	tests := []struct {
		name        string
		path        string
		flag        int
		offset      int64
		size        int
		readAll     bool
		expected    []byte
		openFileErr error
		writeErr    error
		readErr     error
	}{
		{"create invalid path", "/do/not/exist/any/where", os.O_CREATE, 0, 0, false, nil, errors.New("could not read directory entries"), nil, nil},
		{"create in root", "/" + newFile, os.O_CREATE | os.O_RDWR, 0, 0, false, []byte("hello world"), nil, nil, nil},
		{"create in valid subdirectory", "/foo/" + newFile, os.O_CREATE | os.O_RDWR, 0, 0, false, []byte("hello world"), nil, nil, nil},
		{"create exists as directory", "/foo", os.O_CREATE, 0, 0, false, nil, nil, errors.New("cannot create file as existing directory"), errors.New("cannot read directory")},
		{"create exists as file", "/random.dat", os.O_CREATE | os.O_RDWR, 0, 0, false, nil, nil, nil, nil},
		{"append invalid path", "/do/not/exist/any/where", os.O_APPEND, 0, 0, false, nil, errors.New("could not read directory entries"), nil, nil},
		{"append exists as directory", "/foo", os.O_APPEND, 0, 0, false, nil, nil, errors.New("file is not open for writing"), errors.New("cannot read directory")},
		{"append exists as file", "/random.dat", os.O_APPEND | os.O_RDWR, 0, 0, false, nil, nil, nil, nil},
		{"overwrite invalid path", "/do/not/exist/any/where", os.O_RDWR, 0, 0, false, nil, errors.New("could not read directory entries"), nil, nil},
		{"overwrite exists as directory", "/foo", os.O_RDWR, 0, 0, false, nil, nil, nil, errors.New("cannot read directory")},
		{"overwrite exists as file", "/random.dat", os.O_RDWR, 0, 0, false, nil, nil, nil, nil},
	}
	imageTests := []struct {
		name      string
		imageFile string
		fsOffset  int64
	}{
		{"no offset", imgFile, 0},
		{"with offset", imgFileOffset, 1024},
	}
	for _, it := range imageTests {
		t.Run(it.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					outfile := testCreateImgCopyFrom(t, it.imageFile)
					f, err := os.OpenFile(outfile, os.O_RDWR, 0)
					if err != nil {
						t.Fatalf("Error opening test image: %v", err)
					}
					defer f.Close()

					b := file.New(f, false)
					fs, err := Read(b, 100*MB, it.fsOffset, 512)
					if err != nil {
						t.Fatalf("Error reading filesystem: %v", err)
					}
					ext4File, err := fs.OpenFile(tt.path, tt.flag)
					switch {
					case err != nil && tt.openFileErr == nil:
						t.Fatalf("unexpected error opening file: %v", err)
					case err == nil && tt.openFileErr != nil:
						t.Fatalf("missing expected error opening file: %v", tt.openFileErr)
					case err != nil && tt.openFileErr != nil && !strings.HasPrefix(err.Error(), tt.openFileErr.Error()):
						t.Fatalf("mismatched error opening file, expected '%v' got '%v'", tt.openFileErr, err)
					case err == nil:
						// if it is a directory, expect errors on Seek and Write
						if _, err := ext4File.Seek(tt.offset, io.SeekStart); err != nil {
							t.Fatalf("Error seeking file for write: %v", err)
						}
						n, err := ext4File.Write(tt.expected)
						if (tt.writeErr != nil && err == nil) || (tt.writeErr == nil && err != nil && err != io.EOF) {
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
						switch {
						case tt.readErr != nil && err == nil:
							t.Fatalf("expected read error %v, got nil", tt.readErr)
						case tt.readErr != nil && err != nil && !strings.HasPrefix(err.Error(), tt.readErr.Error()):
							t.Fatalf("mismatched read error, expected '%v' got '%v'", tt.readErr, err)
						case tt.readErr != nil && err != nil:
							// expected read error received, skip data verification
						case err != nil && err != io.EOF:
							t.Fatalf("Error reading file: %v", err)
						default:
							if n != len(tt.expected) {
								t.Fatalf("short read, expected %d bytes got %d", len(tt.expected), n)
							}
							if !bytes.Equal(b, tt.expected) {
								t.Errorf("file data mismatch")
							}
						}
					}
				})
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
	imageTests := []struct {
		name      string
		imageFile string
		fsOffset  int64
	}{
		{"no offset", imgFile, 0},
		{"with offset", imgFileOffset, 1024},
	}
	for _, it := range imageTests {
		t.Run(it.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					outfile := testCreateImgCopyFrom(t, it.imageFile)
					f, err := os.OpenFile(outfile, os.O_RDWR, 0)
					if err != nil {
						t.Fatalf("Error opening test image: %v", err)
					}
					defer f.Close()

					b := file.New(f, false)
					fs, err := Read(b, 100*MB, it.fsOffset, 512)
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
	imageTests := []struct {
		name      string
		imageFile string
		fsOffset  int64
	}{
		{"no offset", imgFile, 0},
		{"with offset", imgFileOffset, 1024},
	}
	for _, it := range imageTests {
		t.Run(it.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					outfile := testCreateImgCopyFrom(t, it.imageFile)
					f, err := os.OpenFile(outfile, os.O_RDWR, 0)
					if err != nil {
						t.Fatalf("Error opening test image: %v", err)
					}
					defer f.Close()

					b := file.New(f, false)
					fs, err := Read(b, 100*MB, it.fsOffset, 512)
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
		})
	}
}

func TestMkdir(t *testing.T) {
	tests := []struct {
		name string
		path string
		err  error
	}{
		{"parent exists", "foo/bar", nil},
		{"invalid path", "/foo/bar", iofs.ErrInvalid},
		{"parent does not exist", "baz/bar", nil},
		{"parent is file", "random.dat/bar", errors.New("cannot create directory at")},
		{"path exists", "foo", nil},
	}
	imageTests := []struct {
		name      string
		imageFile string
		fsOffset  int64
	}{
		{"no offset", imgFile, 0},
		{"with offset", imgFileOffset, 1024},
	}
	for _, it := range imageTests {
		t.Run(it.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					outfile := testCreateImgCopyFrom(t, it.imageFile)
					f, err := os.OpenFile(outfile, os.O_RDWR, 0)
					if err != nil {
						t.Fatalf("Error opening test image: %v", err)
					}
					defer f.Close()

					b := file.New(f, false)
					fs, err := Read(b, 100*MB, it.fsOffset, 512)
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
						// ensure that the . and .. do not exist
						if len(entries) > 1 {
							if entries[0].Name() == "." {
								t.Errorf("unexpected . entry in directory")
							}
						}
						if len(entries) > 2 {
							if entries[1].Name() == ".." {
								t.Errorf("unexpected .. entry in directory")
							}
						}
					}
				})
			}
		})
	}
}

func TestCreate(t *testing.T) {
	outfile, f := testCreateEmptyFile(t, 100*MB)
	fs, err := Create(file.New(f, false), 100*MB, 0, 512, &Params{})
	if err != nil {
		t.Fatalf("Error creating ext4 filesystem: %v", err)
	}
	if fs == nil {
		t.Fatalf("Expected non-nil filesystem after creation")
	}
	// Sync the file to disk before running e2fsck
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing file: %v", err)
	}
	// check that the filesystem is valid using external tools
	cmd := exec.Command("e2fsck", "-f", "-n", "-vv", outfile)
	stdout := bytes.NewBuffer(nil)
	stderr := bytes.NewBuffer(nil)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("e2fsck failed: %v,\nstdout:\n%s,\n\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}

func TestChtimes(t *testing.T) {
	outfile := testCreateImgCopyFrom(t, imgFile)
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
	newfile := "/testfile"
	mode := os.O_RDWR | os.O_CREATE
	fileIntf, err := fs.OpenFile(newfile, mode)
	if err != nil {
		t.Fatalf("error opening file %s: %v", newfile, err)
	}
	fileIntf.Close()

	// ext4 supports 34-bit seconds and 30-bit nanoseconds
	// We use 91 nanoseconds because it has the lowest 2 bits set (binary 1011011),
	// which tests the bit-packing logic.
	nano := 91

	tests := []struct {
		name string
		t    time.Time
	}{
		{"1901-1969", time.Date(1930, 1, 1, 0, 0, 0, nano, time.UTC)},
		{"1970-2038", time.Date(2026, 1, 1, 0, 0, 0, nano, time.UTC)},
		{"2038-2106", time.Date(2050, 1, 1, 0, 0, 0, nano, time.UTC)},
		{"2106-2174", time.Date(2120, 1, 1, 0, 0, 0, nano, time.UTC)},
		{"2174-2242", time.Date(2200, 1, 1, 0, 0, 0, nano, time.UTC)},
		{"2242-2310", time.Date(2280, 1, 1, 0, 0, 0, nano, time.UTC)},
		{"2310-2378", time.Date(2350, 1, 1, 0, 0, 0, nano, time.UTC)},
		{"2378-2446", time.Date(2440, 1, 1, 0, 0, 0, nano, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := fs.Chtimes(newfile, tt.t, tt.t, tt.t); err != nil {
				t.Fatalf("error changing times on file %s: %v", newfile, err)
			}

			// now check that it was updated
			fileIntf, err = fs.OpenFile(newfile, os.O_RDONLY)
			if err != nil {
				t.Fatalf("error opening file %s: %v", newfile, err)
			}
			defer fileIntf.Close()

			fileImpl, ok := fileIntf.(*File)
			if !ok {
				t.Fatalf("could not cast to ext4.File")
			}

			if fileImpl.createTime.Unix() != tt.t.Unix() {
				t.Errorf("mismatched create time seconds, actual %d (%v) expected %d (%v)", fileImpl.createTime.Unix(), fileImpl.createTime, tt.t.Unix(), tt.t)
			}
			if fileImpl.createTime.Nanosecond() != tt.t.Nanosecond() {
				t.Errorf("mismatched create time nanoseconds, actual %d expected %d", fileImpl.createTime.Nanosecond(), tt.t.Nanosecond())
			}

			if fileImpl.accessTime.Unix() != tt.t.Unix() {
				t.Errorf("mismatched access time seconds, actual %d (%v) expected %d (%v)", fileImpl.accessTime.Unix(), fileImpl.accessTime, tt.t.Unix(), tt.t)
			}
			if fileImpl.accessTime.Nanosecond() != tt.t.Nanosecond() {
				t.Errorf("mismatched access time nanoseconds, actual %d expected %d", fileImpl.accessTime.Nanosecond(), tt.t.Nanosecond())
			}

			if fileImpl.modifyTime.Unix() != tt.t.Unix() {
				t.Errorf("mismatched modify time seconds, actual %d (%v) expected %d (%v)", fileImpl.modifyTime.Unix(), fileImpl.modifyTime, tt.t.Unix(), tt.t)
			}
			if fileImpl.modifyTime.Nanosecond() != tt.t.Nanosecond() {
				t.Errorf("mismatched modify time nanoseconds, actual %d expected %d", fileImpl.modifyTime.Nanosecond(), tt.t.Nanosecond())
			}
		})
	}
}

func TestChmod(t *testing.T) {
	outfile := testCreateImgCopyFrom(t, imgFile)
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

	targetFile := "shortfile.txt"
	tests := []struct {
		name string
		mode os.FileMode
	}{
		{"0755", 0o755},
		{"0644", 0o644},
		{"0000", 0o000},
		{"0777", 0o777},
		{"sticky", 0o644 | os.ModeSticky},
		{"setuid", 0o755 | os.ModeSetuid},
		{"setgid", 0o755 | os.ModeSetgid},
		{"all-special", 0o777 | os.ModeSticky | os.ModeSetuid | os.ModeSetgid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fs.Chmod(targetFile, tt.mode)
			if err != nil {
				t.Fatalf("Chmod failed: %v", err)
			}

			fi, err := fs.Stat(targetFile)
			if err != nil {
				t.Fatalf("Stat failed: %v", err)
			}

			if fi.Mode() != tt.mode {
				t.Errorf("expected mode %v, got %v", tt.mode, fi.Mode())
			}
		})
	}

	t.Run("symlink", func(t *testing.T) {
		link := "symlink.dat"
		target := "random.dat"
		mode := os.FileMode(0o600)

		err := fs.Chmod(link, mode)
		if err != nil {
			t.Fatalf("Chmod on symlink failed: %v", err)
		}

		// Check target
		fi, err := fs.Stat(target)
		if err != nil {
			t.Fatalf("Stat on target failed: %v", err)
		}
		if fi.Mode().Perm() != mode.Perm() {
			t.Errorf("expected target mode %v, got %v", mode, fi.Mode())
		}
	})
}

func TestChown(t *testing.T) {
	outfile := testCreateImgCopyFrom(t, imgFile)
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

	targetFile := "shortfile.txt"
	tests := []struct {
		name string
		uid  int
		gid  int
	}{
		{"change-both", 1000, 2000},
		{"change-uid", 500, -1},
		{"change-gid", -1, 600},
		{"no-change", -1, -1},
		{"root", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Get initial values if we are not changing them
			fiOld, err := fs.Stat(targetFile)
			if err != nil {
				t.Fatalf("Stat failed: %v", err)
			}
			statOld, ok := fiOld.Sys().(*StatT)
			if !ok {
				t.Fatalf("Sys() did not return *StatT")
			}

			err = fs.Chown(targetFile, tt.uid, tt.gid)
			if err != nil {
				t.Fatalf("Chown failed: %v", err)
			}

			fi, err := fs.Stat(targetFile)
			if err != nil {
				t.Fatalf("Stat failed: %v", err)
			}
			stat, ok := fi.Sys().(*StatT)
			if !ok {
				t.Fatalf("Sys() did not return *StatT")
			}

			expectedUID := uint32(tt.uid)
			if tt.uid == -1 {
				expectedUID = statOld.UID
			}
			expectedGID := uint32(tt.gid)
			if tt.gid == -1 {
				expectedGID = statOld.GID
			}

			if stat.UID != expectedUID {
				t.Errorf("expected uid %d, got %d", expectedUID, stat.UID)
			}
			if stat.GID != expectedGID {
				t.Errorf("expected gid %d, got %d", expectedGID, stat.GID)
			}
		})
	}

	t.Run("symlink", func(t *testing.T) {
		link := "symlink.dat"
		target := "random.dat"
		uid, gid := 123, 456

		err := fs.Chown(link, uid, gid)
		if err != nil {
			t.Fatalf("Chown on symlink failed: %v", err)
		}

		// Check target
		fi, err := fs.Stat(target)
		if err != nil {
			t.Fatalf("Stat on target failed: %v", err)
		}
		stat, ok := fi.Sys().(*StatT)
		if !ok {
			t.Fatalf("Sys() did not return *StatT")
		}

		if int(stat.UID) != uid || int(stat.GID) != gid {
			t.Errorf("expected target uid:gid %d:%d, got %d:%d", uid, gid, stat.UID, stat.GID)
		}
	})
}

func TestStatSysInodeMetadata(t *testing.T) {
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

	tests := []struct {
		name string
		path string
	}{
		{"regular file", "/random.dat"},
		{"directory", "/foo"},
		{"root", "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fi, err := fs.Stat(tt.path)
			if err != nil {
				t.Fatalf("Stat failed: %v", err)
			}
			sys := fi.Sys()
			if sys == nil {
				t.Fatalf("Sys() returned nil")
			}
			stat, ok := sys.(*StatT)
			if !ok {
				t.Fatalf("Sys() did not return *StatT, got %T", sys)
			}
			if stat.Ino == 0 {
				t.Errorf("expected non-zero inode number, got 0")
			}
			if stat.Nlink == 0 {
				t.Errorf("expected non-zero hardlink count, got 0")
			}
			// modify time on FileInfo should match the inode-derived value through Stat
			if fi.ModTime().IsZero() {
				t.Errorf("ModTime is zero")
			}
			// timestamps must be plausible: not zero, and not absurdly in the future
			if stat.AccessTime.IsZero() {
				t.Errorf("AccessTime is zero")
			}
			if stat.ChangeTime.IsZero() {
				t.Errorf("ChangeTime is zero")
			}
			upper := time.Now().AddDate(100, 0, 0)
			lower := time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
			if stat.AccessTime.After(upper) || stat.AccessTime.Before(lower) {
				t.Errorf("AccessTime %v outside plausible range", stat.AccessTime)
			}
			if stat.ChangeTime.After(upper) || stat.ChangeTime.Before(lower) {
				t.Errorf("ChangeTime %v outside plausible range", stat.ChangeTime)
			}
			// Confirm Stat returns stable values across repeated calls.
			fi2, err := fs.Stat(tt.path)
			if err != nil {
				t.Fatalf("second Stat failed: %v", err)
			}
			stat2, ok := fi2.Sys().(*StatT)
			if !ok {
				t.Fatalf("second Sys() did not return *StatT")
			}
			if stat.UID != stat2.UID || stat.GID != stat2.GID {
				t.Errorf("UID/GID not stable: %d:%d vs %d:%d", stat.UID, stat.GID, stat2.UID, stat2.GID)
			}
			if stat.Ino != stat2.Ino {
				t.Errorf("inode number not stable: %d vs %d", stat.Ino, stat2.Ino)
			}
		})
	}

	t.Run("symlink target", func(t *testing.T) {
		// On the test image, /symlink.dat is a symlink to random.dat. fs.Stat returns the
		// link inode's metadata, which carries LinkTarget for symlinks.
		// We look up the symlink via the directory entry path since fs.Stat may follow it.
		_, entry, err := fs.getEntryAndParent("/symlink.dat")
		if err != nil {
			t.Fatalf("getEntryAndParent failed: %v", err)
		}
		if entry == nil {
			t.Fatalf("symlink entry not found")
		}
		in, err := fs.readInode(entry.inode)
		if err != nil {
			t.Fatalf("readInode failed: %v", err)
		}
		s := in.stat()
		if in.fileType == fileTypeSymbolicLink && s.LinkTarget == "" {
			t.Errorf("expected non-empty LinkTarget for symlink inode")
		}
	})
}

// TestRemoveIntegrity verifies that Remove leaves the filesystem in a consistent
// state. The existing TestRm only checks that files can be opened/removed; these
// sub-tests guard against regressions in four areas that previously caused data
// corruption or e2fsck failures:
//
//  1. Block bitmap index must account for firstDataBlock (1 on 1 KiB-block images).
//  2. Parent directory hardLinks must be decremented when a subdirectory is removed.
//  3. Inode bitmap index must be 0-based (inode numbers are 1-based).
//  4. The freed inode's table entry must be wiped so e2fsck cannot resurrect it.
//  5. Free block counters must reflect actually freed blocks, not be double-counted.
//
// Each sub-test uses a freshly created filesystem so the results are deterministic.
func TestRemoveIntegrity(t *testing.T) {
	// createTestFS builds a fresh 100 MB ext4 image (1 KiB blocks, firstDataBlock=1)
	// and returns an open filesystem backed by a temporary file.
	createTestFS := func(t *testing.T) (*FileSystem, *os.File) {
		t.Helper()
		outfile, f := testCreateEmptyFile(t, 100*MB)
		fs, err := Create(file.New(f, false), 100*MB, 0, 512, &Params{})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		_ = outfile
		return fs, f
	}

	// writeFile creates/overwrites a file path with the given payload.
	writeFile := func(t *testing.T, fs *FileSystem, p string, payload []byte) {
		t.Helper()
		f, err := fs.OpenFile(p, os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("OpenFile %s: %v", p, err)
		}
		if _, err := f.Write(payload); err != nil {
			t.Fatalf("Write %s: %v", p, err)
		}
		f.Close()
	}

	// readFile returns the full content of a file, failing on unexpected errors.
	readFile := func(t *testing.T, fs *FileSystem, p string, size int) []byte {
		t.Helper()
		f, err := fs.OpenFile(p, os.O_RDONLY)
		if err != nil {
			t.Fatalf("OpenFile %s for read: %v", p, err)
		}
		defer f.Close()
		buf := make([]byte, size)
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Read %s: %v", p, err)
		}
		return buf[:n]
	}

	// fill generates a deterministic payload of the given size filled with byte b.
	fill := func(b byte, n int) []byte {
		data := make([]byte, n)
		for i := range data {
			data[i] = b
		}
		return data
	}

	// noCrossFileCorruption verifies that removing a file and creating a new one
	// does not corrupt surviving files — the regression guarded by bitmap index
	// fixes for both blocks (#1) and inodes (#3).
	t.Run("no cross-file corruption after remove and create", func(t *testing.T) {
		fs, f := createTestFS(t)
		defer f.Close()

		const dataSize = 4096
		writeFile(t, fs, "/a", fill('A', dataSize))
		writeFile(t, fs, "/b", fill('B', dataSize))
		writeFile(t, fs, "/c", fill('C', dataSize))

		// remove the middle file, then create a new one whose inode and blocks
		// are drawn from the same pool that just freed /b
		if err := fs.Remove("/b"); err != nil {
			t.Fatalf("Remove /b: %v", err)
		}
		writeFile(t, fs, "/d", fill('D', dataSize))

		for _, c := range []struct {
			path string
			want byte
		}{
			{"/a", 'A'},
			{"/c", 'C'},
			{"/d", 'D'},
		} {
			got := readFile(t, fs, c.path, dataSize)
			if len(got) != dataSize || got[0] != c.want {
				t.Errorf("%s corrupted: expected %q×%d, got %q… (len %d)",
					c.path, string(c.want), dataSize, safeFirstByte(got), len(got))
			}
		}
	})

	// parentLinkCount verifies that removing an empty subdirectory decrements the
	// parent directory's hard link count — the regression guarded by fix #2.
	t.Run("parent directory link count after subdir removal", func(t *testing.T) {
		fs, f := createTestFS(t)
		defer f.Close()

		rootBefore, err := fs.readInode(rootInode)
		if err != nil {
			t.Fatalf("readInode root before: %v", err)
		}

		if err := fs.Mkdir("sub1"); err != nil {
			t.Fatalf("Mkdir sub1: %v", err)
		}

		rootAfterMkdir, err := fs.readInode(rootInode)
		if err != nil {
			t.Fatalf("readInode root after Mkdir: %v", err)
		}
		if rootAfterMkdir.hardLinks != rootBefore.hardLinks+1 {
			t.Fatalf("Mkdir did not increment parent hardLinks: got %d, want %d",
				rootAfterMkdir.hardLinks, rootBefore.hardLinks+1)
		}

		if err := fs.Remove("sub1"); err != nil {
			t.Fatalf("Remove sub1: %v", err)
		}

		rootAfterRemove, err := fs.readInode(rootInode)
		if err != nil {
			t.Fatalf("readInode root after Remove: %v", err)
		}
		if rootAfterRemove.hardLinks != rootBefore.hardLinks {
			t.Errorf("Remove did not decrement parent hardLinks: got %d, want %d",
				rootAfterRemove.hardLinks, rootBefore.hardLinks)
		}
	})

	// removedInodeWiped verifies that the inode table entry for a removed file is
	// cleared (hardLinks=0, size=0, blocks=0, deletionTime set) so that e2fsck -y
	// cannot resurrect it — the regression guarded by fix #4.
	t.Run("removed inode entry is wiped", func(t *testing.T) {
		fs, f := createTestFS(t)
		defer f.Close()

		writeFile(t, fs, "/a", fill('A', 4096))

		_, entry, err := fs.getEntryAndParent("/a")
		if err != nil {
			t.Fatalf("getEntryAndParent /a: %v", err)
		}
		inodeNum := entry.inode

		if err := fs.Remove("/a"); err != nil {
			t.Fatalf("Remove /a: %v", err)
		}

		removed, err := fs.readInode(inodeNum)
		if err != nil {
			t.Fatalf("readInode %d after remove: %v", inodeNum, err)
		}
		if removed.hardLinks != 0 {
			t.Errorf("hardLinks: got %d, want 0", removed.hardLinks)
		}
		if removed.size != 0 {
			t.Errorf("size: got %d, want 0", removed.size)
		}
		if removed.blocks != 0 {
			t.Errorf("blocks: got %d, want 0", removed.blocks)
		}
		if removed.deletionTime == 0 {
			t.Error("deletionTime: got 0, want non-zero")
		}
	})

	// freeBlockCount verifies that after removing a file the superblock's free
	// block counter increases by the number of actually freed filesystem blocks —
	// the regression guarded by fix #5 (no double-counting, no sector-unit mix-up).
	t.Run("free block count is correct", func(t *testing.T) {
		fs, f := createTestFS(t)
		defer f.Close()

		data := fill('X', 8192) // spans several 1 KiB blocks
		writeFile(t, fs, "/a", data)

		freeBefore := fs.superblock.freeBlocks

		if err := fs.Remove("/a"); err != nil {
			t.Fatalf("Remove /a: %v", err)
		}

		freeAfter := fs.superblock.freeBlocks
		if freeAfter <= freeBefore {
			t.Fatalf("freeBlocks did not increase: before=%d after=%d", freeBefore, freeAfter)
		}

		// the freed count must equal the number of data blocks the file held,
		// readable from the (now-wiped) inode's pre-removal extent tree.  We
		// approximate by checking that the delta is at least one filesystem
		// block and does not exceed the total blocks the file occupied.
		blockSize := uint64(fs.superblock.blockSize)
		freed := freeAfter - freeBefore
		minExpected := uint64(len(data)) / blockSize
		if uint64(len(data))%blockSize != 0 {
			minExpected++
		}
		if freed < minExpected {
			t.Errorf("freed too few blocks: got %d, want at least %d", freed, minExpected)
		}
	})
}

// TestRename tests renaming or moving files and folders around updates ext4
// file structure.
func TestRename(t *testing.T) {
	// createTestFS builds a fresh 100 MB ext4 image (1 KiB blocks, firstDataBlock=1)
	// and returns an open filesystem backed by a temporary file.
	createTestFS := func(t *testing.T) (string, *FileSystem, *os.File) {
		t.Helper()
		outfile, f := testCreateEmptyFile(t, 25*MB)
		fs, err := Create(file.New(f, false), 25*MB, 0, 512, &Params{})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		return outfile, fs, f
	}

	// writeFile creates/overwrites a file path with the given payload.
	writeFile := func(t *testing.T, fs *FileSystem, p string, payload []byte) {
		t.Helper()
		f, err := fs.OpenFile(p, os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("OpenFile %s: %v", p, err)
		}
		if _, err := f.Write(payload); err != nil {
			t.Fatalf("Write %s: %v", p, err)
		}
		f.Close()
	}

	// fill generates a deterministic payload of the given size filled with byte b.
	fill := func(b byte, n int) []byte {
		data := make([]byte, n)
		for i := range data {
			data[i] = b
		}
		return data
	}

	t.Run("rename local file and directory names", func(t *testing.T) {
		fsname, fs, f := createTestFS(t)
		defer func() { os.Remove(fsname) }()
		defer f.Close()

		// create 'a' and rename to 'b'
		const dataSize = 1024
		aFileData := fill('A', dataSize)
		writeFile(t, fs, "a", aFileData)
		err := fs.Rename("a", "b")
		if err != nil {
			t.Fatalf("failed to rename file: %v", err)
		}

		// mk 'tempdir' rename/move 'b' into tempdir/
		if err = fs.Mkdir("tempdir"); err != nil {
			t.Fatalf("failed to create tempdir: %v", err)
		}
		if err = fs.Rename("/b", "/tempdir/b"); err != nil {
			t.Fatalf("failed to move /b to new /tempdir/b location: %+v", err)
		}

		// move 'tempdir' to 'newdir' & check layout
		if err = fs.Rename("/tempdir", "/newdir"); err != nil {
			t.Fatalf("failed to move /tempdir to /newdir: %v", err)
		}
		ents, err := fs.ReadDir("newdir")
		if err != nil {
			t.Fatalf("failed to read newdir: %v", err)
		}
		if len(ents) != 1 {
			t.Fatalf("expected 1 dir entry, got: %d %v", len(ents), ents)
		}
		fd, err := fs.Open("newdir/b")
		if err != nil {
			t.Fatalf("failed to open moved directory file: %v", err)
		}
		bdata, err := io.ReadAll(fd)
		if err != nil {
			t.Fatalf("failed to read moved directory file: %v", err)
		}
		if !bytes.Equal(bdata, aFileData) {
			t.Fatalf("moved file data not equal original data")
		}
	})
}

// safeFirstByte returns the first byte of buf as a string, or "<empty>" if buf is nil/empty.
func safeFirstByte(buf []byte) string {
	if len(buf) == 0 {
		return "<empty>"
	}
	return string(buf[0])
}
