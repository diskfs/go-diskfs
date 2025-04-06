package squashfs_test

import (
	"bufio"
	"crypto/md5" //nolint:gosec // MD5 is still fine for detecting file corruptions
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	stdfs "io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
	"github.com/diskfs/go-diskfs/testhelper"
)

func getOpenMode(mode int) string {
	modes := make([]string, 0)
	if mode&os.O_CREATE == os.O_CREATE {
		modes = append(modes, "CREATE")
	}
	if mode&os.O_APPEND == os.O_APPEND {
		modes = append(modes, "APPEND")
	}
	if mode&os.O_RDWR == os.O_RDWR {
		modes = append(modes, "RDWR")
	} else {
		modes = append(modes, "RDONLY")
	}
	return strings.Join(modes, "|")
}

func tmpSquashfsFile() (*os.File, error) {
	filename := "squashfs_test.sqs"
	f, err := os.CreateTemp("", filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to create tempfile %s :%v", filename, err)
	}
	return f, nil
}

func getValidSquashfsFSWorkspace() (*squashfs.FileSystem, error) {
	// create the filesystem
	f, err := tmpSquashfsFile()
	if err != nil {
		return nil, fmt.Errorf("Failed to create squashfs tmpfile: %v", err)
	}

	b := file.New(f, false)
	return squashfs.Create(b, 0, 0, 4096)
}

func getValidSquashfsFSReadOnly() (*squashfs.FileSystem, error) {
	f, err := os.Open(squashfs.Squashfsfile)
	if err != nil {
		return nil, fmt.Errorf("Failed to read squashfs testfile %s: %v", squashfs.Squashfsfile, err)
	}

	b := file.New(f, true)
	return squashfs.Read(b, 0, 0, 4096)
}

func TestSquashfsType(t *testing.T) {
	fs := &squashfs.FileSystem{}
	fstype := fs.Type()
	expected := filesystem.TypeSquashfs
	if fstype != expected {
		t.Errorf("Type() returns %v instead of expected %v", fstype, expected)
	}
}

func TestSquashfsSetCacheSize(t *testing.T) {
	fs, err := getValidSquashfsFSReadOnly()
	if err != nil {
		t.Fatalf("Failed to get read-only squashfs filesystem: %v", err)
	}
	assertCacheSize := func(want int) {
		got := fs.GetCacheSize()
		if want != got {
			t.Errorf("Want cache size %d but got %d", want, got)
		}
	}
	// Check we can set the Cache size for a Read FileSystem
	assertCacheSize(128 * 1024 * 1024)
	fs.SetCacheSize(1024 * 1024)
	assertCacheSize(1024 * 1024)
	fs.SetCacheSize(0)
	fs.SetCacheSize(-1)
	assertCacheSize(0)
	// Check we can set the Cache size for a Write FileSystem
	fs = &squashfs.FileSystem{}
	assertCacheSize(0)
}

func TestSquashfsMkdir(t *testing.T) {
	t.Run("read-only", func(t *testing.T) {
		fs, err := getValidSquashfsFSReadOnly()
		if err != nil {
			t.Fatalf("Failed to get read-only squashfs filesystem: %v", err)
		}
		err = fs.Mkdir("/abcdef")
		if err == nil {
			t.Errorf("received no error when trying to mkdir read-only filesystem")
		}
	})
	t.Run("workspace", func(t *testing.T) {
		fs, err := getValidSquashfsFSWorkspace()
		if err != nil {
			t.Fatalf("Failed to get workspace: %v", err)
		}
		existPath := "/abc"
		tests := []struct {
			fs   *squashfs.FileSystem
			path string
			err  error
		}{
			{fs, "/abcdef", nil},                          // new one
			{fs, existPath, nil},                          // already exists
			{fs, path.Join(existPath, "bar/def/la"), nil}, // already exists
			{fs, "/a/b/c", nil},                           // already exists
		}

		// for fsw, we want to work at least once with a path that exists
		existPathFull := path.Join(fs.Workspace(), existPath)
		err = os.MkdirAll(existPathFull, 0o755)
		if err != nil {
			t.Fatalf("could not create path %s in workspace as %s: %v", existPath, existPathFull, err)
		}
		for _, tt := range tests {
			fs := tt.fs
			ws := fs.Workspace()
			err := fs.Mkdir(tt.path)
			if (err == nil && tt.err != nil) || (err != nil && err == nil) {
				t.Errorf("unexpected error mismatch. Actual: %v, expected: %v", err, tt.err)
			}
			// did the path exist?
			if ws != "" {
				fullPath := path.Join(ws, tt.path)
				if _, err := os.Stat(fullPath); os.IsNotExist(err) {
					t.Errorf("path did not exist after creation base %s, in workspace %s", tt.path, fullPath)
				}
			}
		}
	})
}

func TestSquashfsReadDir(t *testing.T) {
	type testList struct {
		fs    *squashfs.FileSystem
		path  string
		count int
		first string
		last  string
		err   error
	}
	//nolint:thelper // this is not a helper function
	runTests := func(t *testing.T, tests []testList) {
		for _, tt := range tests {
			fi, err := tt.fs.ReadDir(tt.path)
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil):
				t.Errorf("fs.ReadDir(%s): mismatched errors, actual %v expected %v", tt.path, err, tt.err)
			case len(fi) != tt.count:
				t.Errorf("fs.ReadDir(%s): mismatched directory received %d entries, expected %d", tt.path, len(fi), tt.count)
			case fi != nil && tt.count > 2 && fi[0].Name() != tt.first:
				t.Errorf("fs.ReadDir(%s): mismatched first non-self or parent entry, actual then expected", tt.path)
				t.Logf("%s", fi[0].Name())
				t.Logf("%s", tt.first)
			case fi != nil && tt.count > 0 && fi[len(fi)-1].Name() != tt.last:
				t.Errorf("fs.ReadDir(%s): mismatched last entry, actual then expected", tt.path)
				t.Logf("%s", fi[len(fi)-1].Name())
				t.Logf("%s", tt.last)
			}
		}
	}
	t.Run("read-only", func(t *testing.T) {
		fs, err := getValidSquashfsFSReadOnly()
		if err != nil {
			t.Errorf("Failed to get read-only squashfs filesystem: %v", err)
		}
		runTests(t, []testList{
			{fs, "/abcdef", 0, "", "", fmt.Errorf("directory does not exist")},      // does not exist
			{fs, "/", 9, "README.md", "zero", nil},                                  // exists
			{fs, "/foo", 501, "filename_0", "filename_99", nil},                     // exists
			{fs, "/abc", 0, "", "LARGEFIL", fmt.Errorf("directory does not exist")}, // should not find rock ridge name
		},
		)
	})
	t.Run("workspace", func(t *testing.T) {
		fs, err := getValidSquashfsFSWorkspace()
		if err != nil {
			t.Errorf("Failed to get workspace: %v", err)
		}
		// make sure existPath exists in the workspace
		ws := fs.Workspace()
		existPath := "/abc"
		existPathWs := path.Join(ws, existPath)
		_ = os.MkdirAll(existPathWs, 0o755)
		// create files
		for i := 0; i < 10; i++ {
			filename := path.Join(existPathWs, fmt.Sprintf("filename_%d", i))
			contents := fmt.Sprintf("abcdefg %d", i)
			_ = os.WriteFile(filename, []byte(contents), 0o600)
		}
		runTests(t, []testList{
			{fs, "/abcdef", 0, "", "", fmt.Errorf("directory does not exist")}, // does not exist
			{fs, existPath, 10, "filename_0", "filename_9", nil},               // exists
		},
		)
	})
}

func TestSquashfsOpenFile(t *testing.T) {
	// opening directories and files for reading
	type testStruct struct {
		path     string
		mode     int
		expected string
		err      error
	}

	t.Run("read", func(t *testing.T) {
		//nolint:thelper // this is not a helper function
		runTests := func(t *testing.T, fs *squashfs.FileSystem, tests []testStruct) {
			for _, tt := range tests {
				header := fmt.Sprintf("OpenFile(%s, %s)", tt.path, getOpenMode(tt.mode))
				reader, err := fs.OpenFile(tt.path, tt.mode)
				switch {
				case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
					t.Errorf("%s: mismatched errors, actual: %v , expected: %v", header, err, tt.err)
				case reader == nil && (tt.err == nil || tt.expected != ""):
					t.Errorf("%s: Unexpected nil output", header)
				case reader != nil:
					b, err := io.ReadAll(reader)
					if err != nil {
						t.Errorf("%s: io.ReadAll(reader) unexpected error: %v", header, err)
					}
					// limit size
					if len(b) > 1024 {
						b = b[:1024]
					}
					expected := []byte(tt.expected)
					if len(expected) > 1024 {
						expected = expected[:1024]
					}
					diff, diffString := testhelper.DumpByteSlicesWithDiffs(b, expected, 32, false, true, true)
					if diff {
						t.Errorf("groupdescriptor.toBytes() mismatched, actual then expected\n%s", diffString)
					}
				}
			}
		}
		t.Run("read-only", func(t *testing.T) {
			fs, err := getValidSquashfsFSReadOnly()
			if err != nil {
				t.Errorf("Failed to get read-only squashfs filesystem: %v", err)
			}
			var zeros1024 [1024]byte
			tests := []testStruct{
				// error opening a directory
				{"/", os.O_RDONLY, "", fmt.Errorf("cannot open directory %s as file", "/")},
				{"/abcdefg", os.O_RDONLY, "", fmt.Errorf("target file %s does not exist", "/abcdefg")},
				{"/foo/filename_10", os.O_RDONLY, "filename_10\n", nil},
				{"/foo/filename_75", os.O_RDONLY, "filename_75\n", nil},
				{"/README.MD", os.O_RDONLY, "", fmt.Errorf("target file %s does not exist", "/README.MD")},
				{"/README.md", os.O_RDONLY, "README\n", nil},
				{"/zero/largefile", os.O_RDONLY, string(zeros1024[:]), nil},
			}
			runTests(t, fs, tests)
		})
		t.Run("workspace", func(t *testing.T) {
			fs, err := getValidSquashfsFSWorkspace()
			if err != nil {
				t.Errorf("Failed to get workspace: %v", err)
			}
			// make sure our test files exist and have necessary content
			ws := fs.Workspace()
			subdir := "/FOO"
			_ = os.MkdirAll(path.Join(ws, subdir), 0o755)
			for i := 0; i <= 75; i++ {
				filename := fmt.Sprintf("FILENA%02d", i)
				content := fmt.Sprintf("filename_%d\n", i)
				_ = os.WriteFile(path.Join(ws, subdir, filename), []byte(content), 0o600)
			}
			tests := []testStruct{
				// error opening a directory
				{"/", os.O_RDONLY, "", fmt.Errorf("cannot open directory %s as file", "/")},
				// open non-existent file for read or read write
				{"/abcdefg", os.O_RDONLY, "", fmt.Errorf("target file %s does not exist", "/abcdefg")},
				// open file for read or read write and check contents
				{"/FOO/FILENA01", os.O_RDONLY, "filename_1\n", nil},
				{"/FOO/FILENA75", os.O_RDONLY, "filename_75\n", nil},
			}
			runTests(t, fs, tests)
		})
	})
}

// Test the Open method on the directory entry
func TestSquashfsOpen(t *testing.T) {
	fs, err := getValidSquashfsFSReadOnly()
	if err != nil {
		t.Errorf("Failed to get read-only squashfs filesystem: %v", err)
	}
	fis, err := fs.ReadDir("/")
	if err != nil {
		t.Errorf("Failed to list squashfs filesystem: %v", err)
	}
	var dir = make(map[string]os.FileInfo, len(fis))
	for _, fi := range fis {
		dir[fi.Name()] = fi
	}

	// Check a file
	fi := dir["README.md"]
	fix, ok := fi.Sys().(squashfs.FileStat)
	if !ok {
		t.Fatal("Wrong type")
	}
	fh, err := fix.Open()
	if err != nil {
		t.Errorf("Failed to open file: %v", err)
	}
	gotBuf, err := io.ReadAll(fh)
	if err != nil {
		t.Errorf("Failed to read file: %v", err)
	}
	err = fh.Close()
	if err != nil {
		t.Errorf("Failed to close file: %v", err)
	}
	wantBuf := "README\n"
	if string(gotBuf) != wantBuf {
		t.Errorf("Expecting to read %q from file but read %q", wantBuf, string(gotBuf))
	}

	// Check a directory
	fi = dir["foo"]
	fix, ok = fi.Sys().(squashfs.FileStat)
	if !ok {
		t.Fatal("Wrong type")
	}
	_, err = fix.Open()
	wantErr := fmt.Errorf("inode is of type 8, neither basic nor extended file")
	if err.Error() != wantErr.Error() {
		t.Errorf("Want error %q but got %q", wantErr, err)
	}
}

func TestSquashfsRead(t *testing.T) {
	tests := []struct {
		blocksize  int64
		filesize   int64
		bytechange int64
		fs         *squashfs.FileSystem
		err        error
	}{
		{500, 6000, -1, nil, fmt.Errorf("blocksize %d too small, must be at least %d", 500, 4096)},
		{4097, squashfs.GB * squashfs.GB, -1, nil, fmt.Errorf("blocksize %d is not a power of 2", 4097)},
		{4096, 10000000, -1, &squashfs.FileSystem{}, nil},
	}
	for i, t2 := range tests {
		tt := t2
		t.Run(fmt.Sprintf("blocksize %d filesize %d bytechange %d", tt.blocksize, tt.filesize, tt.bytechange), func(t *testing.T) {
			// get a temporary working file
			f, err := os.Open(squashfs.Squashfsfile)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			b := file.New(f, true)
			// create the filesystem
			fs, err := squashfs.Read(b, tt.filesize, 0, tt.blocksize)
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("%d: Read(%s, %d, %d, %d): mismatched errors, actual %v expected %v", i, f.Name(), tt.filesize, 0, tt.blocksize, err, tt.err)
			case (fs == nil && tt.fs != nil) || (fs != nil && tt.fs == nil):
				t.Errorf("%d: Read(%s, %d, %d, %d): mismatched fs, actual then expected", i, f.Name(), tt.filesize, 0, tt.blocksize)
				t.Logf("%v", fs)
				t.Logf("%v", tt.fs)
			}
			// we do not match the filesystems here, only check functional accuracy
		})
	}
}

// Check the directory listing is correct
func TestSquashfsCheckListing(t *testing.T) {
	// read the directory listing in
	var listing = map[string]struct{}{}
	flist, err := os.Open(squashfs.SquashfsfileListing)
	if err != nil {
		t.Fatal(err)
	}
	defer flist.Close()
	scanner := bufio.NewScanner(flist)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimPrefix(line, ".")
		if line == "/" {
			continue
		}
		listing[line] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	// Open the squash file
	f, err := os.Open(squashfs.Squashfsfile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	b := file.New(f, true)
	// create the filesystem
	fs, err := squashfs.Read(b, fi.Size(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	var list func(dir string)
	list = func(dir string) {
		fis, err := fs.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, fi := range fis {
			p := path.Join(dir, fi.Name())
			if _, found := listing[p]; found {
				delete(listing, p)
			} else {
				t.Errorf("Found unexpected path %q in listing", p)
			}
			if fi.IsDir() {
				list(p)
			}
			// Check the type
			var wantMode = os.FileMode(0)
			if fi.IsDir() {
				wantMode |= os.ModeDir
			}
			var wantTarget string
			switch p {
			case "/goodlink":
				wantTarget = "README.md"
				wantMode |= os.ModeSymlink
			case "/emptylink":
				wantTarget = "/a/b/c/d/ef/g/h"
				wantMode |= os.ModeSymlink
			}
			gotMode := fi.Mode()
			if (gotMode & os.ModeType) != wantMode {
				t.Errorf("%s: want mode 0o%o got mode 0o%o", p, wantMode, gotMode&os.ModeType)
			}
			fix, ok := fi.Sys().(squashfs.FileStat)
			if !ok {
				t.Fatal("Wrong type")
			}
			gotTarget, err := fix.Readlink()
			if wantTarget == "" {
				if err != stdfs.ErrNotExist {
					t.Errorf("%s: ReadLink want error %q got error %q", p, stdfs.ErrNotExist, err)
				}
			} else {
				if err != nil {
					t.Errorf("%s: ReadLink returned error: %v", p, err)
				}
				if wantTarget != gotTarget {
					t.Errorf("%s: ReadLink want target %q got target %q", p, wantTarget, gotTarget)
				}
			}
		}
	}

	list("/")

	// listing should be empty now
	for p := range listing {
		t.Errorf("Didn't find %q in listing", p)
	}
}

// readTest describes a file reading test
type readTest struct {
	name   string
	p      string
	size   int
	md5sum string
}

// Read the file named in the test in with the blocksize passed in and
// check its size and md5sum are OK.
//
// The test returns true if it thinks more tests for this file would
// be counterproductive.
//
//nolint:thelper // this is not a helper function
func testReadFile(t *testing.T, fs *squashfs.FileSystem, test readTest, blockSize int) bool {
	fh, err := fs.OpenFile(test.p, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	var buf []byte

	if blockSize <= 0 {
		buf, err = io.ReadAll(fh)
		if err != nil {
			t.Error(err)
			return true
		}
	} else {
		maxReads := test.size/blockSize + 1
		block := make([]byte, blockSize)
	OUTER:
		for reads := 0; ; reads++ {
			n, err := fh.Read(block)
			buf = append(buf, block[:n]...)
			switch {
			case err == io.EOF:
				break OUTER
			case err != nil:
				t.Error(err)
				return true
			case n != blockSize:
				t.Errorf("expected %d bytes to be read but got %d", blockSize, n)
				return true
			case reads > maxReads:
				t.Errorf("read more than %d blocks - something has gone wrong", maxReads)
				return true
			}
		}
	}

	if len(buf) != test.size {
		t.Errorf("expected %d bytes read but got %d", test.size, len(buf))
		return true
	}

	//nolint:gosec // MD5 is still fine for detecting file corruptions
	md5sumBytes := md5.Sum(buf)
	md5sum := hex.EncodeToString(md5sumBytes[:])
	if md5sum != test.md5sum {
		t.Errorf("expected md5sum %q got %q", test.md5sum, md5sum)

		// Write corrupted file to disk for examination
		f, err := os.CreateTemp("", test.name+"-*.bin")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		_, err = f.Write(buf)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Written corrupted file %q to disk as %q", test.name, f.Name())
		return true
	}
	return false
}

// Check that we can read some specially crafted files
func TestSquashfsReadFile(t *testing.T) {
	var tests []readTest

	// read the check files in creating the tests
	flist, err := os.Open(squashfs.SquashfsReatTestMd5sums)
	if err != nil {
		t.Fatal(err)
	}
	defer flist.Close()
	scanner := bufio.NewScanner(flist)
	for scanner.Scan() {
		line := scanner.Text()
		field := strings.Fields(line)
		size, err := strconv.Atoi(field[2])
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimPrefix(field[1], ".")
		tests = append(tests, readTest{
			name:   strings.TrimPrefix(name, "/"),
			p:      name,
			size:   size,
			md5sum: field[0],
		})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	// Open the squash file
	f, err := os.Open(squashfs.SquashfsReadTestFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	b := file.New(f, true)
	// create the filesystem
	fs, err := squashfs.Read(b, fi.Size(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Block sizes to test
	blockSizes := []int{
		0,      // use io.ReadAll
		4095,   // medium non binary
		4096,   // medium binary
		4097,   // medium non binary
		131071, // chunk size -1
		131072, // chunk size
		131073, // chunk size +1
		262143, // two chunks -1
		262144, // two chunks
		262145, // two chunks +1
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, blockSize := range blockSizes {
				quit := false
				t.Run(fmt.Sprintf("%d", blockSize), func(t *testing.T) {
					if testReadFile(t, fs, test, blockSize) {
						quit = true
					}
				})
				if quit {
					break
				}
			}
		})
	}
}

func TestSquashfsCreate(t *testing.T) {
	tests := []struct {
		blocksize int64
		filesize  int64
		fs        *squashfs.FileSystem
		err       error
	}{
		{500, 6000, nil, fmt.Errorf("blocksize %d too small, must be at least %d", 500, 4096)},
		{4097, squashfs.GB * squashfs.GB, nil, fmt.Errorf("blocksize %d is not a power of 2", 4097)},
		{4096, 10000000, &squashfs.FileSystem{}, nil},
	}
	for _, t2 := range tests {
		tt := t2
		t.Run(fmt.Sprintf("blocksize %d filesize %d", tt.blocksize, tt.filesize), func(t *testing.T) {
			// create the filesystem
			f, err := tmpSquashfsFile()
			if err != nil {
				t.Errorf("Failed to create squashfs tmpfile: %v", err)
			}

			b := file.New(f, false)
			fs, err := squashfs.Create(b, tt.filesize, 0, tt.blocksize)
			defer os.Remove(f.Name())
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("Create(%s, %d, %d, %d): mismatched errors, actual %v expected %v", f.Name(), tt.filesize, 0, tt.blocksize, err, tt.err)
			case (fs == nil && tt.fs != nil) || (fs != nil && tt.fs == nil):
				t.Errorf("Create(%s, %d, %d, %d): mismatched fs, actual then expected", f.Name(), tt.filesize, 0, tt.blocksize)
				t.Logf("%v", fs)
				t.Logf("%v", tt.fs)
			}
			// we do not match the filesystems here, only check functional accuracy
		})
	}
}

func TestSquashfsReadDirXattr(t *testing.T) {
	fs, err := getValidSquashfsFSReadOnly()
	if err != nil {
		t.Errorf("Failed to get read-only squashfs filesystem: %v", err)
	}
	tests := []struct {
		path   string
		f      string
		xattrs map[string]string
	}{
		{"/", "attrfile", map[string]string{"abc": "def", "myattr": "hello"}},
		{"/", "README.md", map[string]string{}},
	}

	for _, tt := range tests {
		// read the directory
		list, err := fs.ReadDir(tt.path)
		if err != nil {
			t.Errorf("unexpected error reading dir %s: %v", tt.path, err)
			continue
		}
		// get the file we care about
		var (
			fi    os.FileInfo
			found bool
		)
		for _, f := range list {
			if f.Name() == tt.f {
				fi = f
				found = true
			}
		}
		if !found {
			t.Errorf("Did not find file named %s", tt.f)
			continue
		}
		sysbase := fi.Sys()
		sys, ok := sysbase.(squashfs.FileStat)
		if !ok {
			t.Errorf("could not convert fi.Sys() to FileStat")
			continue
		}
		xa := sys.Xattrs()
		if !squashfs.CompareEqualMapStringString(xa, tt.xattrs) {
			t.Errorf("Mismatched xattrs, actual then expected")
			t.Logf("%v", xa)
			t.Logf("%v", tt.xattrs)
		}
	}
}

// Check a squash file with some corner cases
func TestSquashfsReadDirCornerCases(t *testing.T) {
	// Open the squash file
	f, err := os.Open(squashfs.SquashfsReadDirTestFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	b := file.New(f, true)
	// create the filesystem
	fs, err := squashfs.Read(b, fi.Size(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	fis, err := fs.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	want := 300
	if want != len(fis) {
		t.Errorf("Want %d entries but got %d", want, len(fis))
	}
	for i, fi := range fis {
		want := fmt.Sprintf("file_%03d", i+1)
		got := fi.Name()
		if want != got {
			t.Errorf("Want name %q but got %q", want, got)
		}
	}
}

//nolint:unused,revive // keep for future when we implement it and will need t
func TestFinalize(t *testing.T) {

}

func TestCreateAndReadFile(t *testing.T) {
	CreateFilesystem := func(d *disk.Disk, spec disk.FilesystemSpec) (filesystem.FileSystem, error) {
		// find out where the partition starts and ends, or if it is the entire disk
		var (
			size, start int64
		)

		switch {
		case spec.Partition == 0:
			size = d.Size
			start = 0
		case d.Table == nil:
			return nil, fmt.Errorf("cannot create filesystem on a partition without a partition table")
		default:
			partitions := d.Table.GetPartitions()
			// API indexes from 1, but slice from 0
			part := spec.Partition - 1
			if spec.Partition > len(partitions) {
				return nil, fmt.Errorf("cannot create filesystem on partition %d greater than maximum partition %d", spec.Partition, len(partitions))
			}
			size = partitions[part].GetSize()
			start = partitions[part].GetStart()
		}

		//nolint:exhaustive // we only support squashfs
		switch spec.FSType {
		case filesystem.TypeSquashfs:
			return squashfs.Create(d.Backend, size, start, d.LogicalBlocksize)
		default:
			return nil, errors.New("unknown filesystem type requested")
		}
	}

	// Create squashfs image file
	var (
		diskSize          int64 = 10 * 1024 * 1024 // 10 MB
		dir                     = t.TempDir()
		initdataImagePath       = filepath.Join(dir, "sqashtest")
	)
	mydisk, err := diskfs.Create(initdataImagePath, diskSize, 4096)
	if err != nil {
		t.Fatal(err)
	}

	fspec := disk.FilesystemSpec{Partition: 0, FSType: filesystem.TypeSquashfs, VolumeLabel: "label"}
	fs, err := CreateFilesystem(mydisk, fspec)
	if err != nil {
		t.Fatal(err)
	}

	rw, err := fs.OpenFile("/test", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}

	// content must be bigger than the block size
	content := []byte("dmVyc2PSAnJycKW3Rva2VuX2NvbmZpZ3NdCgpbdG9rZW5fY29uZmlncy5jb2NvX2FzXQp1cmwgPSAiaHR0cDovLzguMjE4LjIuMjIzOjgwMDAiCgpbdG9rZW5fY29uZmlncy5rYnNdCnVybCA9ICJodHRwczovLzguMjE4LjIuMjIzOjgwODAiCgpbZXZlbnRsb2dfY29uZmlnXQoKZXZlbnRsb2dfYWxnb3JpdGhtID0gInNoYTM4NCIKaW5pdF9wY3IgPSAxNwplbmFibGVfZXZlbnRsb2cgPSBmYWxzZQonJycKCiJjZGgudG9tbCIgPSAnJycKIyBUaGUgdHRycGMgc29jayBvZiBDREggdGhhdCBpcyB1c2VkIHRvIGxpc3RlbiB0byB0aGUgcmVxdWVzdHMKc29ja2V0ID0gInVuaXg6Ly8vcnVuL2NvbmZpZGVudGlhbC1jb250YWluZXJzL2NkaC5zb2NrIgoKIyBLQkMgcmVsYXRlZCBjb25maWdzLgpba2JjXQojIFJlcXVpcmVkLiBUaGUgS0JDIG5hbWUuIEl0IGNvdWxkIGJlIGBjY19rYmNgLCBgb25saW5lX3Nldl9rYmNgIG9yCiMgYG9mZmxpbmVfZnNfa2JjYC4gQWxsIHRoZSBpdGVtcyB1bmRlciBgW2NyZWRlbnRpYWxzXWAgd2lsbCBiZQojIHJldHJpZXZlZCB1c2luZyB0aGUga2JjLgpuYW1lID0gImNjX2tiYyIKCiMgUmVxdWlyZWQuIFRoZSBVUkwgb2YgS0JTLiBJZiBgbmFtZWAgaXMgZWl0aGVyIGBjY19rYmNgIG9yCiMgYG9ubGluZV9zZXZfa2JjYCwgdGhpcyBVUkwgd2lsbCBiZSB1c2VkIHRvIGNvbm5lY3QgdG8gdGhlCiMgQ29Db0tCUyAoZm9yIGNjX2tiYykgb3IgU2ltcGxlLUtCUyAoZm9yIG9ubGluZV9zZXZfa2JjKS4gSWYKIyBgbmFtZWAgaXMgYG9mZmxpbmVfZnNfa2JjYCwgVGhpcyBVUkwgd2lsbCBiZSBpZ25vcmVkLgp1cmwgPSAiaHR0cHM6Ly84LjIxOC4yLjIyMzo4MDgwIgoKIyBPcHRpb25hbC4gVGhlIHB1YmxpYyBrZXkgY2VydCBvZiBLQlMuIElmIG5vdCBnaXZlbiwgQ0RIIHdpbGwKIyB0cnkgdG8gdXNlIEhUVFAgdG8gY29ubmVjdCB0aGUgc2VydmVyLgojIGtic19jZXJ0ID0gIiIKCiMgY3JlZGVudGlhbHMgYXJlIGl0ZW1zIHRoYXQgd2lsbCBiZSByZXRyaWV2ZWQgZnJvbSBLQlMgd2hlbiBDREgKIyBpcyBsYXVuY2hlZC4gYHJlc291cmNlX3VyaWAgcmVmZXJzIHRvIHRoZSBLQlMgcmVzb3VyY2UgdXJpIGFuZAojIGBwYXRoYCBpcyB3aGVyZSB0byBwbGFjZSB0aGUgZmlsZS4KIyBgcGF0aGAgbXVzdCBiZSB3aXRoIHByZWZpeCBgL3J1bi9jb25maWRlbnRpYWwtY29udGFpbmVycy9jZGhgLAojIG9yIGl0IHdpbGwgYmUgYmxvY2tlZCBieSBDREguCiMgW1tjcmVkZW50aWFsc11dCiMgcGF0aCA9ICIvcnVuL2NvbmZpZGVudGlhbC1jb250YWluZXJzL2NkaC9rbXMtY3JlZGVudGlhbC9hbGl5dW4vZWNzUmFtUm9sZS5qc29uIgojIHJlc291cmNlX3VyaSA9ICJrYnM6Ly8vZGVmYXVsdC9hbGl5dW4vZWNzX3JhbV9yb2xlIgoKIyBbW2NyZWRlbnRpYWxzXV0KIyBwYXRoID0gIi9ydW4vY29uZmlkZW50aWFsLWNvbnRhaW5lcnMvY2RoL3Rlc3QvZmlsZSIKIyByZXNvdXJjZV91cmkgPSAia2JzOi8vL2RlZmF1bHQvdGVzdC9maWxlIgoKW2ltYWdlXQoKIyBUaGUgbWF4aW11bSBudW1iZXIgb2YgbGF5ZXJzIGRvd25sb2FkZWQgY29uY3VycmVudGx5IHdoZW4KIyBwdWxsaW5nIG9uZSBzcGVjaWZpYyBpbWFnZS4KIwojIFRoaXMgZGVmYXVsdHMgdG8gMy4KbWF4X2NvbmN1cnJlbnRfbGF5ZXJfZG93bmxvYWRzX3Blcl9pbWFnZSA9IDMKCiMgU2lnc3RvcmUgY29uZmlnIGZpbGUgVVJJIGZvciBzaW1wbGUgc2lnbmluZyBzY2hlbWUuCiMKIyBXaGVuIGBpbWFnZV9zZWN1cml0eV9wb2xpY3lfdXJpYCBpcyBzZXQgYW5kIGBTaW1wbGVTaWduaW5nYCAoc2lnbmVkQnkpIGlzCiMgdXNlZCBpbiB0aGUgcG9saWN5LCB0aGUgc2lnbmF0dXJlcyBvZiB0aGUgaW1hZ2VzIHdvdWxkIGJlIHVzZWQgZm9yIGltYWdlCiMgc2lnbmF0dXJlIHZhbGlkYXRpb24uIFRoaXMgcG9saWN5IHdpbGwgcmVjb3JkIHdoZXJlIHRoZSBzaWduYXR1cmVzIGlzLgojCiMgTm93IGl0IHN1cHBvcnRzIHR3byBkaWZmZXJlbnQgZm9ybXM6CiMgLSBgS0JTIFVSSWA6IHRoZSBzaWdzdG9yZSBjb25maWcgZmlsZSB3aWxsIGJlIGZldGNoZWQgZnJvbSBLQlMsCiMgZS5nLiBga2JzOi8vL2RlZmF1bHQvc2lnc3RvcmUtY29uZmlnL3Rlc3RgLgojIC0gYExvY2FsIFBhdGhgOiB0aGUgc2lnc3RvcmUgY29uZmlnIGZpbGUgd2lsbCBiZSBmZXRjaGVkIGZyb20gc29tZXdoZXJlIGxvY2FsbHksCiMgZS5nLiBgZmlsZTovLy9ldGMvc2ltcGxlLXNpZ25pbmcueWFtbGAuCiMKIyBCeSBkZWZhdWx0IHRoaXMgdmFsdWUgaXMgbm90IHNldC4Kc2lnc3RvcmVfY29uZmlnX3VyaSA9ICJrYnM6Ly8vZGVmYXVsdC9zaWdzdG9yZS1jb25maWcvdGVzdCIKCiMgSWYgYW55IGltYWdlIHNlY3VyaXR5IHBvbGljeSB3b3VsZCBiZSB1c2VkIHRvIGNvbnRyb2wgdGhlIGltYWdlIHB1bGxpbmcKIyBsaWtlIHNpZ25hdHVyZSB2ZXJpZmljYXRpb24sIHRoaXMgZmllbGQgaXMgdXNlZCB0byBzZXQgdGhlIFVSSSBvZiB0aGUKIyBwb2xpY3kgZmlsZS4KIwojIE5vdyBpdCBzdXBwb3J0cyB0d28gZGlmZmVyZW50IGZvcm1zOgojIC0gYEtCUyBVUklgOiB0aGUgaWFtZ2Ugc2VjdXJpdHkgcG9saWN5IHdpbGwgYmUgZmV0Y2hlZCBmcm9tIEtCUy4KIyAtIGBMb2NhbCBQYXRoYDogdGhlIHNlY3VyaXR5IHBvbGljeSB3aWxsIGJlIGZldGNoZWQgZnJvbSBzb21ld2hlcmUgbG9jYWxseS4KIyBlLmcuIGBmaWxlOi8vL2V0Yy9pbWFnZS1wb2xpY3kuanNvbmAuCiMKIyBUaGUgcG9saWN5IGZvbGxvd3MgdGhlIGZvcm1hdCBvZgojIDxodHRwczovL2dpdGh1Yi5jb20vY29udGFpbmVycy9pbWFnZS9ibG9iL21haW4vZG9jcy9jb250YWluZXJzLXBvbGljeS5qc29uLjUubWQ+LgojCiMgQXQgdGhlIHNhbWUgdGltZSwgc29tZSBlbmhlbmNlbWVudHMgYmFzZWQgb24gQ29DbyBpcyB1c2VkLCB0aGF0IGlzIHRoZQojIGBrZXlQYXRoYCBmaWVsZCBjYW4gYmUgZmlsbGVkIHdpdGggYSBLQlMgVVJJIGxpa2UgYGticzovLy9kZWZhdWx0L2tleS8xYAojCiMgQnkgZGVmYXVsdCB0aGlzIHZhbHVlIGlzIG5vdCBzZXQuCmltYWdlX3NlY3VyaXR5X3BvbGljeV91cmkgPSAia2JzOi8vL2RlZmF1bHQvc2VjdXJpdHktcG9saWN5L3Rlc3QiCgojIElmIGFueSBjcmVkZW50aWFsIGF1dGggKEJhc2UpIHdvdWxkIGJlIHVzZWQgdG8gY29ubmVjdCB0byBkb3dubG9hZAojIGltYWdlIGZyb20gcHJpdmF0ZSByZWdpc3RyeSwgdGhpcyBmaWVsZCBpcyB1c2VkIHRvIHNldCB0aGUgVVJJIG9mIHRoZQojIGNyZWRlbnRpYWwgZmlsZS4KIwojIE5vdyBpdCBzdXBwb3J0cyB0d28gZGlmZmVyZW50IGZvcm1zOgojIC0gYEtCUyBVUklgOiB0aGUgcmVnaXN0cnkgYXV0aCB3aWxsIGJlIGZldGNoZWQgZnJvbSBLQlMsCiMgZS5nLiBga2JzOi8vL2RlZmF1bHQvY3JlZGVudGlhbC90ZXN0YC4KIyAtIGBMb2NhbCBQYXRoYDogdGhlIHJlZ2lzdHJ5IGF1dGggd2lsbCBiZSBmZXRjaGVkIGZyb20gc29tZXdoZXJlIGxvY2FsbHksCiMgZS5nLiBgZmlsZTovLy9ldGMvaW1hZ2UtcmVnaXN0cnktYXV0aC5qc29uYC4KIwojIEJ5IGRlZmF1bHQgdGhpcyB2YWx1ZSBpcyBub3Qgc2V0LgphdXRoZW50aWNhdGVkX3JlZ2lzdHJ5X2NyZWRlbnRpYWxzX3VyaSA9ICJrYnM6Ly8vZGVmYXVsdC9jcmVkZW50aWFsL3Rlc3QiCgojIFByb3h5IHRoYXQgd2lsbCBiZSB1c2VkIHRvIHB1bGwgaW1hZ2UKIwojIEJ5IGRlZmF1bHQgdGhpcyB2YWx1ZSBpcyBub3Qgc2V0LgojIGltYWdlX3B1bGxfcHJveHkgPSAiaHR0cDovLzEyNy4wLjAuMTo1NDMyIgoKIyBObyBwcm94eSBlbnYgdGhhdCB3aWxsIGJlIHVzZWQgdG8gcHVsbCBpbWFnZS4KIwojIFRoaXMgd2lsbCBlbnN1cmUgdGhhdCB3aGVuIHdlIGFjY2VzcyB0aGUgaW1hZ2UgcmVnaXN0cnkgd2l0aCBzcGVjaWZpZWQKIyBJUHMsIHRoZSBgaW1hZ2VfcHVsbF9wcm94eWAgd2lsbCBub3QgYmUgdXNlZC4KIwojIElmIGBpbWFnZV9wdWxsX3Byb3h5YCBpcyBub3Qgc2V0LCB0aGlzIGZpZWxkIHdpbGwgZG8gbm90aGluZy4KIwojIEJ5IGRlZmF1bHQgdGhpcyB2YWx1ZSBpcyBub3Qgc2V0Lgpza2lwX3Byb3h5X2lwcyA9ICIxOTIuMTY4LjAuMSxsb2NhbGhvc3QiCgojIFRvIHN1cHBvcnQgcmVnaXN0cmllcyB3aXRoIHNlbGYgc2lnbmVkIGNlcnRzLiBUaGlzIGNvbmZpZyBpdGVtCiMgaXMgdXNlZCB0byBhZGQgZXh0cmEgdHJ1c3RlZCByb290IGNlcnRpZmljYXRpb25zLiBUaGUgY2VydGlmaWNhdGVzCiMgbXVzdCBiZSBlbmNvZGVkIGJ5IFBFTS4KIwojIEJ5IGRlZmF1bHQgdGhpcyB2YWx1ZSBpcyBub3Qgc2V0LgpleHRyYV9yb290X2NlcnRpZmljYXRlcyA9IFtdCgojIFRoZSBwYXRoIHRvIHN0b3JlIHRoZSBwdWxsZWQgaW1hZ2UgbGF5ZXIgZGF0YS4KIwojIFRoaXMgdmFsdWUgZGVmYXVsdHMgdG8gYC9ydW4vaW1hZ2UtcnMvYC4Kd29ya19kaXIgPSAiL3J1bi9pbWFnZS1ycyIKJycnCgoicG9saWN5LnJlZ28iID0gJycnCiMgQ29weXJpZ2h0IChjKSAyMDIzIE1pY3Jvc29mdCBDb3Jwb3JhdGlvbgojCiMgU1BEWC1MaWNlbnNlLUlkZW50aWZpZXI6IEFwYWNoZS0yLjAKIwoKcGFja2FnZSBhZ2VudF9wb2xpY3kKCmRlZmF1bHQgQWRkQVJQTmVpZ2hib3JzUmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgQWRkU3dhcFJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IENsb3NlU3RkaW5SZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBDb3B5RmlsZVJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IENyZWF0ZUNvbnRhaW5lclJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IENyZWF0ZVNhbmRib3hSZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBEZXN0cm95U2FuZGJveFJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IEV4ZWNQcm9jZXNzUmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgR2V0TWV0cmljc1JlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IEdldE9PTUV2ZW50UmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgR3Vlc3REZXRhaWxzUmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgTGlzdEludGVyZmFjZXNSZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBMaXN0Um91dGVzUmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgTWVtSG90cGx1Z0J5UHJvYmVSZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBPbmxpbmVDUFVNZW1SZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBQYXVzZUNvbnRhaW5lclJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFB1bGxJbWFnZVJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFJlYWRTdHJlYW1SZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBSZW1vdmVDb250YWluZXJSZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBSZW1vdmVTdGFsZVZpcnRpb2ZzU2hhcmVNb3VudHNSZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBSZXNlZWRSYW5kb21EZXZSZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBSZXN1bWVDb250YWluZXJSZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBTZXRHdWVzdERhdGVUaW1lUmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgU2V0UG9saWN5UmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgU2lnbmFsUHJvY2Vzc1JlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFN0YXJ0Q29udGFpbmVyUmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgU3RhcnRUcmFjaW5nUmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgU3RhdHNDb250YWluZXJSZXF1ZXN0IDo9IHRydWUKZGVmYXVsdCBTdG9wVHJhY2luZ1JlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFR0eVdpblJlc2l6ZVJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFVwZGF0ZUNvbnRhaW5lclJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFVwZGF0ZUVwaGVtZXJhbE1vdW50c1JlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFVwZGF0ZUludGVyZmFjZVJlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFVwZGF0ZVJvdXRlc1JlcXVlc3QgOj0gdHJ1ZQpkZWZhdWx0IFdhaXRQcm9jZXNzUmVxdWVzdCA6PSB0cnVlCmRlZmF1bHQgV3JpdGVTdHJlYW1SZXF1ZXN0IDo9IHRydWUKJycnCglvbiA9ICIwLjEuMCIKYWxnb3JpdGhtID0gInNoYTI1NiIKW2RhdGFdCgoiYWEudG9tbCIg")

	if _, err = rw.Write(content); err != nil {
		t.Fatal(err)
	}

	sqs, ok := fs.(*squashfs.FileSystem)
	if !ok {
		t.Fatal("not a squashfs filesystem")
	}
	if err = sqs.Finalize(squashfs.FinalizeOptions{
		NoCompressInodes:    true,
		NoCompressData:      true,
		NoCompressFragments: true,
	}); err != nil {
		t.Fatal(err)
	}

	di, err := diskfs.Open(initdataImagePath, diskfs.WithSectorSize(4096))
	if err != nil {
		t.Fatal(err)
	}
	fsr, err := di.GetFilesystem(0) // assuming it is the whole disk, so partition = 0
	if err != nil {
		t.Fatal(err)
	}

	f, err := fsr.OpenFile("/test", os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}

	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	diff, diffString := testhelper.DumpByteSlicesWithDiffs(b, content, 32, false, true, true)
	if diff {
		t.Errorf("groupdescriptor.toBytes() mismatched, actual then expected\n%s", diffString)
	}
}
