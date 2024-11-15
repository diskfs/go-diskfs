package squashfs_test

import (
	"bufio"
	"crypto/md5" //nolint:gosec // MD5 is still fine for detecting file corruptions
	"encoding/hex"
	"fmt"
	"io"
	stdfs "io/fs"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
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
	return squashfs.Create(f, 0, 0, 4096)
}

func getValidSquashfsFSReadOnly() (*squashfs.FileSystem, error) {
	f, err := os.Open(squashfs.Squashfsfile)
	if err != nil {
		return nil, fmt.Errorf("Failed to read squashfs testfile %s: %v", squashfs.Squashfsfile, err)
	}
	return squashfs.Read(f, 0, 0, 4096)
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
					if string(b) != tt.expected {
						t.Errorf("%s: mismatched contents, actual then expected", header)
						t.Log(string(b))
						t.Log(tt.expected)
					}
				}
			}
		}
		t.Run("read-only", func(t *testing.T) {
			fs, err := getValidSquashfsFSReadOnly()
			if err != nil {
				t.Errorf("Failed to get read-only squashfs filesystem: %v", err)
			}
			tests := []testStruct{
				// error opening a directory
				{"/", os.O_RDONLY, "", fmt.Errorf("cannot open directory %s as file", "/")},
				{"/abcdefg", os.O_RDONLY, "", fmt.Errorf("target file %s does not exist", "/abcdefg")},
				{"/foo/filename_10", os.O_RDONLY, "filename_10\n", nil},
				{"/foo/filename_75", os.O_RDONLY, "filename_75\n", nil},
				{"/README.MD", os.O_RDONLY, "", fmt.Errorf("target file %s does not exist", "/README.MD")},
				{"/README.md", os.O_RDONLY, "README\n", nil},
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
			// create the filesystem
			fs, err := squashfs.Read(f, tt.filesize, 0, tt.blocksize)
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
	// create the filesystem
	fs, err := squashfs.Read(f, fi.Size(), 0, 0)
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
	// create the filesystem
	fs, err := squashfs.Read(f, fi.Size(), 0, 0)
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
			fs, err := squashfs.Create(f, tt.filesize, 0, tt.blocksize)
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
	// create the filesystem
	fs, err := squashfs.Read(f, fi.Size(), 0, 0)
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
