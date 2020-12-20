package squashfs_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
)

func getOpenMode(mode int) string {
	modes := make([]string, 0, 0)
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
	f, err := ioutil.TempFile("", filename)
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

func TestSquashfsMkdir(t *testing.T) {
	t.Run("read-only", func(t *testing.T) {
		fs, err := getValidSquashfsFSReadOnly()
		if err != nil {
			t.Fatalf("Failed to get read-only squashfs filesystem: %v", err)
		}
		err = fs.Mkdir("/abcdef")
		if err == nil {
			t.Errorf("Received no error when trying to mkdir read-only filesystem")
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
		err = os.MkdirAll(existPathFull, 0755)
		if err != nil {
			t.Fatalf("Could not create path %s in workspace as %s: %v", existPath, existPathFull, err)
		}
		for _, tt := range tests {
			fs := tt.fs
			ws := fs.Workspace()
			err := fs.Mkdir(tt.path)
			if (err == nil && tt.err != nil) || (err != nil && err == nil) {
				t.Errorf("Unexpected error mismatch. Actual: %v, expected: %v", err, tt.err)
			}
			// did the path exist?
			if ws != "" {
				fullPath := path.Join(ws, tt.path)
				if _, err := os.Stat(fullPath); os.IsNotExist(err) {
					t.Errorf("Path did not exist after creation base %s, in workspace %s", tt.path, fullPath)
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
		os.MkdirAll(existPathWs, 0755)
		// create files
		for i := 0; i < 10; i++ {
			filename := path.Join(existPathWs, fmt.Sprintf("filename_%d", i))
			contents := fmt.Sprintf("abcdefg %d", i)
			ioutil.WriteFile(filename, []byte(contents), 0644)
		}
		// get the known []FileInfo
		fi, err := ioutil.ReadDir(existPathWs)
		if err != nil {
			t.Errorf("Failed to read directory %s in workspace as %s: %v", existPath, existPathWs, err)
		}
		// convert to []*os.FileInfo to be useful
		fis := make([]*os.FileInfo, 0, len(fi))
		for _, e := range fi {
			fis = append(fis, &e)
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

	t.Run("Read", func(t *testing.T) {
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
					b, err := ioutil.ReadAll(reader)
					if err != nil {
						t.Errorf("%s: ioutil.ReadAll(reader) unexpected error: %v", header, err)
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
				{"/", os.O_RDONLY, "", fmt.Errorf("Cannot open directory %s as file", "/")},
				{"/abcdefg", os.O_RDONLY, "", fmt.Errorf("Target file %s does not exist", "/abcdefg")},
				{"/foo/filename_10", os.O_RDONLY, "filename_10\n", nil},
				{"/foo/filename_75", os.O_RDONLY, "filename_75\n", nil},
				{"/README.MD", os.O_RDONLY, "", fmt.Errorf("Target file %s does not exist", "/README.MD")},
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
			os.MkdirAll(path.Join(ws, subdir), 0755)
			for i := 0; i <= 75; i++ {
				filename := fmt.Sprintf("FILENA%02d", i)
				content := fmt.Sprintf("filename_%d\n", i)
				ioutil.WriteFile(path.Join(ws, subdir, filename), []byte(content), 0644)
			}
			tests := []testStruct{
				// error opening a directory
				{"/", os.O_RDONLY, "", fmt.Errorf("Cannot open directory %s as file", "/")},
				// open non-existent file for read or read write
				{"/abcdefg", os.O_RDONLY, "", fmt.Errorf("Target file %s does not exist", "/abcdefg")},
				// open file for read or read write and check contents
				{"/FOO/FILENA01", os.O_RDONLY, "filename_1\n", nil},
				{"/FOO/FILENA75", os.O_RDONLY, "filename_75\n", nil},
			}
			runTests(t, fs, tests)
		})
	})
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
	for i, tt := range tests {
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
	for _, tt := range tests {
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
			t.Errorf("Unexpected error reading dir %s: %v", tt.path, err)
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
			t.Errorf("Could not convert fi.Sys() to FileStat")
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

func TestFinalize(t *testing.T) {

}
