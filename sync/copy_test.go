package sync

import (
	"bytes"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/diskfs/go-diskfs/filesystem"
)

// fakeFS implements filesystem.FileSystem for testing CopyFileSystem.
type fakeFS struct {
	dirs  []string
	files map[string][]byte
	times map[string]time.Time
}

// fakeFile satisfies filesystem.File.
type fakeFile struct {
	path string
	buf  *bytes.Buffer
	fs   *fakeFS
}

// Mkdir records directory creations.
func (f *fakeFS) Mkdir(path string) error {
	f.dirs = append(f.dirs, path)
	return nil
}

// OpenFile returns a fakeFile for writing.

// Chtimes records the creation time for a file.
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Chtimes(path string, ctime, atime, mtime time.Time) error {
	f.times[path] = ctime
	return nil
}

// Chmod satisfies filesystem.FileSystem interface (no-op).
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Chmod(path string, mode os.FileMode) error { return nil }

// Chown satisfies filesystem.FileSystem interface (no-op).
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Chown(path string, uid, gid int) error { return nil }

// Remove satisfies filesystem.FileSystem interface (no-op).
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Remove(path string) error { return nil }

// Rename satisfies filesystem.FileSystem interface (no-op).
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Rename(oldpath, newpath string) error { return nil }

// Stat satisfies filesystem.FileSystem interface (no-op).
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Stat(path string) (os.FileInfo, error) { return nil, nil }

// Close satisfies filesystem.FileSystem interface (no-op).
func (f *fakeFS) Close() error { return nil }

// Type satisfies filesystem.FileSystem interface.
func (f *fakeFS) Type() filesystem.Type { return filesystem.TypeFat32 }

// Mknod satisfies filesystem.FileSystem interface.
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Mknod(pathname string, mode uint32, dev int) error { return nil }

// Link satisfies filesystem.FileSystem interface.
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Link(oldpath, newpath string) error { return nil }

// Symlink satisfies filesystem.FileSystem interface.
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Symlink(oldpath, newpath string) error { return nil }

// Open satisfies filesystem.FileSystem interface (unused for copy).
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) Open(pathname string) (fs.File, error) { return nil, nil }

// ReadDir satisfies fs.ReadDirFS (no-op).
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) ReadDir(name string) ([]fs.DirEntry, error) { return nil, nil }

// ReadFile satisfies fs.ReadFileFS (no-op).
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) ReadFile(name string) ([]byte, error) { return nil, nil }

// OpenFile satisfies filesystem.FileSystem interface for writing files.
//
//nolint:revive // flag is unused, keeping for clarity of intent.
func (f *fakeFS) OpenFile(pathname string, flag int) (filesystem.File, error) {
	buf := &bytes.Buffer{}
	ff := &fakeFile{path: pathname, buf: buf, fs: f}
	if f.files == nil {
		f.files = make(map[string][]byte)
	}
	if f.times == nil {
		f.times = make(map[string]time.Time)
	}
	return ff, nil
}

// Label satisfies filesystem.FileSystem interface.
func (f *fakeFS) Label() string { return "" }

// SetLabel satisfies filesystem.FileSystem interface.
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFS) SetLabel(label string) error { return nil }

// Write implements io.Writer.
func (f *fakeFile) Write(p []byte) (int, error) {
	n, err := f.buf.Write(p)
	f.fs.files[f.path] = f.buf.Bytes()
	return n, err
}

// Read implements io.Reader (unused here).
func (f *fakeFile) Read(p []byte) (int, error) { return f.buf.Read(p) }

// Close is a no-op.
func (f *fakeFile) Close() error { return nil }

// Seek is a no-op.
//
//nolint:revive // keeping args for clarity of intent.
func (f *fakeFile) Seek(offset int64, whence int) (int64, error) { return 0, nil }

// Stat returns a minimal FileInfo.
func (f *fakeFile) Stat() (os.FileInfo, error) {
	return f, nil
}

// The fakeFile itself implements os.FileInfo for simplicity.
func (f *fakeFile) Name() string       { return f.path }
func (f *fakeFile) Size() int64        { return int64(f.buf.Len()) }
func (f *fakeFile) Mode() os.FileMode  { return 0 }
func (f *fakeFile) ModTime() time.Time { return f.fs.times[f.path] }
func (f *fakeFile) IsDir() bool        { return false }
func (f *fakeFile) Sys() interface{}   { return nil }

// TestCopyFileSystem_Basic verifies directories and files are copied.
func TestCopyFileSystem_Basic(t *testing.T) {
	now := time.Now()
	src := fstest.MapFS{
		"foo.txt": {Data: []byte("hello"), ModTime: now},
		"dir":     {Mode: fs.ModeDir, ModTime: now},
		"dir/bar": {Data: []byte("world"), ModTime: now},
	}
	dst := &fakeFS{}
	if err := CopyFileSystem(src, dst); err != nil {
		t.Fatalf("CopyFileSystem failed: %v", err)
	}
	// directory created
	found := false
	for _, d := range dst.dirs {
		if d == "dir" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected Mkdir(\"dir\"), got %v", dst.dirs)
	}
	// files copied
	if string(dst.files["foo.txt"]) != "hello" {
		t.Errorf("foo.txt = %q, want %q", dst.files["foo.txt"], "hello")
	}
	if string(dst.files["dir/bar"]) != "world" {
		t.Errorf("dir/bar = %q, want %q", dst.files["dir/bar"], "world")
	}
	// timestamp recorded (should default to zero time)
	if ts, ok := dst.times["foo.txt"]; !ok || ts != now {
		t.Errorf("expected timestamp for foo.txt, got %v", ts)
	}
}

// TestCopyFileSystem_SkipNonRegular ensures non-regular entries (symlinks) are skipped.
func TestCopyFileSystem_SkipNonRegular(t *testing.T) {
	src := fstest.MapFS{
		"sl": {Data: []byte(""), Mode: fs.ModeSymlink},
	}
	dst := &fakeFS{}
	if err := CopyFileSystem(src, dst); err != nil {
		t.Fatalf("CopyFileSystem failed: %v", err)
	}
	if _, ok := dst.files["sl"]; ok {
		t.Errorf("expected non-regular file to be skipped, but copied")
	}
}
