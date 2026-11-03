package erofs

import (
	"errors"
	iofs "io/fs"
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/backend"
	fsapi "github.com/diskfs/go-diskfs/filesystem"
)

func checkPathError(t *testing.T, err error, op, path string, cause error) {
	t.Helper()
	var pathError *iofs.PathError
	if !errors.As(err, &pathError) {
		t.Fatalf("error = %T %v, want *fs.PathError", err, err)
	}
	if pathError.Op != op || pathError.Path != path {
		t.Errorf("PathError = {Op: %q, Path: %q}, want {%q, %q}", pathError.Op, pathError.Path, op, path)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Errorf("error = %v, want errors.Is(err, %v)", err, cause)
	}
}

func pathErrorOperations(filesystem *FileSystem) []struct {
	op   string
	call func(string) error
} {
	return []struct {
		op   string
		call func(string) error
	}{
		{op: "open", call: func(p string) error {
			file, err := filesystem.Open(p)
			if err == nil {
				_ = file.Close()
			}
			return err
		}},
		{op: "stat", call: func(p string) error { _, err := filesystem.Stat(p); return err }},
		{op: "readdir", call: func(p string) error { _, err := filesystem.ReadDir(p); return err }},
		{op: "readfile", call: func(p string) error { _, err := filesystem.ReadFile(p); return err }},
		{op: "openfile", call: func(p string) error {
			file, err := filesystem.OpenFile(p, os.O_RDONLY)
			if err == nil {
				_ = file.Close()
			}
			return err
		}},
	}
}

func TestPublicPathErrors(t *testing.T) {
	filesystem := readTestImage(t, uncompressedFixture)
	for _, operation := range pathErrorOperations(filesystem) {
		for _, tt := range []struct {
			path  string
			cause error
		}{
			{path: "missing", cause: iofs.ErrNotExist},
			{path: "missing/child", cause: iofs.ErrNotExist},
			{path: "../hello.txt", cause: iofs.ErrInvalid},
			{path: "/hello.txt", cause: iofs.ErrInvalid},
			{path: "", cause: iofs.ErrInvalid},
		} {
			t.Run(operation.op+"/"+tt.path, func(t *testing.T) {
				checkPathError(t, operation.call(tt.path), operation.op, tt.path, tt.cause)
			})
		}
	}
}

func TestPathErrorsForFileTypesAndFlags(t *testing.T) {
	filesystem := readTestImage(t, uncompressedFixture)
	t.Run("readfile directory", func(t *testing.T) {
		_, err := filesystem.ReadFile(".")
		checkPathError(t, err, "readfile", ".", nil)
	})
	t.Run("openfile directory", func(t *testing.T) {
		_, err := filesystem.OpenFile(".", os.O_RDONLY)
		checkPathError(t, err, "openfile", ".", nil)
	})
	t.Run("readdir file", func(t *testing.T) {
		_, err := filesystem.ReadDir("hello.txt")
		checkPathError(t, err, "readdir", "hello.txt", nil)
	})
	t.Run("openfile write", func(t *testing.T) {
		_, err := filesystem.OpenFile("hello.txt", os.O_WRONLY)
		checkPathError(t, err, "openfile", "hello.txt", fsapi.ErrReadonlyFilesystem)
	})
}

type failingReadBackend struct {
	backend.Storage
	offset int64
	err    error
}

func (b failingReadBackend) ReadAt(p []byte, offset int64) (int, error) {
	if offset == b.offset {
		return 0, b.err
	}
	return b.Storage.ReadAt(p, offset)
}

func TestPathErrorsPreserveBackendFailures(t *testing.T) {
	filesystem, err := Read(testBackend(t, buildImage(false)), 16384, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("injected backend failure")
	original := filesystem.backend
	filesystem.backend = failingReadBackend{Storage: original, offset: 4128, err: cause}
	for _, operation := range pathErrorOperations(filesystem) {
		t.Run(operation.op, func(t *testing.T) {
			checkPathError(t, operation.call("hello"), operation.op, "hello", cause)
		})
	}

	// ReadFile must also wrap failures encountered after Open succeeds.
	filesystem.backend = failingReadBackend{Storage: original, offset: 12288, err: cause}
	_, err = filesystem.ReadFile("hello")
	checkPathError(t, err, "readfile", "hello", cause)
}

func TestPathErrorsForMalformedDirectory(t *testing.T) {
	image := buildImage(false)
	image[8202] = 0 // Invalid directory entry type.
	filesystem, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range pathErrorOperations(filesystem) {
		if operation.op == "stat" {
			continue // Stat does not read the root directory's contents.
		}
		t.Run(operation.op, func(t *testing.T) {
			checkPathError(t, operation.call("."), operation.op, ".", nil)
		})
	}
}
