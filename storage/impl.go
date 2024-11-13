package storage

import (
	"io"
	"io/fs"
	"os"
)

type file struct {
	storage  fs.File
	readonly bool
}

func New(f fs.File, isReadonly bool) Storage {
	return file{
		storage:  f,
		readonly: isReadonly,
	}
}

// storage.File interface guard
var _ Storage = (*file)(nil)

// OS-stecific file for ioctl calls via fd
func (f file) Sys() (*os.File, error) {
	if osFile, ok := f.storage.(*os.File); ok {
		return osFile, nil
	}
	return nil, ErrNotSuitable
}

// file for read-write operations
func (f file) Writable() (WritableFile, error) {
	if rwFile, ok := f.storage.(WritableFile); ok {
		if !f.readonly {
			return rwFile, nil
		} else {
			return nil, ErrIncorrectOpenMode
		}
	}
	return nil, ErrNotSuitable
}

func (f file) Stat() (fs.FileInfo, error) {
	return f.storage.Stat()
}

func (f file) Read(b []byte) (int, error) {
	return f.storage.Read(b)
}

func (f file) Close() error {
	return f.storage.Close()
}

func (f file) ReadAt(p []byte, off int64) (n int, err error) {
	if readerAt, ok := f.storage.(io.ReaderAt); ok {
		return readerAt.ReadAt(p, off)
	}
	return -1, ErrNotSuitable
}

func (f file) Seek(offset int64, whence int) (int64, error) {
	if seeker, ok := f.storage.(io.Seeker); ok {
		return seeker.Seek(offset, whence)
	}
	return -1, ErrNotSuitable
}
