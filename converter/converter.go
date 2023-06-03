package converter

import (
	"io/fs"
	"os"
	"path"

	"github.com/diskfs/go-diskfs/filesystem"
)

type fsCompatible struct {
	filesystem.FileSystem
}

type fsFileWrapper struct {
	filesystem.File
	stat *os.FileInfo
}

func (f *fsFileWrapper) Stat() (fs.FileInfo, error) {
	if f.stat == nil {
		return nil, fs.ErrInvalid
	}
	return *f.stat, nil
}

func (f *fsCompatible) Open(name string) (fs.File, error) {
	file, err := f.OpenFile(name, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	dirname := path.Dir(name)
	var stat *os.FileInfo
	if info, err := f.ReadDir(dirname); err == nil {
		for i := range info {
			if info[i].Name() == path.Base(name) {
				stat = &info[i]
			}
		}
	}
	return &fsFileWrapper{File: file, stat: stat}, nil
}

func FS(f filesystem.FileSystem) fs.FS {
	return &fsCompatible{f}
}
