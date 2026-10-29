package erofs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"

	"github.com/diskfs/go-diskfs/filesystem"
)

const (
	fileTypeRegular uint8 = iota + 1
	fileTypeDirectory
	fileTypeCharDevice
	fileTypeBlockDevice
	fileTypeFIFO
	fileTypeSocket
	fileTypeSymlink
)

var _ fs.File = (*File)(nil)
var _ fs.ReadDirFile = (*directoryFile)(nil)

type File struct {
	// mu protects the cursor and the single decoded extent, including concurrent
	// ReadAt calls. Each open file retains at most maxDecodedExtentSize bytes.
	mu         sync.Mutex
	decoded    decodedExtent
	filesystem *FileSystem
	node       *inode
	name       string
	offset     int64
}

func (f *File) Read(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.filesystem == nil {
		return 0, os.ErrClosed
	}
	if f.offset >= f.node.size {
		return 0, io.EOF
	}
	if len(b) == 0 {
		return 0, nil
	}
	length := int64(len(b))
	if f.node.size-f.offset < length {
		length = f.node.size - f.offset
	}
	read, err := f.filesystem.readInodeData(f.node, b[:length], f.offset, &f.decoded)
	f.offset += int64(read)
	if err != nil {
		return read, err
	}
	if f.offset >= f.node.size {
		err = io.EOF
	}
	return read, err
}

func (f *File) ReadAt(b []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.filesystem == nil {
		return 0, os.ErrClosed
	}
	if offset < 0 {
		return 0, fmt.Errorf("read offset %d is outside file size %d", offset, f.node.size)
	}
	if len(b) == 0 {
		return 0, nil
	}
	if offset >= f.node.size {
		return 0, io.EOF
	}
	length := int64(len(b))
	if f.node.size-offset < length {
		length = f.node.size - offset
	}
	read, err := f.filesystem.readInodeData(f.node, b[:length], offset, &f.decoded)
	if err != nil {
		return read, err
	}
	if read < len(b) {
		return read, io.EOF
	}
	return read, nil
}

func (f *File) Write([]byte) (int, error) { return 0, filesystem.ErrReadonlyFilesystem }

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.filesystem == nil {
		return 0, os.ErrClosed
	}
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		newOffset = f.node.size + offset
	default:
		return f.offset, fmt.Errorf("invalid seek whence %d", whence)
	}
	if newOffset < 0 {
		return f.offset, fmt.Errorf("cannot seek before start of file")
	}
	f.offset = newOffset
	return newOffset, nil
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.decoded = decodedExtent{}
	f.filesystem = nil
	return nil
}

func (f *File) Stat() (fs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.filesystem == nil {
		return nil, os.ErrClosed
	}
	return &fileInfo{name: f.name, node: f.node}, nil
}

type directoryFile struct {
	filesystem *FileSystem
	node       *inode
	name       string
	entries    []*directoryEntry
	offset     int
}

func (f *directoryFile) Read([]byte) (int, error) {
	return 0, errors.New("cannot read from a directory")
}

func (f *directoryFile) Close() error {
	f.filesystem = nil
	return nil
}

func (f *directoryFile) Stat() (fs.FileInfo, error) {
	if f.filesystem == nil {
		return nil, os.ErrClosed
	}
	return &fileInfo{name: f.name, node: f.node}, nil
}

func (f *directoryFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.filesystem == nil {
		return nil, os.ErrClosed
	}
	if n <= 0 {
		entries := make([]fs.DirEntry, 0, len(f.entries)-f.offset)
		for _, entry := range f.entries[f.offset:] {
			entries = append(entries, &dirEntry{entry: entry, filesystem: f.filesystem})
		}
		f.offset = len(f.entries)
		return entries, nil
	}
	if f.offset >= len(f.entries) {
		return nil, io.EOF
	}
	n = min(n, len(f.entries)-f.offset)
	end := f.offset + n
	entries := make([]fs.DirEntry, 0, end-f.offset)
	for _, entry := range f.entries[f.offset:end] {
		entries = append(entries, &dirEntry{entry: entry, filesystem: f.filesystem})
	}
	f.offset = end
	return entries, nil
}

type dirEntry struct {
	entry      *directoryEntry
	filesystem *FileSystem
}

var _ fs.DirEntry = (*dirEntry)(nil)

func (d *dirEntry) Name() string { return d.entry.name }
func (d *dirEntry) IsDir() bool {
	return d.entry.fileType == fileTypeDirectory
}
func (d *dirEntry) Type() fs.FileMode {
	switch d.entry.fileType {
	case fileTypeDirectory:
		return fs.ModeDir
	case fileTypeCharDevice:
		return fs.ModeDevice | fs.ModeCharDevice
	case fileTypeBlockDevice:
		return fs.ModeDevice
	case fileTypeFIFO:
		return fs.ModeNamedPipe
	case fileTypeSymlink:
		return fs.ModeSymlink
	case fileTypeSocket:
		return fs.ModeSocket
	default:
		return 0
	}
}
func (d *dirEntry) Info() (fs.FileInfo, error) {
	node, err := d.filesystem.getInode(d.entry.nid)
	if err != nil {
		return nil, err
	}
	return &fileInfo{name: d.entry.name, node: node}, nil
}
