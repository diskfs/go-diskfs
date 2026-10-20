package erofs

import (
	"encoding/binary"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/filesystem"
)

const defaultBlockSize int64 = 4096

// FileSystem implements read-only access to EROFS images.
type FileSystem struct {
	superblock *superblock
	backend    backend.Storage
	size       int64
	blocksize  int64
	workspace  string
}

var _ filesystem.FileSystem = (*FileSystem)(nil)

func Read(b backend.Storage, size, start, blocksize int64) (*FileSystem, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid filesystem size %d", size)
	}
	if start < 0 {
		return nil, fmt.Errorf("invalid filesystem start %d", start)
	}
	if blocksize < 0 || (blocksize != 0 && blocksize&(blocksize-1) != 0) {
		return nil, fmt.Errorf("block size %d is not a power of two", blocksize)
	}
	if start != 0 {
		b = backend.Sub(b, start, size)
	}

	header := make([]byte, 14)
	if err := readAt(b, header, superOffset); err != nil {
		return nil, fmt.Errorf("unable to read EROFS superblock header: %w", err)
	}
	superblockLength := superblockBaseSize + int(header[13])*superblockExtSlotSize
	superblockBytes := make([]byte, superblockLength)
	if err := readAt(b, superblockBytes, superOffset); err != nil {
		return nil, fmt.Errorf("unable to read EROFS superblock: %w", err)
	}
	preliminary := append([]byte(nil), superblockBytes...)
	binary.LittleEndian.PutUint32(preliminary[8:12], binary.LittleEndian.Uint32(preliminary[8:12])&^featureCompatChecksum)
	s, err := parseSuperblock(preliminary, size)
	checksumEnabled := binary.LittleEndian.Uint32(superblockBytes[8:12])&featureCompatChecksum != 0
	if err == nil && checksumEnabled {
		checksumLength := int(s.blockSize())
		if checksumLength > superOffset {
			checksumLength -= superOffset
		}
		checksumBytes := make([]byte, checksumLength)
		if err := readAt(b, checksumBytes, superOffset); err != nil {
			return nil, fmt.Errorf("unable to read EROFS checksum area: %w", err)
		}
		s, err = parseSuperblock(checksumBytes, size)
	}
	if err != nil {
		return nil, err
	}
	if blocksize != 0 && blocksize != s.blockSize() {
		return nil, fmt.Errorf("requested block size %d does not match superblock block size %d", blocksize, s.blockSize())
	}

	result := &FileSystem{superblock: s, backend: b, size: size, blocksize: s.blockSize()}
	if _, err := result.getInode(uint64(s.rootNID)); err != nil {
		return nil, fmt.Errorf("unable to read root inode: %w", err)
	}
	return result, nil
}

func Create(b backend.Storage, size, start, blocksize int64) (*FileSystem, error) {
	if size <= 0 || start < 0 {
		return nil, fmt.Errorf("invalid filesystem size %d or start %d", size, start)
	}
	if blocksize == 0 {
		blocksize = defaultBlockSize
	}
	if blocksize < 512 || blocksize > 65536 || blocksize&(blocksize-1) != 0 {
		return nil, fmt.Errorf("invalid block size %d", blocksize)
	}
	workspace, err := os.MkdirTemp("", "diskfs_erofs")
	if err != nil {
		return nil, fmt.Errorf("unable to create EROFS workspace: %w", err)
	}
	if start != 0 {
		b = backend.Sub(b, start, size)
	}
	return &FileSystem{backend: b, size: size, blocksize: blocksize, workspace: workspace}, nil
}

func (f *FileSystem) blockSize() int64 { return f.blocksize }

func (fs *FileSystem) Type() filesystem.Type { return filesystem.TypeErofs }
func (fs *FileSystem) Label() string {
	if fs.superblock == nil {
		return ""
	}
	return strings.TrimRight(string(fs.superblock.volumeName[:]), "\x00")
}
func (fs *FileSystem) SetLabel(string) error { return filesystem.ErrReadonlyFilesystem }
func (fs *FileSystem) Close() error {
	if fs.workspace == "" {
		return nil
	}
	return os.RemoveAll(fs.workspace)
}

func (f *FileSystem) stat(p string) (*inode, error) {
	if !iofs.ValidPath(p) {
		return nil, iofs.ErrInvalid
	}
	node, err := f.getInode(uint64(f.superblock.rootNID))
	if err != nil {
		return nil, err
	}
	if p == "." {
		if !node.isDir() {
			return nil, fmt.Errorf("root inode is not a directory")
		}
		return node, nil
	}
	for _, component := range strings.Split(p, "/") {
		if !node.isDir() {
			return nil, fmt.Errorf("%s is not a directory", component)
		}
		node, err = f.lookup(node, component)
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

func (f *FileSystem) Open(p string) (iofs.File, error) {
	if !iofs.ValidPath(p) {
		return nil, iofs.ErrInvalid
	}
	node, err := f.stat(p)
	if err != nil {
		return nil, err
	}
	if node.isDir() {
		entries, err := f.readDirectory(node)
		if err != nil {
			return nil, err
		}
		return &directoryFile{filesystem: f, node: node, entries: entries}, nil
	}
	return &File{filesystem: f, node: node, name: path.Base(p)}, nil
}

func (f *FileSystem) OpenFile(p string, flag int) (filesystem.File, error) {
	if flag != os.O_RDONLY {
		return nil, filesystem.ErrReadonlyFilesystem
	}
	file, err := f.Open(p)
	if err != nil {
		return nil, err
	}
	if regular, ok := file.(*File); ok {
		return regular, nil
	}
	return nil, fmt.Errorf("%s is a directory", p)
}

func (f *FileSystem) Stat(p string) (iofs.FileInfo, error) {
	node, err := f.stat(p)
	if err != nil {
		return nil, err
	}
	return &fileInfo{name: path.Base(p), node: node}, nil
}

func (f *FileSystem) ReadDir(p string) ([]iofs.DirEntry, error) {
	node, err := f.stat(p)
	if err != nil {
		return nil, err
	}
	if !node.isDir() {
		return nil, fmt.Errorf("%s is not a directory", p)
	}
	entries, err := f.readDirectory(node)
	if err != nil {
		return nil, err
	}
	result := make([]iofs.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.name == "." || entry.name == ".." {
			continue
		}
		result = append(result, &dirEntry{entry: entry, filesystem: f})
	}
	return result, nil
}

func (f *FileSystem) ReadFile(p string) ([]byte, error) {
	file, err := f.Open(p)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if _, ok := file.(*File); !ok {
		return nil, fmt.Errorf("%s is a directory", p)
	}
	return io.ReadAll(file)
}

func (f *FileSystem) readInodeData(node *inode, b []byte, offset int64) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if node.isDir() || node.mode&0xF000 == linuxModeRegular || node.isSymlink() {
		nonInlineSize := node.size
		if node.dataLayout == dataLayoutFlatInline {
			tailSize := node.size % f.blockSize()
			nonInlineSize = node.size - tailSize
		}
		if offset < nonInlineSize {
			location := int64(node.startBlock)*f.blockSize() + offset
			if location < 0 || location+int64(len(b)) > f.size {
				return 0, fmt.Errorf("inode %d data range is outside the image", node.nid)
			}
			if err := readAt(f.backend, b, location); err != nil {
				return 0, err
			}
			return len(b), nil
		}
		if node.dataLayout != dataLayoutFlatInline {
			return 0, fmt.Errorf("inode %d data range is outside the image", node.nid)
		}
		inodeOffset := int64(f.superblock.metaBlockAddress)*f.blockSize() + int64(node.nid)*inodeSlotSize
		inlineStart := inodeOffset + int64(node.inodeSize+node.xattrSize) + (offset - nonInlineSize)
		if inlineStart < inodeOffset || inlineStart+int64(len(b)) > f.size ||
			inlineStart/f.blockSize() != (inlineStart+int64(len(b))-1)/f.blockSize() {
			return 0, fmt.Errorf("inode %d inline data crosses or exceeds a block boundary", node.nid)
		}
		if err := readAt(f.backend, b, inlineStart); err != nil {
			return 0, err
		}
		return len(b), nil
	}
	return 0, fmt.Errorf("inode %d has no readable data", node.nid)
}

func readAt(b backend.Storage, p []byte, offset int64) error {
	if len(p) == 0 {
		return nil
	}
	read, err := b.ReadAt(p, offset)
	if err != nil {
		return err
	}
	if read != len(p) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (fs *FileSystem) Mkdir(string) error { return filesystem.ErrNotImplemented }
func (fs *FileSystem) Mknod(string, uint32, int) error {
	return filesystem.ErrNotImplemented
}
func (fs *FileSystem) Link(string, string) error { return filesystem.ErrNotImplemented }
func (fs *FileSystem) Symlink(string, string) error {
	return filesystem.ErrNotImplemented
}
func (fs *FileSystem) Chmod(string, os.FileMode) error { return filesystem.ErrNotImplemented }
func (fs *FileSystem) Chown(string, int, int) error {
	return filesystem.ErrNotImplemented
}
func (f *FileSystem) Chtimes(string, time.Time, time.Time, time.Time) error {
	return filesystem.ErrNotImplemented
}
func (fs *FileSystem) Rename(string, string) error { return filesystem.ErrNotImplemented }
func (fs *FileSystem) Remove(string) error         { return filesystem.ErrNotImplemented }
