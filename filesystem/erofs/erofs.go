package erofs

import (
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

// FileSystem implements read-only access to EROFS images.
type FileSystem struct {
	superblock   *superblock
	backend      backend.Storage
	size         int64
	blocksize    int64
	lzmaDictSize uint32
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
	s, err := parseSuperblock(superblockBytes, size)
	if err != nil {
		return nil, err
	}
	if s.featureCompat&featureCompatChecksum != 0 {
		checksumLength := superblockChecksumLength(s.blockSize())
		checksumBytes := make([]byte, checksumLength)
		if err := readAt(b, checksumBytes, superOffset); err != nil {
			return nil, fmt.Errorf("unable to read EROFS checksum area: %w", err)
		}
		if err := verifySuperblockChecksum(s, checksumBytes); err != nil {
			return nil, err
		}
	}
	if blocksize != 0 && blocksize != s.blockSize() {
		return nil, fmt.Errorf("requested block size %d does not match superblock block size %d", blocksize, s.blockSize())
	}

	result := &FileSystem{superblock: s, backend: b, size: size, blocksize: s.blockSize()}
	if err := result.parseCompressionConfig(s); err != nil {
		return nil, err
	}
	if _, err := result.getInode(uint64(s.rootNID)); err != nil {
		return nil, fmt.Errorf("unable to read root inode: %w", err)
	}
	return result, nil
}

func Create(_ backend.Storage, _, _, _ int64) (*FileSystem, error) {
	return nil, filesystem.ErrNotImplemented
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
	return nil
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
		visibleEntries := make([]*directoryEntry, 0, len(entries))
		for _, entry := range entries {
			if entry.name == "." || entry.name == ".." {
				continue
			}
			visibleEntries = append(visibleEntries, entry)
		}
		return &directoryFile{filesystem: f, node: node, name: path.Base(p), entries: visibleEntries}, nil
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
	if node.dataLayout == dataLayoutCompressedFull || node.dataLayout == dataLayoutCompressedCompact {
		read := 0
		for read < len(b) {
			currentOffset := offset + int64(read)
			extent, err := f.mapCompressed(node, currentOffset, false)
			if err != nil {
				return read, err
			}
			available := extent.logicalStart + extent.logicalLength - currentOffset
			if available <= 0 {
				return read, fmt.Errorf("invalid compressed extent for inode %d", node.nid)
			}
			if available > int64(len(b)-read) {
				available = int64(len(b) - read)
			}
			target := b[read : read+int(available)]
			if extent.fragment {
				if f.superblock.packedNID == 0 || f.superblock.packedNID == node.nid {
					return read, fmt.Errorf("invalid packed inode for fragment of inode %d", node.nid)
				}
				packedNode, err := f.getInode(f.superblock.packedNID)
				if err != nil {
					return read, err
				}
				packedOffset := node.compressed.fragmentOffset + (currentOffset - extent.logicalStart)
				if packedOffset < 0 || packedOffset+int64(len(target)) > packedNode.size {
					return read, fmt.Errorf("fragment range is outside packed inode for inode %d", node.nid)
				}
				n, err := f.readInodeData(packedNode, target, packedOffset)
				read += n
				if err != nil {
					return read, err
				}
				continue
			}
			encoded := make([]byte, extent.physicalLength)
			if err := readAt(f.backend, encoded, extent.physicalStart); err != nil {
				return read, fmt.Errorf("unable to read compressed data for inode %d: %w", node.nid, err)
			}
			start := currentOffset - extent.logicalStart
			if extent.algorithm == compressionShifted {
				if extent.logicalLength > extent.physicalLength || start+int64(len(target)) > extent.logicalLength {
					return read, fmt.Errorf("invalid shifted extent for inode %d", node.nid)
				}
				copy(target, encoded[start:start+int64(len(target))])
				read += len(target)
				continue
			}
			if extent.algorithm == compressionInterlaced {
				if extent.logicalLength > extent.physicalLength || start+int64(len(target)) > extent.logicalLength {
					return read, fmt.Errorf("invalid interlaced extent for inode %d", node.nid)
				}
				skip := int((extent.logicalStart + start) % f.blockSize())
				count := len(target)
				right := int(f.blockSize()) - skip
				if right > count {
					right = count
				}
				copy(target, encoded[skip:skip+right])
				copy(target[right:], encoded[:count-right])
				read += count
				continue
			}
			if extent.algorithm != compressionLZMA {
				return read, fmt.Errorf("unsupported compression algorithm %d for inode %d", extent.algorithm, node.nid)
			}
			decoded, err := f.decompressLZMA(encoded, extent.logicalLength)
			if err != nil {
				return read, err
			}
			copy(target, decoded[start:start+int64(len(target))])
			read += len(target)
		}
		return read, nil
	}
	if node.isDir() || node.mode&0xF000 == linuxModeRegular || node.isSymlink() {
		total := 0
		nonInlineSize := node.size
		if node.dataLayout == dataLayoutFlatInline {
			tailSize := node.size % f.blockSize()
			nonInlineSize = node.size - tailSize
		}
		if offset < nonInlineSize {
			location := int64(node.startBlock)*f.blockSize() + offset
			readLength := min(int64(len(b)), nonInlineSize-offset)
			if location < 0 || location+readLength > f.size {
				return 0, fmt.Errorf("inode %d data range is outside the image", node.nid)
			}
			if err := readAt(f.backend, b[:readLength], location); err != nil {
				return total, err
			}
			total += int(readLength)
			if readLength == int64(len(b)) {
				return total, nil
			}
			b = b[readLength:]
			offset += readLength
		}
		if node.dataLayout != dataLayoutFlatInline {
			return 0, fmt.Errorf("inode %d data range is outside the image", node.nid)
		}
		inodeOffset, err := f.inodeOffset(node.nid)
		if err != nil {
			return total, err
		}
		inlineStart := inodeOffset + int64(node.inodeSize+node.xattrSize) + (offset - nonInlineSize)
		if inlineStart < inodeOffset || inlineStart+int64(len(b)) > f.size ||
			inlineStart/f.blockSize() != (inlineStart+int64(len(b))-1)/f.blockSize() {
			return total, fmt.Errorf("inode %d inline data crosses or exceeds a block boundary", node.nid)
		}
		if err := readAt(f.backend, b, inlineStart); err != nil {
			return total, err
		}
		return total + len(b), nil
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
