package ext4

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"

	"github.com/diskfs/go-diskfs/filesystem"
)

var _ filesystem.File = (*File)(nil)
var _ fs.File = (*File)(nil)

// File represents a single file in an ext4 filesystem
type File struct {
	*inode
	isReadWrite bool
	isAppend    bool
	offset      int64
	filesystem  *FileSystem
	extents     extents
	fileType    directoryFileType
	filename    string
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF
// reads from the last known offset in the file from last read or write
// use Seek() to set at a particular point
func (fl *File) Read(b []byte) (int, error) {
	if fl.fileType == dirFileTypeDirectory {
		return 0, fmt.Errorf("cannot read directory")
	}
	if fl.offset < 0 || fl.size > math.MaxInt64 {
		return 0, fmt.Errorf("invalid ext4 file size or offset")
	}
	fileSize := int64(fl.size)
	if fl.offset >= fileSize {
		return 0, io.EOF
	}
	if len(b) == 0 {
		return 0, nil
	}
	blockSize := int64(fl.filesystem.superblock.blockSize)
	if blockSize < 1024 || blockSize > 65536 || blockSize&(blockSize-1) != 0 {
		return 0, fmt.Errorf("invalid ext4 block size")
	}
	var previousEnd int64
	for _, extent := range fl.extents {
		count := int64(extent.count)
		if count > 32768 {
			count -= 32768
		}
		start := int64(extent.fileBlock) * blockSize
		end := start + count*blockSize
		if count == 0 || start < previousEnd {
			return 0, fmt.Errorf("invalid ext4 extent order or length")
		}
		if extent.count <= 32768 && (extent.startingBlock > uint64(math.MaxInt64)/uint64(blockSize) ||
			int64(extent.startingBlock)*blockSize > math.MaxInt64-count*blockSize) {
			return 0, fmt.Errorf("ext4 extent address overflows")
		}
		previousEnd = end
	}
	want := int64(len(b))
	if want > fileSize-fl.offset {
		want = fileSize - fl.offset
	}
	start, end := fl.offset, fl.offset+want
	clear(b[:want])
	for _, extent := range fl.extents {
		count := int64(extent.count)
		unwritten := count > 32768
		if unwritten {
			count -= 32768
		}
		extentStart := int64(extent.fileBlock) * blockSize
		extentEnd := extentStart + count*blockSize
		if extentStart >= end {
			// extents are validated to be sorted, so nothing further can overlap the window
			break
		}
		overlapStart, overlapEnd := max(start, extentStart), min(end, extentEnd)
		if overlapStart >= overlapEnd || unwritten {
			continue
		}
		destination := b[overlapStart-start : overlapEnd-start]
		physicalOffset := int64(extent.startingBlock)*blockSize + overlapStart - extentStart
		n, err := fl.filesystem.backend.ReadAt(destination, physicalOffset)
		if n < 0 || n > len(destination) {
			fl.offset = overlapStart
			return int(overlapStart - start), errors.Join(fmt.Errorf("ext4 backend returned invalid read count"), err)
		}
		if n != len(destination) {
			err = errors.Join(io.ErrUnexpectedEOF, err)
		} else if err == io.EOF {
			// io.ReaderAt may report io.EOF together with a complete read at the end of the backend
			err = nil
		}
		if err != nil {
			fl.offset = overlapStart + int64(n)
			return int(fl.offset - start), fmt.Errorf("failed to read extent bytes: %w", err)
		}
	}
	fl.offset = end
	if end == fileSize {
		return int(want), io.EOF
	}
	return int(want), nil
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// returns a non-nil error when n != len(b)
// writes to the last known offset in the file from last read or write
// use Seek() to set at a particular point
func (fl *File) Write(b []byte) (int, error) {
	var (
		fileSize           = int64(fl.size)
		originalFileSize   = int64(fl.size)
		originalBlockCount = fl.blocks
		blocksize          = uint64(fl.filesystem.superblock.blockSize)
	)
	if !fl.isReadWrite {
		return 0, fmt.Errorf("file is not open for writing")
	}

	// if adding these bytes goes past the filesize, update the inode filesize to the new size and write the inode
	// if adding these bytes goes past the total number of blocks, add more blocks, update the inode block count and write the inode
	// if the offset is greater than the filesize, update the inode filesize to the offset
	if fl.offset >= fileSize {
		fl.size = uint64(fl.offset)
	}

	// Calculate the number of bytes to write
	bytesToWrite := int64(len(b))

	offsetAfterWrite := fl.offset + bytesToWrite
	if offsetAfterWrite > int64(fl.size) {
		fl.size = uint64(fl.offset + bytesToWrite)
	}

	// calculate the number of blocks in the file post-write
	newBlockCount := fl.size / blocksize
	if fl.size%blocksize > 0 {
		newBlockCount++
	}
	allocatedBlocks := fl.extents.blockCount()
	if newBlockCount > allocatedBlocks {
		// Calculate the previously accumulated tree metadata blocks so we don't lose them.
		// fl.blocks (in its current unit) = data blocks + meta blocks from prior writes.
		var oldMetaBlocks uint64
		if fl.filesystemBlocks {
			oldMetaBlocks = fl.blocks - allocatedBlocks
		} else {
			oldMetaBlocks = fl.blocks*512/blocksize - allocatedBlocks
		}

		newExtents, err := fl.filesystem.allocateExtents(fl.size, &fl.extents)
		if err != nil {
			return 0, fmt.Errorf("could not allocate disk space for file %w", err)
		}
		extentTreeParsed, metaBlocks, err := extendExtentTree(fl.inode.extents, newExtents, fl.filesystem, nil)
		if err != nil {
			return 0, fmt.Errorf("could not convert extents into tree: %w", err)
		}
		fl.inode.extents = extentTreeParsed
		updatedExtents, err := fl.inode.extents.blocks(fl.filesystem)
		if err != nil {
			return 0, fmt.Errorf("could not read updated extents: %w", err)
		}
		fl.extents = updatedExtents
		totalMetaBlocks := oldMetaBlocks + metaBlocks
		if fl.filesystemBlocks {
			fl.blocks = newBlockCount + totalMetaBlocks
		} else {
			fl.blocks = (newBlockCount + totalMetaBlocks) * blocksize / 512
		}
	}

	if originalFileSize != int64(fl.size) || originalBlockCount != fl.blocks {
		err := fl.filesystem.writeInode(fl.inode)
		if err != nil {
			return 0, fmt.Errorf("could not write inode: %w", err)
		}
	}

	writtenBytes := int64(0)

	// the offset given for reading is relative to the file, so we need to calculate
	// where these are in the extents relative to the file
	writeStartBlock := uint64(fl.offset) / blocksize

	writableFile, err := fl.filesystem.backend.Writable()
	if err != nil {
		return -1, err
	}

	for _, e := range fl.extents {
		// if the last block of the extent is before the first block we want to write, skip it
		// notice: need equal sign
		if uint64(e.fileBlock)+uint64(e.count) <= writeStartBlock {
			continue
		}
		// extentSize is the number of bytes on the disk for the extent
		extentSize := int64(e.count) * int64(blocksize)
		// where do we start and end in the extent?
		startPositionInExtent := fl.offset - int64(e.fileBlock)*int64(blocksize)

		// Protect
		if startPositionInExtent < 0 || startPositionInExtent > extentSize {
			return int(writtenBytes), fmt.Errorf("write offset %d falls outside extent (fileBlock %d, count %d) of file %s", fl.offset, e.fileBlock, e.count, fl.filename)
		}

		leftInExtent := extentSize - startPositionInExtent
		// how many bytes are left in the extent?
		toWriteInOffset := bytesToWrite - writtenBytes
		if toWriteInOffset > leftInExtent {
			toWriteInOffset = leftInExtent
		}
		// read those bytes
		startPosOnDisk := e.startingBlock*blocksize + uint64(startPositionInExtent)
		b2 := make([]byte, toWriteInOffset)
		copy(b2, b[writtenBytes:])
		written, err := writableFile.WriteAt(b2, int64(startPosOnDisk))
		if err != nil {
			return int(writtenBytes), fmt.Errorf("failed to read bytes: %v", err)
		}
		writtenBytes += int64(written)
		fl.offset += int64(written)

		if written >= len(b) {
			break
		}
	}

	return int(writtenBytes), nil
}

// Seek set the offset to a particular point in the file
func (fl *File) Seek(offset int64, whence int) (int64, error) {
	newOffset := int64(0)
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekEnd:
		newOffset = int64(fl.size) + offset
	case io.SeekCurrent:
		newOffset = fl.offset + offset
	}
	if newOffset < 0 {
		return fl.offset, fmt.Errorf("cannot set offset %d before start of file", offset)
	}
	fl.offset = newOffset
	return fl.offset, nil
}

// Close close a file that is being read
func (fl *File) Close() error {
	*fl = File{}
	return nil
}

// Stat returns a fs.FileInfo structure describing file
func (fl *File) Stat() (fs.FileInfo, error) {
	return &FileInfo{
		modTime: fl.modifyTime,
		name:    fl.filename,
		size:    int64(fl.size),
		isDir:   fl.fileType == dirFileTypeDirectory,
		mode:    fl.permissionsToMode(),
		sys:     fl.stat(),
	}, nil
}
