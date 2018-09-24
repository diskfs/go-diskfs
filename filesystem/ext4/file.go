package ext4

import (
	"errors"
	"fmt"
	"io"
)

// File represents a single file in an ext4 filesystem
type File struct {
	*directoryEntry
	*inode
	isReadWrite bool
	isAppend    bool
	offset      int64
	filesystem  *FileSystem
	extents     extents
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF
// reads from the last known offset in the file from last read or write
// use Seek() to set at a particular point
func (fl *File) Read(b []byte) (int, error) {
	var (
		fileSize  = int64(fl.size)
		blocksize = uint64(fl.filesystem.superblock.blockSize)
	)
	if fl.offset >= fileSize {
		return 0, io.EOF
	}

	// Calculate the number of bytes to read
	bytesToRead := int64(len(b))
	if fl.offset+bytesToRead > fileSize {
		bytesToRead = fileSize - fl.offset
	}

	// Create a buffer to hold the bytes to be read
	readBytes := int64(0)
	b = b[:bytesToRead]

	// the offset given for reading is relative to the file, so we need to calculate
	// where these are in the extents relative to the file
	readStartBlock := uint64(fl.offset) / blocksize
	for _, e := range fl.extents {
		// if the last block of the extent is before the first block we want to read, skip it
		if uint64(e.fileBlock)+uint64(e.count) < readStartBlock {
			continue
		}
		// extentSize is the number of bytes on the disk for the extent
		extentSize := int64(e.count) * int64(blocksize)
		// where do we start and end in the extent?
		startPositionInExtent := fl.offset - int64(e.fileBlock)*int64(blocksize)
		leftInExtent := extentSize - startPositionInExtent
		// how many bytes are left to read
		toReadInOffset := bytesToRead - readBytes
		if toReadInOffset > leftInExtent {
			toReadInOffset = leftInExtent
		}
		// read those bytes
		startPosOnDisk := e.startingBlock*blocksize + uint64(startPositionInExtent)
		b2 := make([]byte, toReadInOffset)
		read, err := fl.filesystem.file.ReadAt(b2, int64(startPosOnDisk))
		if err != nil {
			return int(readBytes), fmt.Errorf("failed to read bytes: %v", err)
		}
		copy(b[readBytes:], b2[:read])
		readBytes += int64(read)
		fl.offset += int64(read)

		if readBytes >= bytesToRead {
			break
		}
	}
	var err error
	if fl.offset >= fileSize {
		err = io.EOF
	}

	return int(readBytes), err
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// returns a non-nil error when n != len(b)
// writes to the last known offset in the file from last read or write
// use Seek() to set at a particular point
//
//nolint:revive // params not used because still read-only, will be used in the future when read-write
func (fl *File) Write(p []byte) (int, error) {
	return 0, errors.New("not implemented")
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
