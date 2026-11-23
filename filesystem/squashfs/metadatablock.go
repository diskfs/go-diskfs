package squashfs

import (
	"encoding/binary"
	"fmt"
	"io"
)

type metadatablock struct {
	compressed bool
	data       []byte
}

const (
	minMetadataBlockSize      = 3
	standardMetadataBlocksize = 8192
)

func getMetadataSize(b []byte) (size uint16, compressed bool, err error) {
	if len(b) < 2 {
		return 0, false, fmt.Errorf("cannot read size of metadata block with %d bytes, must have minimum %d", len(b), 2)
	}
	header := binary.LittleEndian.Uint16(b[:2])
	size = header & 0x7fff
	compressed = header&0x8000 != 0x8000
	return size, compressed, nil
}

// FIXME this isn't used anywhere except in the test code
func parseMetadata(b []byte, c Compressor) (block *metadatablock, err error) {
	if len(b) < minMetadataBlockSize {
		return nil, fmt.Errorf("metadata block was of len %d, less than minimum %d", len(b), minMetadataBlockSize)
	}
	size, compressed, err := getMetadataSize(b[:2])
	if err != nil {
		return nil, fmt.Errorf("error reading metadata header: %v", err)
	}
	if len(b) < int(2+size) {
		return nil, fmt.Errorf("metadata header said size should be %d but was only %d", size, len(b)-2)
	}
	data := b[2 : 2+size]
	if compressed {
		data, err = c.decompress(data, int(metadataBlockSize))
		if err != nil {
			return nil, fmt.Errorf("decompress error: %w", err)
		}
	}
	return &metadatablock{
		compressed: compressed,
		data:       data,
	}, nil
}

func (m *metadatablock) toBytes(c Compressor) ([]byte, error) {
	var (
		header uint16
		data   = m.data
		err    error
	)
	if !m.compressed {
		header |= 0x8000
	} else {
		data, err = c.compress(m.data)
		if err != nil {
			return nil, fmt.Errorf("compression error: %v", err)
		}
	}
	b := make([]byte, 2, 2+len(data))
	header |= uint16(len(data))
	binary.LittleEndian.PutUint16(b[:2], header)
	b = append(b, data...)
	return b, nil
}

func (fs *FileSystem) readMetaBlock(r io.ReaderAt, c Compressor, location int64) (data []byte, size uint16, err error) {
	return fs.cache.get(location, func() (data []byte, size uint16, err error) {
		// read bytes off the reader to determine how big it is and if compressed
		b := make([]byte, 2)
		_, _ = r.ReadAt(b, location)
		size, compressed, err := getMetadataSize(b)
		if err != nil {
			return nil, 0, fmt.Errorf("error getting size and compression for metadata block at %d: %v", location, err)
		}
		b = make([]byte, size)
		read, err := r.ReadAt(b, location+2)
		if err != nil && err != io.EOF {
			return nil, 0, fmt.Errorf("unable to read metadata block of size %d at location %d: %v", size, location, err)
		}
		if read != len(b) {
			return nil, 0, fmt.Errorf("read %d instead of expected %d bytes for metadata block at location %d", read, size, location)
		}
		data = b
		if compressed {
			if c == nil {
				return nil, 0, fmt.Errorf("metadata block at %d compressed, but no compressor provided", location)
			}
			data, err = c.decompress(b, int(metadataBlockSize))
			if err != nil {
				return nil, 0, fmt.Errorf("decompress error: %w", err)
			}
		}
		return data, size + 2, nil
	})
}

// readMetadata read as many bytes of metadata as required for the given size, with the byteOffset provided as a starting
// point into the first block. Can read multiple blocks if necessary, e.g. if a block is 8192 bytes (standard), and
// requests to read 500 bytes beginning at offset 8000 into the first block.
// It returns exactly size bytes, in a new slice the caller may keep or modify: copying up to the end of the block
// instead would cost up to a whole block per call, e.g. for every inode of a directory being listed.
func (fs *FileSystem) readMetadata(r io.ReaderAt, c Compressor, firstBlock int64, initialBlockOffset uint32, byteOffset uint16, size int) ([]byte, error) {
	blockOffset := int(initialBlockOffset)
	m, read, err := fs.readMetaBlock(r, c, firstBlock+int64(blockOffset))
	if err != nil {
		return nil, err
	}
	if int(byteOffset) > len(m) {
		return nil, fmt.Errorf("offset %d is beyond the %d bytes of metadata block at %d", byteOffset, len(m), firstBlock+int64(blockOffset))
	}
	m = m[byteOffset:]
	b := make([]byte, 0, size)
	b = append(b, m[:min(len(m), size)]...)
	// do we have any more to read?
	for len(b) < size {
		blockOffset += int(read)
		m, read, err = fs.readMetaBlock(r, c, firstBlock+int64(blockOffset))
		if err != nil {
			return nil, err
		}
		b = append(b, m[:min(len(m), size-len(b))]...)
	}
	return b, nil
}
