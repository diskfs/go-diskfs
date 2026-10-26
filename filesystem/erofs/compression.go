package erofs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ulikunitz/xz/lzma"
)

func (f *FileSystem) parseCompressionConfig(s *superblock) error {
	if s.featureIncompat&featureIncompatComprCfgs == 0 {
		return nil
	}
	const allCompressionAlgorithms uint16 = (1 << 4) - 1
	if unknown := s.availableComprAlgs &^ allCompressionAlgorithms; unknown != 0 {
		return fmt.Errorf("unsupported compression algorithms 0x%04x", unknown)
	}
	offset := int64(superOffset + s.superblockSize())
	for algorithm := uint8(0); algorithm < 4; algorithm++ {
		if s.availableComprAlgs&(1<<algorithm) == 0 {
			continue
		}
		offset = (offset + 3) &^ 3
		lengthBytes := make([]byte, 2)
		if err := readAt(f.backend, lengthBytes, offset); err != nil {
			return fmt.Errorf("unable to read compression configuration length: %w", err)
		}
		length := int(binary.LittleEndian.Uint16(lengthBytes))
		if length == 0 {
			return fmt.Errorf("empty compression configuration for algorithm %d", algorithm)
		}
		config := make([]byte, length)
		if err := readAt(f.backend, config, offset+2); err != nil {
			return fmt.Errorf("unable to read compression configuration for algorithm %d: %w", algorithm, err)
		}
		if algorithm == compressionLZMA {
			if length != 14 {
				return fmt.Errorf("invalid LZMA configuration length %d", length)
			}
			dictSize := binary.LittleEndian.Uint32(config[0:4])
			format := binary.LittleEndian.Uint16(config[4:6])
			if dictSize < 4096 || dictSize > 8*1024*1024 || format != 0 {
				return fmt.Errorf("invalid LZMA configuration dictionary size %d format %d", dictSize, format)
			}
			for _, reserved := range config[6:] {
				if reserved != 0 {
					return fmt.Errorf("nonzero reserved byte in LZMA configuration")
				}
			}
			f.lzmaDictSize = dictSize
		}
		offset += int64(2 + length)
	}
	return nil
}

func (f *FileSystem) decompressLZMA(encoded []byte, decodedLength int64) ([]byte, error) {
	input := encoded
	for len(input) > 0 && input[0] == 0 {
		input = input[1:]
	}
	if len(input) == 0 || decodedLength <= 0 {
		return nil, fmt.Errorf("invalid LZMA compressed data")
	}
	dictSize := f.lzmaDictSize
	if dictSize == 0 {
		dictSize = 8 * 1024 * 1024
	}
	headerSize := decodedLength
	header := make([]byte, lzma.HeaderLen)
	header[0] = ^input[0]
	binary.LittleEndian.PutUint32(header[1:5], dictSize)
	binary.LittleEndian.PutUint64(header[5:13], uint64(headerSize))
	classic := append([]byte{0}, input[1:]...)
	reader, err := lzma.ReaderConfig{DictCap: int(dictSize)}.NewReader(io.MultiReader(bytes.NewReader(header), bytes.NewReader(classic)))
	if err != nil {
		return nil, fmt.Errorf("unable to initialize LZMA decoder: %w", err)
	}
	decoded := make([]byte, decodedLength)
	if n, err := io.ReadFull(reader, decoded); err != nil {
		return nil, fmt.Errorf("unable to decode LZMA data (%d/%d bytes): %w", n, decodedLength, err)
	}
	return decoded, nil
}
