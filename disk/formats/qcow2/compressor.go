package qcow2

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io/ioutil"
)

type compression int8

const (
	compressionZlib compression = 0
	compressionZstd             = 1
)

// Compressor defines a compressor. Fulfilled by various implementations in this package
type Compressor interface {
	compress([]byte) ([]byte, error)
	decompress([]byte) ([]byte, error)
	flavour() compression
}

// CompressorZlib zlib compression
type CompressorZlib struct {
	CompressionLevel int
}

func (c CompressorZlib) compress(in []byte) ([]byte, error) {
	var b bytes.Buffer
	zl, err := zlib.NewWriterLevel(&b, int(c.CompressionLevel))
	if err != nil {
		return nil, fmt.Errorf("Error creating zlib compressor: %v", err)
	}
	if _, err := zl.Write(in); err != nil {
		return nil, err
	}
	if err := zl.Flush(); err != nil {
		return nil, err
	}
	if err := zl.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
func (c CompressorZlib) decompress(in []byte) ([]byte, error) {
	b := bytes.NewReader(in)
	zl, err := zlib.NewReader(b)
	if err != nil {
		return nil, fmt.Errorf("Error creating zlib decompressor: %v", err)
	}
	p, err := ioutil.ReadAll(zl)
	if err != nil {
		return nil, fmt.Errorf("Error decompressing: %v", err)
	}
	return p, nil
}
func (c CompressorZlib) flavour() compression {
	return compressionZlib
}

func newCompressor(flavour compression) (Compressor, error) {
	var c Compressor
	switch flavour {
	case compressionZlib:
		c = &CompressorZlib{}
	case compressionZstd:
		return nil, fmt.Errorf("zstd compression not yet supported")
	default:
		return nil, fmt.Errorf("Unknown compression type: %d", flavour)
	}
	return c, nil
}
