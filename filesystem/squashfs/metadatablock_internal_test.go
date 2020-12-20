package squashfs

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestGetMetadataSize(t *testing.T) {
	tests := []struct {
		b          []byte
		size       uint16
		compressed bool
		err        error
	}{
		{[]byte{0x25, 0xff}, 0x7f25, false, nil},
		{[]byte{0x25, 0x7f}, 0x7f25, true, nil},
		{[]byte{0x25}, 0, false, fmt.Errorf("Cannot read size of metadata block with 1 bytes, must have minimum 2")},
	}

	for i, tt := range tests {
		size, compressed, err := getMetadataSize(tt.b)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case size != tt.size:
			t.Errorf("%d: mismatched size, actual %d expected %d", i, size, tt.size)
		case compressed != tt.compressed:
			t.Errorf("%d: mismatched compressed, actual '%v' expected '%v'", i, compressed, tt.compressed)
		}
	}
}

func TestParseMetadata(t *testing.T) {
	tests := []struct {
		b     []byte
		c     Compressor
		block *metadatablock
		err   error
	}{
		// not enough bytes
		{[]byte{0x25, 0x75}, nil, nil, fmt.Errorf("Metadata block was of len 2")},
		// header size different than data
		{[]byte{0x20, 0x00, 0xa, 0xb, 0xc}, nil, nil, fmt.Errorf("Metadata header said size should be %d but was only %d", 0x20, 3)},
		// uncompressed
		{[]byte{0x3, 0x80, 0xa, 0xb, 0xc}, nil, &metadatablock{compressed: false, data: []byte{0xa, 0xb, 0xc}}, nil},
		// compressed
		{[]byte{0x3, 0x00, 0xa, 0xb, 0xc}, &testCompressorAddBytes{b: []byte{0x25}}, &metadatablock{compressed: true, data: []byte{0xa, 0xb, 0xc, 0x25}}, nil},
		// compressed with error
		{[]byte{0x3, 0x00, 0xa, 0xb, 0xc}, &testCompressorAddBytes{err: fmt.Errorf("bad error")}, nil, fmt.Errorf("decompress error: bad error")},
	}

	for i, tt := range tests {
		block, err := parseMetadata(tt.b, tt.c)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case (block == nil && tt.block != nil) || (block != nil && tt.block == nil) || (block != nil && tt.block != nil && (block.compressed != tt.block.compressed || bytes.Compare(block.data, tt.block.data) != 0)):
			t.Errorf("%d: mismatched block, actual then expected", i)
			t.Logf("%v", block)
			t.Logf("%v", tt.block)
		}
	}
}

func TestMetadataToBytes(t *testing.T) {
	// func (m *metadatablock) toBytes(c compressor) ([]byte, error)
	tests := []struct {
		b     []byte
		c     Compressor
		block *metadatablock
		err   error
	}{
		// uncompressed
		{[]byte{0x3, 0x80, 0xa, 0xb, 0xc}, nil, &metadatablock{compressed: false, data: []byte{0xa, 0xb, 0xc}}, nil},
		// compressed
		{[]byte{0x3, 0x00, 0xa, 0xb, 0xc}, &testCompressorAddBytes{b: []byte{0x25}}, &metadatablock{compressed: true, data: []byte{0xa, 0xb, 0xc, 0x25}}, nil},
		// error
		{nil, &testCompressorAddBytes{err: fmt.Errorf("bad error")}, &metadatablock{compressed: true, data: []byte{0xa, 0xb, 0xc, 0x25}}, fmt.Errorf("Compression error: bad error")},
	}

	for i, tt := range tests {
		b, err := tt.block.toBytes(tt.c)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case bytes.Compare(b, tt.b) != 0:
			t.Errorf("%d: mismatched bytes, actual then expected", i)
			t.Logf("%v", b)
			t.Logf("%v", tt.b)
		}
	}
}

func TestReadMetaBlock(t *testing.T) {
	tests := []struct {
		b        []byte
		location int64
		c        Compressor
		size     uint16
		err      error
		out      []byte
	}{
		// no compressor, no compression
		{[]byte{0x5, 0x80, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0, nil, 0x5 + 2, nil, []byte{1, 2, 3, 4, 5}},
		// unchanging compressor, yes compression
		{[]byte{0x5, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0, &testCompressorAddBytes{}, 0x5 + 2, nil, []byte{1, 2, 3, 4, 5}},
		// expanding compressor, yes compression
		{[]byte{0x5, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0, &testCompressorAddBytes{b: []byte{0x25}}, 0x5 + 2, nil, []byte{1, 2, 3, 4, 5, 0x25}},
		// no compressor, yes compression
		{[]byte{0x5, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0, nil, 0x0, fmt.Errorf("Metadata block at %d compressed, but no compressor provided", 0), nil},
		// bad size
		{[]byte{0x5}, 0, nil, 0x0, fmt.Errorf("Read %d instead of expected %d bytes for metadata block at location %d", 0, 5, 0), nil},
		// decompression error
		{[]byte{0x5, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0, &testCompressorAddBytes{err: fmt.Errorf("unknown")}, 0x0, fmt.Errorf("decompress error: unknown"), nil},
	}

	for i, tt := range tests {
		b, size, err := readMetaBlock(bytes.NewReader(tt.b), tt.c, tt.location)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case size != tt.size:
			t.Errorf("%d: mismatched size, actual %d expected %d", i, size, tt.size)
		case bytes.Compare(b, tt.out) != 0:
			t.Errorf("%d: mismatched output, actual then expected", i)
			t.Logf("% x", b)
			t.Logf("% x", tt.out)
		}
	}

}
