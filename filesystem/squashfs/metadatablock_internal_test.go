package squashfs

import (
	"bytes"
	"errors"
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
		{[]byte{0x25}, 0, false, fmt.Errorf("cannot read size of metadata block with 1 bytes, must have minimum 2")},
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
		{[]byte{0x25, 0x75}, nil, nil, fmt.Errorf("metadata block was of len 2")},
		// header size different than data
		{[]byte{0x20, 0x00, 0xa, 0xb, 0xc}, nil, nil, fmt.Errorf("metadata header said size should be %d but was only %d", 0x20, 3)},
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
		case (block == nil && tt.block != nil) || (block != nil && tt.block == nil) || (block != nil && tt.block != nil && (block.compressed != tt.block.compressed || !bytes.Equal(block.data, tt.block.data))):
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
		{nil, &testCompressorAddBytes{err: fmt.Errorf("bad error")}, &metadatablock{compressed: true, data: []byte{0xa, 0xb, 0xc, 0x25}}, fmt.Errorf("compression error: bad error")},
	}

	for i, tt := range tests {
		b, err := tt.block.toBytes(tt.c)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case !bytes.Equal(b, tt.b):
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
		{[]byte{0x5, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0, nil, 0x0, fmt.Errorf("metadata block at %d compressed, but no compressor provided", 0), nil},
		// bad size
		{[]byte{0x5}, 0, nil, 0x0, fmt.Errorf("read %d instead of expected %d bytes for metadata block at location %d", 0, 5, 0), nil},
		// decompression error
		{[]byte{0x5, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0, &testCompressorAddBytes{err: fmt.Errorf("unknown")}, 0x0, fmt.Errorf("decompress error: unknown"), nil},
	}

	var fs = &FileSystem{}
	for i, tt := range tests {
		b, size, err := fs.readMetaBlock(bytes.NewReader(tt.b), tt.c, tt.location)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case size != tt.size:
			t.Errorf("%d: mismatched size, actual %d expected %d", i, size, tt.size)
		case !bytes.Equal(b, tt.out):
			t.Errorf("%d: mismatched output, actual then expected", i)
			t.Logf("% x", b)
			t.Logf("% x", tt.out)
		}
	}
}

// TestReadMetaBlockLimit checks that a metadata block may decompress to at most 8 KiB.
func TestReadMetaBlockLimit(t *testing.T) {
	b := []byte{0x5, 0x00, 1, 2, 3, 4, 5}
	// 5 bytes in, metadataBlockSize - 5 added: exactly the limit, then one byte over it
	exact := &testCompressorAddBytes{b: make([]byte, metadataBlockSize-5)}
	over := &testCompressorAddBytes{b: make([]byte, metadataBlockSize-4)}

	fs := &FileSystem{}
	if out, _, err := fs.readMetaBlock(bytes.NewReader(b), exact, 0); err != nil || len(out) != int(metadataBlockSize) {
		t.Errorf("exactly %d bytes: got %d bytes, %v", metadataBlockSize, len(out), err)
	}
	if _, _, err := fs.readMetaBlock(bytes.NewReader(b), over, 0); !errors.Is(err, ErrDecompressedTooLarge) {
		t.Errorf("%d bytes: expected %v, got %v", metadataBlockSize+1, ErrDecompressedTooLarge, err)
	}
}

func TestReadMetadata(t *testing.T) {
	// three uncompressed metadata blocks, of 5, 4 and 3 bytes, at 0, 7 and 13
	table := []byte{
		0x5, 0x80, 1, 2, 3, 4, 5,
		0x4, 0x80, 6, 7, 8, 9,
		0x3, 0x80, 10, 11, 12,
	}
	tests := []struct {
		name        string
		blockOffset uint32
		byteOffset  uint16
		size        int
		out         []byte
		err         string
	}{
		{"within the first block", 0, 1, 2, []byte{2, 3}, ""},
		{"to the end of the first block", 0, 0, 5, []byte{1, 2, 3, 4, 5}, ""},
		{"across two blocks", 0, 3, 4, []byte{4, 5, 6, 7}, ""},
		{"across three blocks", 0, 4, 6, []byte{5, 6, 7, 8, 9, 10}, ""},
		{"from a later block", 7, 1, 2, []byte{7, 8}, ""},
		{"from the end of a block", 0, 5, 2, []byte{6, 7}, ""},
		{"nothing", 0, 2, 0, []byte{}, ""},
		{"offset beyond the block", 0, 6, 1, nil, "offset 6 is beyond the 5 bytes of metadata block at 0"},
		{"beyond the end of the table", 7, 0, 20, nil, "metadata block at 18 compressed, but no compressor provided"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &FileSystem{cache: newLRU(10)}
			b, err := fs.readMetadata(bytes.NewReader(table), nil, 0, tt.blockOffset, tt.byteOffset, tt.size)
			switch {
			case tt.err != "" && (err == nil || !strings.Contains(err.Error(), tt.err)):
				t.Errorf("expected error %q, got %v", tt.err, err)
			case tt.err == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case !bytes.Equal(b, tt.out):
				t.Errorf("got % x, expected % x", b, tt.out)
			}
		})
	}

	// the bytes returned are the caller's own: changing them does not change what the cache returns next
	t.Run("returns a copy", func(t *testing.T) {
		fs := &FileSystem{cache: newLRU(10)}
		b, err := fs.readMetadata(bytes.NewReader(table), nil, 0, 0, 0, 5)
		if err != nil {
			t.Fatal(err)
		}
		b[0] = 0xff
		b, err = fs.readMetadata(bytes.NewReader(table), nil, 0, 0, 0, 5)
		if err != nil {
			t.Fatal(err)
		}
		if b[0] != 1 {
			t.Errorf("cached block changed through a returned slice: got % x", b)
		}
	})
}
