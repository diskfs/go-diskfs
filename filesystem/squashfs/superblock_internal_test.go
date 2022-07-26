package squashfs

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

var rootInodeTests = []struct {
	number uint64
	inode  *inodeRef
}{
	// we built these test numbers manually too...
	{0x66447788, &inodeRef{block: 0x6644, offset: 0x7788}},
	{0xaabb0000, &inodeRef{block: 0xaabb, offset: 0x0000}},
}

var superblockFlagsTests = []struct {
	b     []byte
	flags *superblockFlags
	err   error
}{
	// we built these test numbers manually...
	{[]byte{1}, nil, fmt.Errorf("received 1")},
	{[]byte{1, 2, 3}, nil, fmt.Errorf("received 3")},
	// all of them flagged
	{[]byte{0xfb, 0xf}, &superblockFlags{true, true, true, true, true, true, true, true, true, true, true}, nil},
	// none of them flagged
	{[]byte{0x0, 0x0}, &superblockFlags{}, nil},
	// first 7 flagged
	{[]byte{0xfb, 0x0}, &superblockFlags{true, true, true, true, true, true, true, false, false, false, false}, nil},
	// last 4 flagged
	{[]byte{0x0, 0xf}, &superblockFlags{false, false, false, false, false, false, false, true, true, true, true}, nil},
}

func testGetValidSuperblock() ([]byte, *superblock, error) {
	file, err := os.Open(SquashfsUncompressedfile)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening uncompressed file %s: %v", SquashfsUncompressedfile, err)
	}
	defer file.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("could not open file %s to read: %v", Squashfsfile, err)
	}
	b := make([]byte, superblockSize)
	read, err := file.ReadAt(b, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("could not read from %s: %v", Squashfsfile, err)
	}
	if read != len(b) {
		return nil, nil, fmt.Errorf("read %d bytes instead of expected %d from %s", read, len(b), Squashfsfile)
	}
	return b, testValidSuperblockUncompressed, nil
}

func TestParseFlags(t *testing.T) {
	tests := superblockFlagsTests
	//nolint:dupl // these functions vary slightly from one another
	for i, tt := range tests {
		flags, err := parseFlags(tt.b)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case (flags == nil && tt.flags != nil) || (flags != nil && tt.flags == nil) || (flags != nil && tt.flags != nil && *flags != *tt.flags):
			t.Errorf("%d: mismatched results, actual then expected", i)
			t.Logf("%v", flags)
			t.Logf("%v", tt.flags)
		}
	}
}

func TestParseSuperblock(t *testing.T) {
	b, s, err := testGetValidSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		b   []byte
		s   *superblock
		err error
	}{
		// too many bytes
		{append(b, []byte{1, 2, 3}...), nil, fmt.Errorf("superblock had %d bytes", superblockSize+3)},
		// not enough bytes
		{b[2:], nil, fmt.Errorf("superblock had %d bytes", superblockSize-2)},
		// corrupted magic bytes
		{append([]byte{0x10, 0x20}, b[2:]...), nil, fmt.Errorf("superblock had magic of")},
		// valid
		{b, s, nil},
	}
	for i, tt := range tests {
		s, err := parseSuperblock(tt.b)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case (s == nil && tt.s != nil) || (s != nil && tt.s == nil) || (s != nil && tt.s != nil && !s.equal(tt.s)):
			t.Errorf("%d: mismatched results, actual then expected", i)
			t.Logf("%#v", s)
			t.Logf("%#v", tt.s)
		}
	}
}
func TestSuperblockToBytes(t *testing.T) {
	b, s, err := testGetValidSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	output := s.toBytes()
	// strip the dates, which are in positions 8:12
	copy(b[8:12], []byte{0, 0, 0, 0})
	copy(output[8:12], []byte{0, 0, 0, 0})
	if !bytes.Equal(output, b) {
		t.Errorf("mismatched bytes, actual then expected")
		t.Logf("%x", output)
		t.Logf("%x", b)
	}
}

func TestParseRootInode(t *testing.T) {
	tests := rootInodeTests
	for i, tt := range tests {
		output := parseRootInode(tt.number)
		switch {
		case output == nil:
			t.Errorf("%d: unexpected nil output", i)
		case (output == nil && tt.inode != nil) || (output != nil && tt.inode == nil) || (*output != *tt.inode):
			t.Errorf("%d: mismatched results, actual then expected", i)
			t.Logf("%v", output)
			t.Logf("%v", tt.inode)
		}
	}
}
func TestRootInodeToUint64(t *testing.T) {
	tests := rootInodeTests
	for i, tt := range tests {
		output := tt.inode.toUint64()
		if output != tt.number {
			t.Errorf("%d: mismatched results, actual then expected", i)
			t.Logf("%v", output)
			t.Logf("%v", tt.number)
		}
	}
}

func TestSuperblockFlagsUint16(t *testing.T) {
	tests := superblockFlagsTests
	for i, tt := range tests {
		if tt.flags == nil {
			continue
		}
		b := tt.flags.bytes()
		if !bytes.Equal(b, tt.b) {
			t.Errorf("%d: mismatched results, actual then expected", i)
			t.Logf("%v", b)
			t.Logf("%v", tt.b)
		}
	}
}
