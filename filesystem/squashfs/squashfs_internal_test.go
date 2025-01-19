package squashfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/testhelper"
)

func TestWorkspace(t *testing.T) {
	tests := []struct {
		fs *FileSystem
		ws string
	}{
		{&FileSystem{workspace: ""}, ""},
		{&FileSystem{workspace: "abc"}, "abc"},
	}
	for _, tt := range tests {
		ws := tt.fs.Workspace()
		if ws != tt.ws {
			t.Errorf("Mismatched workspace, actual '%s', expected '%s'", ws, tt.ws)
		}
	}
}

func TestFSType(t *testing.T) {
	fs := &FileSystem{}
	fsType := fs.Type()
	if fsType != filesystem.TypeSquashfs {
		t.Errorf("Mismatched type, actual '%v', expected '%v'", fsType, filesystem.TypeSquashfs)
	}
}

func TestValidateBlocksize(t *testing.T) {
	tests := []struct {
		size int64
		err  error
	}{
		{2, fmt.Errorf("blocksize %d too small, must be at least %d", 2, minBlocksize)},
		{minBlocksize - 1, fmt.Errorf("blocksize %d too small, must be at least %d", minBlocksize-1, minBlocksize)},
		{minBlocksize, nil},
		{maxBlocksize + 1, fmt.Errorf("blocksize %d too large, must be no more than %d", maxBlocksize+1, maxBlocksize)},
		{maxBlocksize, nil},
	}
	for _, tt := range tests {
		err := validateBlocksize(tt.size)
		if (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())) {
			t.Errorf("Mismatched errors for %d, actual then expected", tt.size)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		}
	}
	for i := 12; i <= 20; i++ {
		size := int64(math.Exp2(float64(i)))
		err := validateBlocksize(size)
		if err != nil {
			t.Errorf("unexpected erorr for size %d: %v", size, err)
		}
	}
}

func TestParseXAttrsTable(t *testing.T) {
	// parseXattrsTable(bUIDXattr, bIndex []byte, offset uint64, c compressor) (*xAttrTable, error) {
	b, err := testGetInodeMetabytes()
	if err != nil {
		t.Fatalf("error getting metadata bytes: %v", err)
	}
	startUID := testValidSuperblockUncompressed.idTableStart - testMetaOffset
	startXattrsID := testValidSuperblockUncompressed.xattrTableStart - testMetaOffset
	bUIDXattr := b[startUID:startXattrsID]
	xattrIDBytes := testValidSuperblockUncompressed.size - testValidSuperblockUncompressed.xattrTableStart
	// dir table + fragment table
	bIndex := b[startUID : startUID+xattrIDBytes] // xattr index table

	// entries in the xattr ID table are offset from beginning of disk, not from xattr table
	//   so need offset of bUIDXattr from beginning of disk to make use of it
	table, err := parseXattrsTable(bUIDXattr, bIndex, map[uint32]uint32{0: 0}, nil)
	if err != nil {
		t.Fatalf("error reading xattrs table: %v", err)
	}
	expectedEntries := 1
	if len(table.list) != expectedEntries {
		t.Errorf("Mismatched entries, has %d instead of expected %d", len(table.list), expectedEntries)
	}
}

func TestReadXAttrsTable(t *testing.T) {
	s := &superblock{
		xattrTableStart: 2000,
		idTableStart:    1000,
		superblockFlags: superblockFlags{
			uncompressedXattrs: true,
		},
	}
	table := []byte{
		0x30, 0x80, // 0x30 bytes of uncompressed data
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
		0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
	}

	idTable := make([]byte, 2*xAttrIDEntrySize+2) // 2 entries plus the metadata block header

	xAttrIDStart := int64(s.xattrTableStart) - int64(len(idTable)) //
	xAttrStart := xAttrIDStart - int64(len(table))                 //

	list := []*xAttrIndex{
		{uint64(xAttrStart), 0x02, 0x10},
		{uint64(xAttrStart) + 0x10, 0x01, 0x10},
	}

	idTable[0] = uint8(len(idTable) - 2)
	idTable[1] = 0x80
	binary.LittleEndian.PutUint64(idTable[2:10], list[0].pos)
	binary.LittleEndian.PutUint32(idTable[10:14], list[0].count)
	binary.LittleEndian.PutUint32(idTable[14:18], list[0].size)
	binary.LittleEndian.PutUint64(idTable[18:26], list[1].pos)
	binary.LittleEndian.PutUint32(idTable[26:30], list[1].count)
	binary.LittleEndian.PutUint32(idTable[30:34], list[1].size)

	indexHeader := make([]byte, 16)
	binary.LittleEndian.PutUint64(indexHeader[:8], uint64(xAttrStart))
	binary.LittleEndian.PutUint32(indexHeader[8:12], 2)
	indexBody := make([]byte, 8)
	binary.LittleEndian.PutUint64(indexBody, uint64(xAttrIDStart))

	testFile := &testhelper.FileImpl{
		Reader: func(b []byte, offset int64) (int, error) {
			var b2 []byte
			switch offset {
			case xAttrStart: // xAttr meta block header
				b2 = table
			case xAttrStart + 2: // xAttr meta block
				b2 = table[2:]
			case xAttrIDStart: // index block heaeer
				b2 = idTable
			case xAttrIDStart + 2: // index block
				b2 = idTable[2:]
			case int64(s.xattrTableStart): // xattr ID block
				b2 = indexHeader
			case int64(s.xattrTableStart) + int64(xAttrHeaderSize): // xattr ID block minus the header
				b2 = indexBody
			}
			copy(b, b2)
			count := len(b2)
			if len(b) < len(b2) {
				count = len(b)
			}
			return count, io.EOF
		},
	}
	expectedTable := &xAttrTable{
		data: table[2:],
		list: list,
	}
	xtable, err := readXattrsTable(s, testFile, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	switch {
	case xtable == nil:
		t.Errorf("unexpected xtable nil")
	case !bytes.Equal(xtable.data, expectedTable.data):
		t.Errorf("Mismatched xtable.data, actual then expected")
		t.Logf("% x", xtable.data)
		t.Logf("% x", expectedTable.data)
	case len(xtable.list) != len(expectedTable.list):
		t.Errorf("Mismatched list, actual then expected")
		t.Logf("%#v", xtable.list)
		t.Logf("%#v", expectedTable.list)
	}
}

func TestReadFragmentTable(t *testing.T) {
	fs, _, err := testGetFilesystem(nil)
	if err != nil {
		t.Fatalf("unable to read test file: %v", err)
	}
	entries, err := readFragmentTable(fs.superblock, fs.backend, fs.compressor)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != len(testFragEntries) {
		t.Errorf("Mismatched entries, actual %d expected %d", len(entries), len(testFragEntries))
	} else {
		for i, e := range entries {
			if *e != *testFragEntries[i] {
				t.Errorf("Mismatched entry %d, actual then expected", i)
				t.Logf("%#v", *e)
				t.Logf("%#v", *testFragEntries[i])
			}
		}
	}
}

func TestReadDirectory(t *testing.T) {
	fs, _, err := testGetFilesystem(nil)
	if err != nil {
		t.Fatalf("error getting valid test filesystem: %v", err)
	}

	tests := []struct {
		p       string
		err     error
		entries []*directoryEntry
	}{
		{"/a/b/c", nil, []*directoryEntry{
			{
				isSubdirectory: true,
				name:           "d",
				size:           5,
			},
		}},
	}
	for i, tt := range tests {
		entries, err := fs.readDirectory(tt.p)
		// just check that it called getDirectoryEntries
		switch {
		case (err != nil && tt.err == nil) || (err == nil && tt.err != nil):
			t.Errorf("%d: Mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case len(entries) != len(tt.entries):
			t.Errorf("%d: mismatched entries, actual then expected", i)
			t.Logf("%v", entries)
			t.Logf("%v", tt.entries)
		}
	}
}

func TestReadBlock(t *testing.T) {
	location := int64(10000)
	smallLocation := int64(2000)
	size := uint32(20)
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	testFile := &testhelper.FileImpl{
		Reader: func(b []byte, offset int64) (int, error) {
			switch {
			case offset == location:
				copy(b, data)
				count := len(data)
				if len(b) < len(data) {
					count = len(b)
				}
				return count, io.EOF
			case offset == smallLocation:
				copy(b, data[:10])
				return 10, nil
			default:
				return 0, fmt.Errorf("unknown location")
			}
		},
	}

	tests := []struct {
		location   int64
		compressed bool
		compressor Compressor
		data       []byte
		err        error
	}{
		{location, false, nil, data, nil},
		{smallLocation, false, nil, nil, fmt.Errorf("read %d bytes instead of expected %d", 10, 20)},
		{location + 25, false, nil, nil, fmt.Errorf("unknown location")},
		{location, true, &testCompressorAddBytes{b: []byte{0x25}}, append(data, 0x25), nil},
		{location, true, &testCompressorAddBytes{err: fmt.Errorf("foo")}, nil, fmt.Errorf("foo")},
	}
	for i, tt := range tests {
		fs := &FileSystem{
			backend:    file.New(testFile, true),
			compressor: tt.compressor,
		}
		b, err := fs.readBlock(tt.location, tt.compressed, size)
		switch {
		case (err != nil && tt.err == nil) || (err == nil && tt.err != nil):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case !bytes.Equal(b, tt.data):
			t.Errorf("%d: Mismatched data, actual then expected", i)
			t.Logf("% x", b)
			t.Logf("% x", tt.data)
		}
	}
}

func TestReadFragment(t *testing.T) {
	// func (fs *FileSystem) readFragment(index, offset uint32, fragmentSize int64) ([]byte, error) {
	fragments := []*fragmentEntry{
		{start: 0, size: 20, compressed: false},
		{start: 20, size: 10, compressed: false},
		{start: 30, size: 10, compressed: true},
	}
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
		30, 31, 32, 33, 34, 35, 36, 37, 38, 39}
	testFile := &testhelper.FileImpl{
		Reader: func(b []byte, offset int64) (int, error) {
			for _, f := range fragments {
				if uint64(offset) == f.start {
					copy(b, data[f.start:f.start+uint64(f.size)])
					return int(f.size), nil
				}
			}
			return 0, fmt.Errorf("unknown location")
		},
	}
	tests := []struct {
		index      uint32
		offset     uint32
		size       int64
		compressor Compressor
		data       []byte
		err        error
	}{
		{0, 10, 5, nil, data[10:15], nil},
		{1, 2, 5, nil, data[22:27], nil},
		{2, 2, 5, nil, nil, fmt.Errorf("fragment compressed but do not have valid compressor")},
		{2, 2, 9, &testCompressorAddBytes{b: []byte{0x40}}, append(data[32:40], 0x40), nil},
		{2, 2, 5, &testCompressorAddBytes{err: fmt.Errorf("foo")}, nil, fmt.Errorf("decompress error: foo")},
		{3, 2, 5, nil, nil, fmt.Errorf("cannot find fragment block with index %d", 3)},
	}

	for i, tt := range tests {
		fs := &FileSystem{
			fragments:  fragments,
			backend:    file.New(testFile, true),
			compressor: tt.compressor,
		}
		b, err := fs.readFragment(tt.index, tt.offset, tt.size)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case !bytes.Equal(b, tt.data):
			t.Errorf("%d: Mismatched data, actual then expected", i)
			t.Logf("% x", b)
			t.Logf("% x", tt.data)
		}
	}
}

func TestReadUidsGids(t *testing.T) {
	// func readUidsGids(s *superblock, file backend.File, c compressor) ([]uint32, error) {
	expected := []uint32{
		0, 10, 100, 1000,
	}
	ids := []byte{
		0x10, 0x80, // 16 bytes of uncompressed data
		0, 0, 0, 0,
		10, 0, 0, 0,
		100, 0, 0, 0,
		0xe8, 0x03, 0, 0,
	}
	idStart := uint64(1000)
	indexStart := idStart + uint64(len(ids))
	index := make([]byte, 8)
	binary.LittleEndian.PutUint64(index, idStart)
	s := &superblock{
		idTableStart: indexStart,
		idCount:      uint16(len(ids)-2) / 4,
	}
	testFile := &testhelper.FileImpl{
		Reader: func(b []byte, offset int64) (int, error) {
			switch uint64(offset) {
			case idStart:
				copy(b, ids)
				return len(ids), nil
			case idStart + 2:
				copy(b, ids[2:])
				return len(ids) - 2, nil
			case indexStart:
				copy(b, index)
				return len(index), nil
			default:
				return 0, fmt.Errorf("No data at position %d", offset)
			}
		},
	}
	uidsgids, err := readUidsGids(s, testFile, nil)
	switch {
	case err != nil:
		t.Errorf("unexpected error: %v", err)
	case !testEqualUint32Slice(uidsgids, expected):
		t.Errorf("Mismatched results, actual then expected")
		t.Logf("%#v", uidsgids)
		t.Logf("%#v", expected)
	}
}
