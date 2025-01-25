package squashfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

// inode implementation for testing
//
//nolint:structcheck // ignore unused entries, which we keep for reference
type inodeTestImpl struct {
	iIndex   uint32
	fileSize int64
	iType    inodeType
	entries  []*directoryEntry
	err      error
	body     inodeBody
}

func (i *inodeTestImpl) toBytes() []byte {
	return nil
}

func (i *inodeTestImpl) equal(_ inode) bool {
	return false
}
func (i *inodeTestImpl) size() int64 {
	return i.fileSize
}
func (i *inodeTestImpl) inodeType() inodeType {
	return i.iType
}
func (i *inodeTestImpl) index() uint32 {
	return i.iIndex
}
func (i *inodeTestImpl) getHeader() *inodeHeader {
	return nil
}
func (i *inodeTestImpl) getBody() inodeBody {
	return i.body
}

func TestInodeType(t *testing.T) {
	iType := inodeType(102)
	in := &inodeImpl{
		header: &inodeHeader{
			inodeType: iType,
		},
	}
	out := in.inodeType()
	if out != iType {
		t.Errorf("Mismatched type, actual %d, expected %d", out, iType)
	}
}
func TestInodeSize(t *testing.T) {
	size := uint64(107)
	body := &extendedFile{
		fileSize: size,
	}
	in := &inodeImpl{
		body: body,
	}
	out := in.size()
	if uint64(out) != size {
		t.Errorf("Mismatched size, actual %d, expected %d", out, size)
	}
}

func TestInodeHeader(t *testing.T) {
	b, err := testGetInodeMetabytes()
	if err != nil {
		t.Fatal(err)
	}
	goodHeader := b[:inodeHeaderSize]
	tests := []struct {
		b      []byte
		header *inodeHeader
		err    error
	}{
		{goodHeader, testGetFirstInodeHeader(), nil},
		{goodHeader[:10], nil, fmt.Errorf("received only %d bytes instead of minimum %d", 10, inodeHeaderSize)},
	}
	t.Run("parse", func(t *testing.T) {
		for i, tt := range tests {
			header, err := parseInodeHeader(tt.b)
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("%d: mismatched error, actual then expected", i)
				t.Logf("%v", err)
				t.Logf("%v", tt.err)
			case header != nil && tt.header != nil && *header != *tt.header:
				t.Errorf("%d: mismatched results, actual then expected", i)
				t.Logf("%#v", header)
				t.Logf("%#v", tt.header)
			}
		}
	})
	t.Run("toBytes", func(t *testing.T) {
		for i, tt := range tests {
			if tt.header == nil {
				continue
			}
			b := tt.header.toBytes()
			if !bytes.Equal(b, tt.b) {
				t.Errorf("%d: mismatched results, actual then expected", i)
				t.Logf("% x", b)
				t.Logf("% x", tt.b)
			}
		}
	})
}

func TestBlockData(t *testing.T) {
	tests := []struct {
		b   *blockData
		num uint32
	}{
		{&blockData{size: 0x212056, compressed: true}, 0x212056},
		{&blockData{size: 0x212056, compressed: false}, 0x1212056},
	}
	t.Run("parse", func(t *testing.T) {
		for i, tt := range tests {
			b := parseBlockData(tt.num)
			if *b != *tt.b {
				t.Errorf("%d: mismatched output, actual then expected", i)
				t.Logf("%#v", b)
				t.Logf("%#v", tt.b)
			}
		}
	})
	t.Run("toUint32", func(t *testing.T) {
		for i, tt := range tests {
			num := tt.b.toUint32()
			if num != tt.num {
				t.Errorf("%d: mismatched output, actual %x expected %x", i, num, tt.num)
			}
		}
	})
}

func TestBasicDirectory(t *testing.T) {
	dir := testBasicDirectory
	b, err := testGetInodeMetabytes()
	if err != nil {
		t.Fatal(err)
	}
	inodeB := b[testBasicDirectoryStart:testBasicDirectoryEnd]
	tests := []struct {
		b   []byte
		dir *basicDirectory
		err error
	}{
		{inodeB, dir, nil},
		{inodeB[:10], nil, fmt.Errorf("received %d bytes, fewer than minimum %d", 10, 16)},
	}

	t.Run("toBytes", func(t *testing.T) {
		for i, tt := range tests {
			if tt.dir == nil {
				continue
			}
			b := tt.dir.toBytes()
			if !bytes.Equal(b, tt.b) {
				t.Errorf("%d: mismatched output, actual then expected", i)
				t.Logf("% x", b)
				t.Logf("% x", tt.b)
			}
		}
	})
	t.Run("Size", func(t *testing.T) {
		size := dir.size()
		if size != int64(dir.fileSize) {
			t.Errorf("mismatched sizes, actual %d expected %d", size, dir.fileSize)
		}
	})
	t.Run("parse", func(t *testing.T) {
		for i, tt := range tests {
			d, err := parseBasicDirectory(tt.b)
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("%d: mismatched error, actual then expected", i)
				t.Logf("%v", err)
				t.Logf("%v", tt.err)
			case d != nil && tt.dir != nil && *d != *tt.dir:
				t.Errorf("%d: mismatched results, actual then expected", i)
				t.Logf("%#v", *d)
				t.Logf("%#v", *tt.dir)
			}
		}
	})
}

//nolint:unused,revive // keep for future when we implement it and will need t
func TestExtendedDirectory(t *testing.T) {
	// do some day when we have good raw data

	// func (i extendedDirectory) toBytes() []byte {
	// func (i extendedDirectory) size() int64 {
	// func parseExtendedDirectory(b []byte) (*extendedDirectory, error) {
}

func TestBasicFile(t *testing.T) {
	f := testBasicFile
	b, err := testGetInodeMetabytes()
	if err != nil {
		t.Fatal(err)
	}

	inodeB := b[testBasicFileStart:testBasicFileEnd]
	tests := []struct {
		b    []byte
		file *basicFile
		err  error
	}{
		{inodeB, f, nil},
		{inodeB[:10], nil, fmt.Errorf("received %d bytes, fewer than minimum %d", 10, 16)},
	}

	t.Run("toBytes", func(t *testing.T) {
		for i, tt := range tests {
			if tt.file == nil {
				continue
			}
			b := tt.file.toBytes()
			if !bytes.Equal(b, tt.b) {
				t.Errorf("%d: mismatched output, actual then expected", i)
				t.Logf("% x", b)
				t.Logf("% x", tt.b)
			}
		}
	})
	t.Run("Size", func(t *testing.T) {
		size := f.size()
		if size != int64(f.fileSize) {
			t.Errorf("mismatched sizes, actual %d expected %d", size, f.fileSize)
		}
	})
	//nolint:dupl // these functions vary slightly from one another
	t.Run("parse", func(t *testing.T) {
		for i, tt := range tests {
			fl, _, err := parseBasicFile(tt.b, int(testValidBlocksize))
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("%d: mismatched error, actual then expected", i)
				t.Logf("%v", err)
				t.Logf("%v", tt.err)
			case (fl == nil && tt.file != nil) || (fl != nil && tt.file == nil) || (fl != nil && tt.file != nil && !fl.equal(*tt.file)):
				t.Errorf("%d: mismatched results, actual then expected", i)
				t.Logf("%#v", *fl)
				t.Logf("%#v", *tt.file)
			}
		}
	})
	t.Run("toExtended", func(t *testing.T) {
		// func (i basicFile) toExtended() extendedFile {
		ext := f.toExtended()
		if ext.size() != f.size() {
			t.Errorf("Mismatched sizes actual %d expected %d", ext.size(), f.size())
		}
		if ext.startBlock != uint64(f.startBlock) {
			t.Errorf("Mismatched startBlock actual %d expected %d", ext.startBlock, f.startBlock)
		}
		if ext.fragmentOffset != f.fragmentOffset {
			t.Errorf("Mismatched fragmentOffset actual %d expected %d", ext.fragmentOffset, f.fragmentOffset)
		}
		if ext.fragmentBlockIndex != f.fragmentBlockIndex {
			t.Errorf("Mismatched fragmentBlockIndex actual %d expected %d", ext.fragmentBlockIndex, f.fragmentBlockIndex)
		}
		if len(ext.blockSizes) != len(f.blockSizes) {
			t.Errorf("Mismatched blockSizes actual then expected")
			t.Logf("%#v", ext.blockSizes)
			t.Logf("%#v", f.blockSizes)
		}
	})
}

func TestExtendedFile(t *testing.T) {
	fd := testExtendedFile
	f := &fd
	b, err := testGetInodeMetabytes()
	if err != nil {
		t.Fatal(err)
	}
	inodeB := b[testExtendedFileStart:testExtendedFileEnd]
	tests := []struct {
		b    []byte
		file *extendedFile
		err  error
	}{
		{inodeB, f, nil},
		{inodeB[:10], nil, fmt.Errorf("received %d bytes instead of expected minimal %d", 10, 40)},
	}

	t.Run("toBytes", func(t *testing.T) {
		for i, tt := range tests {
			if tt.file == nil {
				continue
			}
			b := tt.file.toBytes()
			if !bytes.Equal(b, tt.b) {
				t.Errorf("%d: mismatched output, actual then expected", i)
				t.Logf("% x", b)
				t.Logf("% x", tt.b)
			}
		}
	})
	t.Run("Size", func(t *testing.T) {
		size := f.size()
		if size != int64(f.fileSize) {
			t.Errorf("mismatched sizes, actual %d expected %d", size, f.fileSize)
		}
	})
	//nolint:dupl // these functions vary slightly from one another
	t.Run("parse", func(t *testing.T) {
		for i, tt := range tests {
			fl, _, err := parseExtendedFile(tt.b, int(testValidBlocksize))
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("%d: mismatched error, actual then expected", i)
				t.Logf("%v", err)
				t.Logf("%v", tt.err)
			case (fl == nil && tt.file != nil) || (fl != nil && tt.file == nil) || (fl != nil && tt.file != nil && !fl.equal(*tt.file)):
				t.Errorf("%d: mismatched results, actual then expected", i)
				t.Logf("%#v", *fl)
				t.Logf("%#v", *tt.file)
			}
		}
	})
}

func TestBasicSymlink(t *testing.T) {
	s := testBasicSymlink
	b, err := testGetInodeMetabytes()
	if err != nil {
		t.Fatal(err)
	}

	inodeB := b[testBasicSymlinkStart:testBasicSymlinkEnd]
	tests := []struct {
		b   []byte
		sym *basicSymlink
		err error
	}{
		{inodeB, s, nil},
		{inodeB[:7], nil, fmt.Errorf("received %d bytes instead of expected minimal %d", 7, 8)},
	}

	t.Run("toBytes", func(t *testing.T) {
		for i, tt := range tests {
			if tt.sym == nil {
				continue
			}
			b := tt.sym.toBytes()
			if !bytes.Equal(b, tt.b) {
				t.Errorf("%d: mismatched output, actual then expected", i)
				t.Logf("% x", b)
				t.Logf("% x", tt.b)
			}
		}
	})
	t.Run("Size", func(t *testing.T) {
		size := s.size()
		if size != 0 {
			t.Errorf("mismatched sizes, actual %d expected %d", size, 0)
		}
	})
	t.Run("parse", func(t *testing.T) {
		for i, tt := range tests {
			sym, _, err := parseBasicSymlink(tt.b)
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("%d: mismatched error, actual then expected", i)
				t.Logf("%v", err)
				t.Logf("%v", tt.err)
			case (sym == nil && tt.sym != nil) || (sym != nil && tt.sym == nil) || (sym != nil && tt.sym != nil && *sym != *tt.sym):
				t.Errorf("%d: mismatched results, actual then expected", i)
				t.Logf("%#v", *sym)
				t.Logf("%#v", *tt.sym)
			}
		}
	})
}

func TestExtendedSymlink(t *testing.T) {
	s := &extendedSymlink{
		links:      1,
		target:     "/a/b/c/d/ef/g/h",
		xAttrIndex: 46,
	}
	b, err := testGetInodeMetabytes()
	if err != nil {
		t.Fatal(err)
	}

	inodeB := binary.LittleEndian.AppendUint32(b[testBasicSymlinkStart:testBasicSymlinkEnd], s.xAttrIndex)

	t.Run("toBytes", func(t *testing.T) {
		b := s.toBytes()
		if !bytes.Equal(b, inodeB) {
			t.Errorf("mismatched output, actual then expected")
			t.Logf("% x", b)
			t.Logf("% x", inodeB)
		}
	})
	t.Run("Size", func(t *testing.T) {
		size := s.size()
		if size != 0 {
			t.Errorf("mismatched sizes, actual %d expected %d", size, 0)
		}
	})

	tests := []struct {
		b   []byte
		sym *extendedSymlink
		ext int
		err error
	}{
		{inodeB, s, 0, nil},
		{inodeB[:7], nil, 0, fmt.Errorf("received %d bytes instead of expected minimal %d", 7, 8)},
		{inodeB[:20], &extendedSymlink{links: 1}, 19, nil},
	}

	t.Run("parse", func(t *testing.T) {
		for i, tt := range tests {
			sym, ext, err := parseExtendedSymlink(tt.b)
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("%d: mismatched error, actual then expected", i)
				t.Logf("%v", err)
				t.Logf("%v", tt.err)
			case tt.ext != ext:
				t.Errorf("%d: mismatched extra, actual then expected", i)
				t.Logf("%v", ext)
				t.Logf("%v", tt.ext)
			case (sym == nil && tt.sym != nil) || (sym != nil && tt.sym == nil) || (sym != nil && tt.sym != nil && *sym != *tt.sym):
				t.Errorf("%d: mismatched results, actual then expected", i)
				t.Logf("%#v", *sym)
				t.Logf("%#v", *tt.sym)
			}
		}
	})
}

//nolint:unused,revive // keep for future when we implement it and will need t
func TestBasicDevice(t *testing.T) {
	// when we have more data with which to work

	// func (i basicDevice) toBytes() []byte {
	// func (i basicDevice) size() int64 {
	// func parseBasicDevice(b []byte) (*basicDevice, error) {
}

//nolint:unused,revive // keep for future when we implement it and will need t
func TestExtendedDevice(t *testing.T) {
	// when we have more data with which to work

	// func (i extendedDevice) toBytes() []byte {
	// func (i extendedDevice) size() int64 {
	// func parseExtendedDevice(b []byte) (*extendedDevice, error) {

}

//nolint:unused,revive // keep for future when we implement it and will need t
func TestBasicIPC(t *testing.T) {
	// when we have more data with which to work

	// func (i basicIPC) toBytes() []byte {
	// func (i basicIPC) size() int64 {
	// func parseBasicIPC(b []byte) (*basicIPC, error) {
}

//nolint:unused,revive // keep for future when we implement it and will need t
func TestExtendedIPC(t *testing.T) {
	// when we have more data with which to work

	// func (i extendedIPC) toBytes() []byte {
	// func (i extendedIPC) size() int64 {
	// func parseExtendedIPC(b []byte) (*extendedIPC, error) {
}

func TestInode(t *testing.T) {
	b, err := testGetInodeMetabytes()
	if err != nil {
		t.Fatal(err)
	}
	inodeB := b[testFirstInodeStart:testFirstInodeEnd]
	in := &inodeImpl{
		header: testGetFirstInodeHeader(),
		body:   testGetFirstInodeBody(),
	}
	tests := []struct {
		b   []byte
		i   *inodeImpl
		err error
	}{
		{inodeB, in, nil},
		{inodeB[:10], nil, fmt.Errorf("received %d bytes, insufficient for minimum %d for header and inode", 10, 17)},
	}

	t.Run("toBytes", func(t *testing.T) {
		for i, tt := range tests {
			if tt.i == nil {
				continue
			}
			b := tt.i.toBytes()
			if !bytes.Equal(b, tt.b) {
				t.Errorf("%d: mismatched output, actual then expected", i)
				t.Logf("% x", b)
				t.Logf("% x", tt.b)
			}
		}
	})
}
