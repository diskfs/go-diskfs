package squashfs

// This file contains all of the constants for running tests. These are generated from
// the following:
// 1. Run the testdata/buildsqs.sh script, which generates file.sqs and file_uncompressed.sqs
// 2. Run github.com/diskfs/squashfs-utils on the file
// 3. Take the relevant sizes, locations and inodes and use them here.

import (
	"io/fs"
	"os"
	"time"

	"github.com/diskfs/go-diskfs/backend/file"
)

const (
	Squashfsfile             = "./testdata/file.sqs"
	SquashfsUncompressedfile = "./testdata/file_uncompressed.sqs"
	SquashfsfileListing      = "./testdata/list.txt"
	SquashfsReadTestFile     = "./testdata/read_test.sqs"
	SquashfsReatTestMd5sums  = "./testdata/read_test.md5sums"
	SquashfsReadDirTestFile  = "./testdata/dir_read.sqs"
)

// first header
func testGetFirstInodeHeader() *inodeHeader {
	return &inodeHeader{
		inodeType: inodeType(9),
		mode:      0x01a4,
		uidIdx:    0,
		gidIdx:    1,
		modTime:   time.Unix(0x5fe4d90b, 0),
		index:     1,
	}
}
func testGetFirstInodeBody() inodeBody {
	return extendedFile{
		startBlock:         0,
		fragmentBlockIndex: 0,
		fileSize:           7,
		fragmentOffset:     0,
		sparse:             0,
		links:              2,
		xAttrIndex:         0xffffffff,
		blockSizes:         nil,
	}
}

/*
	basic file: 0x61-0x80
	extended file: 0x02-0x39
	basic directory: 0x826a-0x8289
	basic symlink: 0x3a-61
*/

var (
	testValidModTime                       = time.Unix(0x5c20d8d7, 0)
	testValidBlocksize              uint32 = 0x20000
	testFragmentStart               uint64 = 0x5082ac // this is not the start of the fragment ID table, but of the fragment table
	testValidSuperblockUncompressed        = &superblock{
		inodes:        0x203,
		modTime:       testValidModTime,
		blocksize:     testValidBlocksize,
		fragmentCount: 1,
		compression:   compressionGzip,
		idCount:       3,
		versionMajor:  4,
		versionMinor:  0,
		rootInode: &inodeRef{
			block:  0x4004,
			offset: 0x01d8,
		},
		size:                0x509413,
		idTableStart:        0x5093be,
		xattrTableStart:     0x5093fb,
		inodeTableStart:     0x50196f,
		directoryTableStart: 0x505b6d,
		fragmentTableStart:  0x508386,
		exportTableStart:    0x5093a8,
		superblockFlags: superblockFlags{
			dedup:                 true,
			exportable:            true,
			uncompressedData:      true,
			uncompressedInodes:    true,
			uncompressedFragments: true,
		},
	}
	testMetaOffset      = testValidSuperblockUncompressed.inodeTableStart
	testDirectoryOffset = testValidSuperblockUncompressed.directoryTableStart - testMetaOffset
	testFragmentOffset  = testFragmentStart - testMetaOffset
	testFragEntries     = []*fragmentEntry{
		{size: 6415, compressed: false, start: 5242976},
	}

	// this is for /foo/filename_0
	testBasicFile = &basicFile{
		startBlock:         0,
		fragmentBlockIndex: 0,
		fileSize:           0xb,
		fragmentOffset:     0xc,
		blockSizes:         []*blockData{},
	}
	testBasicFileStart = 0x117 + 0x10
	testBasicFileEnd   = testBasicFileStart + 0x10

	// this is for /a
	testBasicDirectory = &basicDirectory{
		startBlock:       0x0,
		links:            0x3,
		fileSize:         24,
		offset:           0x2a,
		parentInodeIndex: 0x203,
	}
	testBasicDirectoryStart = 0x98 + 0x10 // add for the header
	testBasicDirectoryEnd   = testBasicDirectoryStart + 0x10

	// this is for /README.md
	testExtendedFile, _   = testGetFirstInodeBody().(extendedFile)
	testExtendedFileStart = 0x0 + 0x10
	testExtendedFileEnd   = testExtendedFileStart + 0x28

	// this is for /emptylink
	testBasicSymlink = &basicSymlink{ // 	basic symlink: 0x4a-61
		links:  1,
		target: "/a/b/c/d/ef/g/h",
	}
	testBasicSymlinkStart = 0xf0 + 0x10
	testBasicSymlinkEnd   = testBasicSymlinkStart + 0x17
	testFirstInodeStart   = 0x0
	testFirstInodeEnd     = testFirstInodeStart + 0x38
)

//nolint:deadcode,varcheck,unused // we need these references in the future
var (
	testIDTableStart       = 0x5092c8 + 2 - testMetaOffset
	testIDTableEnd         = 0x5092d6 - testMetaOffset
	testXattrIDStart       = 0x509301 + 2 - testMetaOffset
	testXattrIDEnd         = testValidSuperblockUncompressed.xattrTableStart - testMetaOffset
	testXattrMetaStart     = 0x5092de + 2 - testMetaOffset
	testXattrMetaEnd       = testXattrIDStart
	testInodeDataLength    = 16766
	testDirectoryDataSize  = testFragmentOffset - testDirectoryOffset
	testLargeDirEntryCount = 252
)

func testGetFilesystem(f fs.File) (*FileSystem, []byte, error) {
	var (
		err error
		b   []byte
	)
	if f == nil {
		f, err = os.Open(SquashfsUncompressedfile)
		if err != nil {
			return nil, nil, err
		}
		b, err = os.ReadFile(SquashfsUncompressedfile)
		if err != nil {
			return nil, nil, err
		}
	}
	blocksize := int64(testValidBlocksize)
	sb := testValidSuperblockUncompressed
	testFs := &FileSystem{
		/*
			TODO: Still need to add these in
			uidsGids       []byte
		*/
		fragments: []*fragmentEntry{
			{start: 200000, size: 12},
		},
		workspace:  "",
		compressor: &CompressorGzip{},
		size:       5251072,
		start:      0,
		backend:    file.New(f, true),
		blocksize:  blocksize,
		xattrs:     nil,
		rootDir: &inodeImpl{
			header: &inodeHeader{
				inodeType: inodeBasicDirectory,
				uidIdx:    0,
				gidIdx:    1,
				modTime:   sb.modTime,
				index:     0x0203,
				mode:      0o755,
			},
			body: &basicDirectory{
				startBlock:       0x2002,
				links:            6,
				fileSize:         0xb3,
				offset:           0x0753,
				parentInodeIndex: 0x0204,
			},
		},
		uidsGids:   []uint32{5, 0, 1},
		superblock: sb,
	}
	return testFs, b, nil
}

func testGetInodeMetabytes() (inodeBytes []byte, err error) {
	testFs, b, err := testGetFilesystem(nil)
	if err != nil {
		return nil, err
	}
	return b[testFs.superblock.inodeTableStart+2:], nil
}

//nolint:deadcode // we need these references in the future
func testGetFilesystemRoot() []*directoryEntry {
	/*
		isSubdirectory bool
		name           string
		size           int64
		modTime        time.Time
		mode           os.FileMode
		inode          *inode
	*/

	// data taken from reading the bytes of the file SquashfsUncompressedfile
	modTime := time.Unix(0x5c20d8d7, 0)
	return []*directoryEntry{
		{isSubdirectory: true, name: "foo", size: 9949, modTime: modTime, mode: 0o755},
		{isSubdirectory: true, name: "zero", size: 32, modTime: modTime, mode: 0o755},
		{isSubdirectory: true, name: "random", size: 32, modTime: modTime, mode: 0o755},
		{isSubdirectory: false, name: "emptylink", size: 0, modTime: modTime, mode: 0o777},
		{isSubdirectory: false, name: "goodlink", size: 0, modTime: modTime, mode: 0o777},
		{isSubdirectory: false, name: "hardlink", size: 7, modTime: modTime, mode: 0o644, uid: 1, gid: 2},
		{isSubdirectory: false, name: "README.md", size: 7, modTime: modTime, mode: 0o644, uid: 1, gid: 2},
		{isSubdirectory: false, name: "attrfile", size: 5, modTime: modTime, mode: 0o644, xattrs: map[string]string{"abc": "def", "myattr": "hello"}},
	}
}

// GetTestFileSmall get a *squashfs.File to a usable and known test file
func GetTestFileSmall(f fs.File, c Compressor) (*File, error) {
	testFs, _, err := testGetFilesystem(f)
	if err != nil {
		return nil, err
	}
	testFs.compressor = c
	ef := &extendedFile{
		startBlock:         superblockSize,
		fileSize:           7,
		sparse:             0,
		links:              0,
		fragmentBlockIndex: 0,
		fragmentOffset:     0,
		xAttrIndex:         0,
		blockSizes:         []*blockData{},
	}
	// inode 0, offset 0, name "README.md", type basic file
	return &File{
		extendedFile: ef,
		isReadWrite:  false,
		isAppend:     false,
		offset:       0,
		filesystem:   testFs,
	}, nil
}

// GetTestFileBig get a *squashfs.File to a usable and known test file
func GetTestFileBig(f fs.File, c Compressor) (*File, error) {
	testFs, _, err := testGetFilesystem(f)
	if err != nil {
		return nil, err
	}
	testFs.compressor = c
	fragSize := uint64(5)
	size := uint64(testFs.blocksize) + fragSize
	ef := &extendedFile{
		startBlock:         superblockSize,
		fileSize:           size,
		sparse:             0,
		links:              0,
		fragmentBlockIndex: 0,
		fragmentOffset:     7,
		xAttrIndex:         0,
		blockSizes: []*blockData{
			{size: uint32(testFs.blocksize), compressed: false},
		},
	}
	// inode 0, offset 0, name "README.md", type basic file
	return &File{
		extendedFile: ef,
		isReadWrite:  false,
		isAppend:     false,
		offset:       0,
		filesystem:   testFs,
	}, nil
}
