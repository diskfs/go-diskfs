package fat32

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/testhelper"
	"github.com/google/go-cmp/cmp"
)

/*
 TODO:
  test when fat32 is inside a partition
	in that case, the dataStart is relative to partition, not to disk, so need to read the offset correctly
*/

func clustersFromMap(m map[uint32]uint32, maxCluster uint32) []uint32 {
	clusters := make([]uint32, maxCluster+1)
	for k, v := range m {
		clusters[k] = v
	}

	return clusters
}

func getValidFat32FSFull() *FileSystem {
	fs := getValidFat32FSSmall()
	fs.table = *getValidFat32Table()
	return fs
}

func getValidFat32FSSmall() *FileSystem {
	eoc := uint32(0xffffffff)
	maxCluster := uint32(128)
	fs := &FileSystem{
		table: table{
			rootDirCluster: 2,
			size:           512,
			maxCluster:     maxCluster,
			eocMarker:      eoc,
			/*
				 map:
					 2
					 3-4-5-6
					 7-10
					 8-9-11
					 11
					 15
					 16-broken
					 17-broken
			*/
			clusters: clustersFromMap(map[uint32]uint32{
				2:  eoc,
				3:  4,
				4:  5,
				5:  6,
				6:  eoc,
				7:  10,
				10: eoc,
				8:  9,
				9:  11,
				11: eoc,
				15: eoc,
				16: 1,
				17: 999,
			}, maxCluster),
		},
		bytesPerCluster: 512,
		dataStart:       178176,
		file: &testhelper.FileImpl{
			//nolint:revive // unused parameter, keeping name makes it easier to use in the future
			Writer: func(b []byte, offset int64) (int, error) {
				return len(b), nil
			},
		},
		fsis: FSInformationSector{},
		bootSector: msDosBootSector{
			biosParameterBlock: &dos71EBPB{
				fsInformationSector: 2,
				backupBootSector:    6,
				dos331BPB: &dos331BPB{
					dos20BPB: &dos20BPB{
						reservedSectors:   32,
						sectorsPerCluster: 1,
					},
				},
			},
		},
	}
	return fs
}

func TestFat32GetClusterList(t *testing.T) {
	fs := getValidFat32FSSmall()

	tests := []struct {
		firstCluster uint32
		clusters     []uint32
		err          error
	}{
		{2, []uint32{2}, nil},
		{3, []uint32{3, 4, 5, 6}, nil},
		{7, []uint32{7, 10}, nil},
		{8, []uint32{8, 9, 11}, nil},
		{15, []uint32{15}, nil},
		// test non-existent ones, just to see that they come back empty
		{14, nil, fmt.Errorf("invalid start cluster")},
		{100, nil, fmt.Errorf("invalid start cluster")},
		{16, nil, fmt.Errorf("invalid cluster chain")},
	}

	for i, tt := range tests {
		output, err := fs.getClusterList(tt.firstCluster)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched errors, actual %v expected %v", i, err, tt.err)
		case !reflect.DeepEqual(output, tt.clusters):
			t.Errorf("%d: mismatched cluster list, actual then expected", i)
			t.Logf("%v", output)
			t.Logf("%v", tt.clusters)
		}
	}
}

func TestFat32ReadDirectory(t *testing.T) {
	// will use the fat32.img fixture to test an actual directory
	// \ (root directory) should be in one cluster
	// \foo should be in two clusters
	file, err := os.Open(Fat32File)
	if err != nil {
		t.Fatalf("could not open file %s to read: %v", Fat32File, err)
	}
	defer file.Close()
	fs := &FileSystem{
		table:           *getValidFat32Table(),
		file:            file,
		bytesPerCluster: int(fsInfo.bytesPerCluster),
		dataStart:       fsInfo.dataStartBytes,
	}
	validDe, _, err := GetValidDirectoryEntries()
	if err != nil {
		t.Fatalf("unable to read valid directory entries: %v", err)
	}
	validDeExtended, _, err := GetValidDirectoryEntriesExtended("/foo")
	if err != nil {
		t.Fatalf("unable to read valid directory entries extended: %v", err)
	}
	tests := []struct {
		path    string
		cluster uint32
		entries []*directoryEntry
	}{
		{"\\", 2, validDe},
		{"/", 2, validDe},
		{"\\foo", 3, validDeExtended},
		{"/foo", 3, validDeExtended},
	}
	for _, tt := range tests {
		dir := &Directory{
			directoryEntry: directoryEntry{
				clusterLocation: tt.cluster,
			},
		}
		entries, err := fs.readDirectory(dir)
		switch {
		case err != nil:
			t.Errorf("fs.readDirectory(%s): unexpected nil error: %v", tt.path, err)
		case len(entries) != len(tt.entries):
			t.Errorf("fs.readDirectory(%s): number of entries do not match, actual %d expected %d", tt.path, len(entries), len(tt.entries))
		default:
			for i, entry := range entries {
				if !compareDirectoryEntriesIgnoreDates(entry, tt.entries[i]) {
					t.Errorf("fs.readDirectory(%s) %d: entries do not match, actual then expected", tt.path, i)
					t.Log(cmp.Diff(*entry, *tt.entries[i], cmp.AllowUnexported(directoryEntry{})))
				}
			}
		}
	}
}

func TestFat32AllocateSpace(t *testing.T) {
	/*
			 map:
				 2
				 3-4-5-6
				 7-10
				 8-9-11
				 11
				 15
				 16-broken
				 17-broken
		// recall that 512 bytes per cluster here
	*/
	tests := []struct {
		size     uint64
		previous uint32
		clusters []uint32
		err      error
	}{
		{500, 2, []uint32{2}, nil},
		{600, 2, []uint32{2, 12}, nil},
		{2000, 2, []uint32{2, 12, 13, 14}, nil},
		{2000, 0, []uint32{12, 13, 14, 18}, nil},
		{200000000000, 0, nil, fmt.Errorf("no space left on device")},
		{200000000000, 2, nil, fmt.Errorf("no space left on device")},
		{2000, 17, nil, fmt.Errorf("unable to get cluster list: invalid cluster chain at 999")},
		{2000, 999, nil, fmt.Errorf("invalid cluster chain at 999")},
	}
	for _, tt := range tests {
		// reset for each test
		fs := getValidFat32FSSmall()
		output, err := fs.allocateSpace(tt.size, tt.previous)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("fs.allocateSpace(%d, %d): mismatched errors, actual %v expected %v", tt.size, tt.previous, err, tt.err)
		case len(output) != len(tt.clusters):
			t.Errorf("fs.allocateSpace(%d, %d): mismatched output lengths, actual %d expected %d", tt.size, tt.previous, len(output), len(tt.clusters))
		case !reflect.DeepEqual(output, tt.clusters):
			t.Errorf("fs.allocateSpace(%d, %d): mismatched outputs, actual then expected", tt.size, tt.previous)
			t.Logf("%v", output)
			t.Logf("%v", tt.clusters)
		}
	}
}

func TestFat32MkSubdir(t *testing.T) {
	fs := getValidFat32FSSmall()
	d := &Directory{
		entries: []*directoryEntry{},
	}
	expected := &directoryEntry{
		filenameShort:   "SUB",
		fileExtension:   "",
		filenameLong:    "sub",
		isSubdirectory:  true,
		clusterLocation: 12,
	}
	de, err := fs.mkSubdir(d, "sub")
	switch {
	case err != nil:
		t.Errorf("unexpected non-nil error: %v", err)
	case de.filenameLong != expected.filenameLong ||
		de.filenameShort != expected.filenameShort ||
		de.fileExtension != expected.fileExtension ||
		de.isSubdirectory != expected.isSubdirectory ||
		de.clusterLocation != expected.clusterLocation:
		t.Errorf("mismatched created DirectoryEntry, actual then expected")
		t.Logf("%v", *de)
		t.Logf("%v", *expected)
	}
}

func TestFat32MkFile(t *testing.T) {
	fs := getValidFat32FSSmall()
	d := &Directory{
		entries: []*directoryEntry{},
	}
	expected := &directoryEntry{
		filenameShort:     "FILE",
		fileExtension:     "",
		filenameLong:      "file",
		isSubdirectory:    false,
		clusterLocation:   12,
		longFilenameSlots: 1,
		isNew:             true,
	}
	de, err := fs.mkFile(d, "file")
	switch {
	case err != nil:
		t.Errorf("unexpected non-nil error: %v", err)
	case !compareDirectoryEntriesIgnoreDates(de, expected):
		/*
				de.FilenameLong != expected.FilenameLong ||
			de.FilenameShort != expected.FilenameShort ||
			de.FileExtension != expected.FileExtension ||
			de.IsSubdirectory != expected.IsSubdirectory ||
			de.clusterLocation != expected.clusterLocation:
		*/
		t.Errorf("mismatched created DirectoryEntry, actual then expected")
		t.Logf("%v", *de)
		t.Logf("%v", *expected)
	}
}

func TestFat32ReadDirWithMkdir(t *testing.T) {
	fs := getValidFat32FSFull()
	datab, err := os.ReadFile(Fat32File)
	if err != nil {
		t.Fatalf("unable to read data from file %s: %v", Fat32File, err)
	}
	validDe, _, err := GetValidDirectoryEntries()
	if err != nil {
		t.Fatalf("unable to read valid directory entries: %v", err)
	}
	validDeLong, _, err := GetValidDirectoryEntriesExtended("/foo")
	if err != nil {
		t.Fatalf("unable to read valid directory entries extended: %v", err)
	}
	tests := []struct {
		path    string
		doMake  bool
		dir     *Directory
		entries []*directoryEntry
		err     error
	}{
		{"/", false, &Directory{
			directoryEntry: directoryEntry{
				filenameShort:   "",
				fileExtension:   "",
				filenameLong:    "",
				isSubdirectory:  true,
				clusterLocation: 2,
			},
		}, validDe, nil},
		{"/FOO", false, &Directory{
			directoryEntry: directoryEntry{
				filenameShort:   "FOO",
				fileExtension:   "",
				filenameLong:    "foo",
				isSubdirectory:  true,
				clusterLocation: 3,
			},
		}, validDeLong, nil},
		{"/FOO2", false, nil, nil, fmt.Errorf("path /FOO2 not found")},
		{"/FOO2", true, &Directory{
			directoryEntry: directoryEntry{
				filenameShort:   "FOO2",
				fileExtension:   "",
				filenameLong:    "",
				isSubdirectory:  true,
				clusterLocation: 127,
			},
		}, nil, nil},
	}

	for _, tt := range tests {
		fs.file = &testhelper.FileImpl{
			//nolint:revive // unused parameter, keeping name makes it easier to use in the future
			Writer: func(b []byte, offset int64) (int, error) {
				return len(b), nil
			},
			Reader: func(b []byte, offset int64) (int, error) {
				copy(b, datab[offset:])
				return len(b), nil
			},
		}
		dir, entries, err := fs.readDirWithMkdir(tt.path, tt.doMake)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("fs.readDirWithMkdir(%s, %t): mismatched errors, actual %v expected %v", tt.path, tt.doMake, err, tt.err)
		case dir != nil && tt.dir == nil || dir == nil && tt.dir != nil:
			t.Errorf("fs.readDirWithMkdir(%s, %t): mismatched directory unexpected nil, actual then expected", tt.path, tt.doMake)
			t.Logf("%v", dir)
			t.Logf("%v", tt.dir)
		case dir != nil && tt.dir != nil && dir.filenameShort != tt.dir.filenameShort:
			t.Errorf("fs.readDirWithMkdir(%s, %t): mismatched directory, actual then expected", tt.path, tt.doMake)
			t.Logf("%v", dir)
			t.Logf("%v", tt.dir)
		case len(entries) != len(tt.entries):
			t.Errorf("fs.readDirWithMkdir(%s, %t): mismatched entries, actual then expected", tt.path, tt.doMake)
			t.Logf("%v", entries)
			t.Logf("%v", tt.entries)
		}
	}
}
