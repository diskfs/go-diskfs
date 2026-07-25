package sync

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/partition/gpt"
)

// dotMarkerFile is the marker Alpine Linux puts on the boot media, created with
// nothing but `touch` by scripts/mkimg.base.sh, so it is both zero length and
// dot-prefixed.
const dotMarkerFile = ".boot_repository"

const (
	fat32SectorSize  = 512
	fat32PartSectors = 65536 // 32 MiB per partition
	fat32Part1Start  = 2048
	fat32Part2Start  = fat32Part1Start + fat32PartSectors
	fat32PartBytes   = int64(fat32PartSectors) * fat32SectorSize
	fat32DiskSize    = (int64(fat32Part2Start) + fat32PartSectors + fat32Part1Start) * fat32SectorSize
)

// fat32Entry describes one entry to create on the source partition. A file with
// nil data is zero length.
type fat32Entry struct {
	name string
	data []byte
	dir  bool
}

// fat32Tree is the source tree for the copy tests: zero-length files with a
// plain 8.3 name, a lower-case name and a dot-prefixed name, plus a non-empty
// file and a subdirectory for company.
var fat32Tree = []fat32Entry{
	{name: "TEST.TXT"},
	{name: "empty"},
	{name: dotMarkerFile},
	{name: "kernel", data: []byte("not empty")},
	{name: "EFI", dir: true},
}

// TestCopyFat32ZeroLengthFileFileSystem copies a FAT32 partition holding
// zero-length files into a freshly formatted partition file by file — the path
// taken when the target partition's geometry differs from the source.
func TestCopyFat32ZeroLengthFileFileSystem(t *testing.T) {
	imgPath := newFat32Disk(t, fat32Tree)
	copyFat32FileSystem(t, imgPath)
	checkFat32Copy(t, imgPath, fat32Tree)
}

// TestCopyFat32ZeroLengthFileRaw copies the same partition byte for byte with
// CopyPartitionRaw — the path taken when source and target geometry match.
func TestCopyFat32ZeroLengthFileRaw(t *testing.T) {
	imgPath := newFat32Disk(t, fat32Tree)

	d := openFat32Disk(t, imgPath)
	if err := CopyPartitionRaw(d, 1, 2); err != nil {
		t.Fatalf("CopyPartitionRaw: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close disk: %v", err)
	}

	checkFat32Copy(t, imgPath, fat32Tree)
}

// TestCopyFat32UnallocatedEmptyFile copies a partition whose zero-length file
// has no cluster allocated to it. That is how mkfs.vfat, mtools and the Linux
// kernel all record an empty file, so a FAT32 filesystem written by anything
// other than go-diskfs arrives in this shape.
func TestCopyFat32UnallocatedEmptyFile(t *testing.T) {
	tree := []fat32Entry{
		{name: "TEST.TXT"},
		{name: "kernel", data: []byte("not empty")},
	}
	imgPath := newFat32Disk(t, tree)
	clearStartCluster(t, imgPath, "TEST    TXT")

	copyFat32FileSystem(t, imgPath)
	checkFat32Copy(t, imgPath, tree)
}

// copyFat32FileSystem formats partition 2 as FAT32 and copies partition 1 into
// it file by file.
func copyFat32FileSystem(t *testing.T, imgPath string) {
	t.Helper()

	d := openFat32Disk(t, imgPath)
	srcFS, err := d.GetFilesystem(1)
	if err != nil {
		t.Fatalf("get source filesystem: %v", err)
	}
	dstFS, err := d.CreateFilesystem(disk.FilesystemSpec{
		Partition:   2,
		FSType:      filesystem.TypeFat32,
		VolumeLabel: "TARGET",
	})
	if err != nil {
		t.Fatalf("create target filesystem: %v", err)
	}
	if err := CopyFileSystem(srcFS, dstFS); err != nil {
		t.Fatalf("CopyFileSystem: %v", err)
	}
	if err := dstFS.Close(); err != nil {
		t.Fatalf("close target filesystem: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close disk: %v", err)
	}
}

// newFat32Disk creates a GPT image with two identically sized ESP partitions,
// formats the first as FAT32 and populates it with tree. It returns the image
// path, leaving the image closed.
func newFat32Disk(t *testing.T, tree []fat32Entry) string {
	t.Helper()

	imgPath := filepath.Join(t.TempDir(), "disk.img")
	d, err := diskfs.Create(imgPath, fat32DiskSize, fat32SectorSize)
	if err != nil {
		t.Fatalf("create disk: %v", err)
	}
	table := &gpt.Table{
		LogicalSectorSize:  fat32SectorSize,
		PhysicalSectorSize: fat32SectorSize,
		ProtectiveMBR:      true,
		Partitions: []*gpt.Partition{
			{Index: 1, Start: fat32Part1Start, Size: uint64(fat32PartBytes), Type: gpt.EFISystemPartition, Name: "source"},
			{Index: 2, Start: fat32Part2Start, Size: uint64(fat32PartBytes), Type: gpt.EFISystemPartition, Name: "target"},
		},
	}
	if err := d.Partition(table); err != nil {
		t.Fatalf("write partition table: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close disk after partitioning: %v", err)
	}

	// Reopen before addressing partitions: a Partition built in an in-memory
	// table does not carry the disk's sector size.
	d = openFat32Disk(t, imgPath)
	defer func() { _ = d.Close() }()

	fsys, err := d.CreateFilesystem(disk.FilesystemSpec{
		Partition:   1,
		FSType:      filesystem.TypeFat32,
		VolumeLabel: "SOURCE",
	})
	if err != nil {
		t.Fatalf("create source filesystem: %v", err)
	}
	for _, e := range tree {
		if e.dir {
			if err := fsys.Mkdir("/" + e.name); err != nil {
				t.Fatalf("mkdir %s: %v", e.name, err)
			}
			continue
		}
		writeFat32(t, fsys, "/"+e.name, e.data)
	}
	// The source itself must list everything that was just created; otherwise a
	// copy failure below cannot be told apart from a creation failure.
	checkListing(t, fsys, ".", treeNames(tree))
	if err := fsys.Close(); err != nil {
		t.Fatalf("close source filesystem: %v", err)
	}
	return imgPath
}

func openFat32Disk(t *testing.T, imgPath string) *disk.Disk {
	t.Helper()
	d, err := diskfs.Open(imgPath, diskfs.WithSectorSize(fat32SectorSize))
	if err != nil {
		t.Fatalf("open disk: %v", err)
	}
	if _, err := d.GetPartitionTable(); err != nil {
		t.Fatalf("read partition table: %v", err)
	}
	return d
}

// writeFat32 creates a file with the given content; nil content leaves it at
// zero length, the equivalent of touch(1) on a name that does not yet exist.
func writeFat32(t *testing.T, fsys filesystem.FileSystem, name string, content []byte) {
	t.Helper()
	f, err := fsys.OpenFile(name, os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	if len(content) > 0 {
		if _, err := f.Write(content); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
}

// clearStartCluster zeroes the first-cluster field of the 8.3 directory entry
// whose 11-byte name field is shortName, e.g. "TEST    TXT". go-diskfs allocates
// a cluster for every new file, even an empty one; other FAT32 implementations
// leave the field at zero until the file has content.
func clearStartCluster(t *testing.T, imgPath, shortName string) {
	t.Helper()
	if len(shortName) != 11 {
		t.Fatalf("short name %q must be 11 bytes", shortName)
	}
	raw, err := os.ReadFile(imgPath)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	partStart := int64(fat32Part1Start) * fat32SectorSize
	idx := bytes.Index(raw[partStart:partStart+fat32PartBytes], []byte(shortName))
	if idx < 0 {
		t.Fatalf("directory entry %q not found in partition 1", shortName)
	}
	if idx%32 != 0 {
		t.Fatalf("directory entry %q found at unaligned offset %d", shortName, idx)
	}
	entry := raw[partStart+int64(idx) : partStart+int64(idx)+32]
	if size := binary.LittleEndian.Uint32(entry[28:32]); size != 0 {
		t.Fatalf("directory entry %q has size %d, want an empty file", shortName, size)
	}
	binary.LittleEndian.PutUint16(entry[20:22], 0) // high word of first cluster
	binary.LittleEndian.PutUint16(entry[26:28], 0) // low word of first cluster
	if err := os.WriteFile(imgPath, raw, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
}

// checkFat32Copy reopens the image and asserts that partition 2 holds the same
// tree as partition 1, with the zero-length files intact.
func checkFat32Copy(t *testing.T, imgPath string, tree []fat32Entry) {
	t.Helper()

	d := openFat32Disk(t, imgPath)
	defer func() { _ = d.Close() }()

	srcFS, err := d.GetFilesystem(1)
	if err != nil {
		t.Fatalf("reopen source filesystem: %v", err)
	}
	dstFS, err := d.GetFilesystem(2)
	if err != nil {
		t.Fatalf("reopen target filesystem: %v", err)
	}
	checkListing(t, dstFS, ".", treeNames(tree))
	for _, e := range tree {
		if !e.dir && len(e.data) == 0 {
			checkZeroLength(t, dstFS, e.name)
		}
	}
	if err := CompareFS(srcFS, dstFS); err != nil {
		t.Errorf("CompareFS: %v", err)
	}
}

func treeNames(tree []fat32Entry) []string {
	names := make([]string, 0, len(tree))
	for _, e := range tree {
		names = append(names, e.name)
	}
	return names
}

// checkListing asserts that dir contains exactly want, in any order.
func checkListing(t *testing.T, fsys filesystem.FileSystem, dir string, want []string) {
	t.Helper()
	entries, err := fsys.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		got = append(got, e.Name())
	}
	sorted := slices.Clone(want)
	slices.Sort(got)
	slices.Sort(sorted)
	if !slices.Equal(got, sorted) {
		t.Errorf("dir %s: listing = %v, want %v", dir, got, sorted)
	}
}

// checkZeroLength asserts the named file exists, reports size 0, and reads back
// as empty rather than erroring out.
func checkZeroLength(t *testing.T, fsys filesystem.FileSystem, name string) {
	t.Helper()
	fi, err := fsys.Stat(name)
	if err != nil {
		t.Errorf("stat %s: %v", name, err)
		return
	}
	if fi.Size() != 0 {
		t.Errorf("%s: size = %d, want 0", name, fi.Size())
	}
	f, err := fsys.Open(name)
	if err != nil {
		t.Errorf("open %s: %v", name, err)
		return
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("read %s: %v", name, err)
		return
	}
	if len(data) != 0 {
		t.Errorf("%s: read %d bytes, want 0", name, len(data))
	}
}
