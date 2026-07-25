package sync

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/partition/gpt"
)

// The mtools round trip needs enough clusters for fsck.vfat to accept the
// filesystem as FAT32 (65525 minimum), so its partitions are larger than the
// ones the pure-Go tests use.
const (
	mtoolsPartSectors = 196608 // 96 MiB
	mtoolsPart1Start  = 2048
	mtoolsPart2Start  = mtoolsPart1Start + mtoolsPartSectors
	mtoolsPartBytes   = int64(mtoolsPartSectors) * fat32SectorSize
	mtoolsDiskSize    = (int64(mtoolsPart2Start) + mtoolsPartSectors + mtoolsPart1Start) * fat32SectorSize
)

// TestCopyFat32MtoolsRoundTrip builds the source filesystem with mkfs.vfat and
// mcopy, copies the partition with CopyFileSystem, and hands the result back to
// mdir and fsck.vfat: every file the external tools wrote must still be found by
// the external tools afterwards, and the copy must pass fsck.
func TestCopyFat32MtoolsRoundTrip(t *testing.T) {
	mkfsVfat := fatTool(t, "mkfs.vfat")
	mcopy := fatTool(t, "mcopy")
	mdir := fatTool(t, "mdir")
	fsckVfat := fatTool(t, "fsck.vfat")

	dir := t.TempDir()

	// 1. mkfs.vfat creates the source filesystem, mcopy populates it. An empty
	// file written this way has no cluster allocated, and a dot-prefixed name
	// gets a generated 8.3 short name such as BOOT_R~1.
	srcFat := filepath.Join(dir, "source.fat32")
	if err := os.Truncate(createEmptyFile(t, srcFat), mtoolsPartBytes); err != nil {
		t.Fatalf("size source filesystem image: %v", err)
	}
	runTool(t, dir, mkfsVfat, "-F", "32", "-n", "SOURCE", srcFat)

	empty := filepath.Join(dir, "empty-input")
	_ = createEmptyFile(t, empty)
	kernel := filepath.Join(dir, "kernel-input")
	if err := os.WriteFile(kernel, []byte("not empty"), 0o600); err != nil {
		t.Fatalf("write kernel input: %v", err)
	}
	runTool(t, dir, mcopy, "-i", srcFat, empty, "::/TEST.TXT")
	runTool(t, dir, mcopy, "-i", srcFat, empty, "::/"+dotMarkerFile)
	runTool(t, dir, mcopy, "-i", srcFat, kernel, "::/kernel")

	// 2. Place that filesystem in partition 1 of a two-partition disk.
	imgPath := filepath.Join(dir, "disk.img")
	d, err := diskfs.Create(imgPath, mtoolsDiskSize, fat32SectorSize)
	if err != nil {
		t.Fatalf("create disk: %v", err)
	}
	table := &gpt.Table{
		LogicalSectorSize:  fat32SectorSize,
		PhysicalSectorSize: fat32SectorSize,
		ProtectiveMBR:      true,
		Partitions: []*gpt.Partition{
			{Index: 1, Start: mtoolsPart1Start, Size: uint64(mtoolsPartBytes), Type: gpt.EFISystemPartition, Name: "source"},
			{Index: 2, Start: mtoolsPart2Start, Size: uint64(mtoolsPartBytes), Type: gpt.EFISystemPartition, Name: "target"},
		},
	}
	if err := d.Partition(table); err != nil {
		t.Fatalf("write partition table: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close disk after partitioning: %v", err)
	}
	copyFileRange(t, srcFat, imgPath, int64(mtoolsPart1Start)*fat32SectorSize)

	// 3. go-diskfs copies the partition file by file.
	d, err = diskfs.Open(imgPath, diskfs.WithSectorSize(fat32SectorSize))
	if err != nil {
		t.Fatalf("open disk: %v", err)
	}
	if _, err := d.GetPartitionTable(); err != nil {
		t.Fatalf("read partition table: %v", err)
	}
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

	// 4. The external tools must find everything in the copy.
	dstFat := filepath.Join(dir, "target.fat32")
	extractRange(t, imgPath, dstFat, int64(mtoolsPart2Start)*fat32SectorSize, mtoolsPartBytes)

	listing := runTool(t, dir, mdir, "-b", "-i", dstFat, "::/")
	for _, name := range []string{dotMarkerFile, "TEST.TXT", "kernel"} {
		if !strings.Contains(listing, "::/"+name+"\n") {
			t.Errorf("mdir does not list %s in the copy:\n%s", name, listing)
		}
	}

	fsckOut, fsckErr := toolOutput(dir, fsckVfat, "-n", dstFat)
	if fsckErr != nil {
		t.Errorf("fsck.vfat on the copy: %v\n%s", fsckErr, fsckOut)
	}
	for _, complaint := range []string{"Bad short file name", "Auto-renaming"} {
		if strings.Contains(fsckOut, complaint) {
			t.Errorf("fsck.vfat reports %q on the copy:\n%s", complaint, fsckOut)
		}
	}
}

// fatTool locates an external FAT tool, skipping the test when it is missing.
// mkfs.vfat and fsck.vfat live in /usr/sbin, which is not always on PATH.
func fatTool(t *testing.T, name string) string {
	t.Helper()
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	for _, dir := range []string{"/usr/sbin", "/sbin"} {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Skipf("%s not available", name)
	return ""
}

// runTool runs an external tool and fails the test if it does not succeed.
func runTool(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	out, err := toolOutput(dir, name, args...)
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
	return out
}

// toolOutput runs an external tool, returning its combined output. mtools
// refuses to touch an image without a partition table unless the check is
// disabled.
func toolOutput(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "MTOOLS_SKIP_CHECK=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func createEmptyFile(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
	return path
}

// copyFileRange writes the whole of src into dst at the given offset.
func copyFileRange(t *testing.T, src, dst string, offset int64) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	f, err := os.OpenFile(dst, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s: %v", dst, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteAt(data, offset); err != nil {
		t.Fatalf("write %s at %d: %v", dst, offset, err)
	}
}

// extractRange writes size bytes of src starting at offset into dst.
func extractRange(t *testing.T, src, dst string, offset, size int64) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open %s: %v", src, err)
	}
	defer func() { _ = in.Close() }()
	buf := make([]byte, size)
	if _, err := in.ReadAt(buf, offset); err != nil {
		t.Fatalf("read %s at %d: %v", src, offset, err)
	}
	if err := os.WriteFile(dst, buf, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}
