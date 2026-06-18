//go:build linux

package disk_test

// On-device regression test for the BLKPG re-read fallback added in
// ReReadPartitionTable. The file-based tests cannot reach it: a regular file
// has no kernel partition table to re-read, and BLKRRPART only fails (and the
// fallback only fires) on a real block device whose partitions are in use.
//
// The test builds a GPT image, exposes it as a loop device, mounts one
// partition so a whole-disk BLKRRPART is guaranteed to fail with EBUSY, then
// rewrites the table to shrink a *different* partition and confirms the kernel
// picked up the change via the per-partition BLKPG path while the mounted
// sibling was left untouched. It requires root (losetup/mount) and is skipped
// otherwise.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/partition/gpt"
	"golang.org/x/sys/unix"
)

// blkrrpart is the BLKRRPART ioctl (linux uapi/linux/fs.h); duplicated here
// because the disk package keeps it unexported. Used only to assert the
// precondition that a plain whole-disk re-read fails on the busy device.
const blkrrpart = 0x125f

const sectorBytes = 512

func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func requireCmd(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not available: %v", name, err)
	}
}

// readSysPartitions returns the kernel's view of base's partitions (e.g.
// "loop7"), keyed by partition number, with start and size in bytes. /sys
// reports both in 512-byte sectors regardless of logical sector size.
func readSysPartitions(t *testing.T, base string) map[int][2]int64 {
	t.Helper()
	dir := "/sys/block/" + base
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	out := make(map[int][2]int64)
	for _, e := range entries {
		pnoRaw, err := os.ReadFile(filepath.Join(dir, e.Name(), "partition"))
		if err != nil {
			continue // not a partition subdirectory
		}
		pno, err := strconv.Atoi(strings.TrimSpace(string(pnoRaw)))
		if err != nil {
			continue
		}
		start := readSysInt(t, filepath.Join(dir, e.Name(), "start"))
		size := readSysInt(t, filepath.Join(dir, e.Name(), "size"))
		out[pno] = [2]int64{start * sectorBytes, size * sectorBytes}
	}
	return out
}

func readSysInt(t *testing.T, path string) int64 {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return n
}

func isMounted(t *testing.T, dev string) bool {
	t.Helper()
	b, err := os.ReadFile("/proc/mounts")
	if err != nil {
		t.Fatalf("read /proc/mounts: %v", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == dev {
			return true
		}
	}
	return false
}

// mkPartition builds a GPT partition spanning [startSector, startSector+sectors).
func mkPartition(index int, name string, startSector, sectors uint64) *gpt.Partition {
	return &gpt.Partition{
		Index: index,
		Start: startSector,
		End:   startSector + sectors - 1,
		Size:  sectors * sectorBytes,
		Type:  gpt.LinuxFilesystem,
		Name:  name,
	}
}

func TestReReadPartitionTableBLKPGFallback(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root for losetup/mount")
	}
	requireCmd(t, "losetup")
	requireCmd(t, "mkfs.ext4")
	requireCmd(t, "mount")

	tmp := t.TempDir()
	imgPath := filepath.Join(tmp, "disk.img")

	// 64 MiB image with two 8 MiB partitions: p1 (mounted, kept) and p2 (shrunk).
	const (
		diskSize     = int64(64 << 20)
		p1Start      = uint64(2048)
		eightMiBSecs = uint64(8 << 20 / sectorBytes) // 16384 sectors
		fourMiBSecs  = uint64(4 << 20 / sectorBytes) // 8192 sectors
		p2Start      = p1Start + eightMiBSecs
	)
	if f, err := os.Create(imgPath); err != nil {
		t.Fatalf("create image: %v", err)
	} else {
		if err := f.Truncate(diskSize); err != nil {
			t.Fatalf("truncate image: %v", err)
		}
		f.Close()
	}

	// Write the initial table to the file (regular file: ReReadPartitionTable
	// inside Partition() is a no-op, so no fallback involved here).
	initBk, err := file.OpenFromPathWithExclusive(imgPath, false, false)
	if err != nil {
		t.Fatalf("open image: %v", err)
	}
	dInit, err := diskfs.OpenBackend(initBk, diskfs.WithSectorSize(diskfs.SectorSize512))
	if err != nil {
		t.Fatalf("OpenBackend image: %v", err)
	}
	initTable := &gpt.Table{
		LogicalSectorSize:  sectorBytes,
		PhysicalSectorSize: sectorBytes,
		ProtectiveMBR:      true,
		Partitions: []*gpt.Partition{
			mkPartition(1, "busy", p1Start, eightMiBSecs),
			mkPartition(2, "resizeme", p2Start, eightMiBSecs),
		},
	}
	if err := dInit.Partition(initTable); err != nil {
		t.Fatalf("write initial table: %v", err)
	}
	if err := initBk.Close(); err != nil {
		t.Fatalf("close image: %v", err)
	}

	// Expose as a loop device with partition scanning.
	loopdev := run(t, "losetup", "--find", "--partscan", "--show", imgPath)
	base := filepath.Base(loopdev)
	t.Cleanup(func() {
		// Detach only after every partition is unmounted (cleanup below runs first).
		_ = exec.Command("losetup", "--detach", loopdev).Run()
	})
	// Best-effort wait for the partition device nodes.
	if _, err := exec.LookPath("udevadm"); err == nil {
		_ = exec.Command("udevadm", "settle").Run()
	}

	p1Dev := loopdev + "p1"
	if _, err := os.Stat(p1Dev); err != nil {
		t.Fatalf("partition node %s did not appear: %v", p1Dev, err)
	}

	// Make p1 busy by mounting an ext4 on it -- guarantees BLKRRPART EBUSY.
	run(t, "mkfs.ext4", "-F", "-q", p1Dev)
	mnt := filepath.Join(tmp, "mnt")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatalf("mkdir mnt: %v", err)
	}
	run(t, "mount", p1Dev, mnt)
	t.Cleanup(func() { _ = exec.Command("umount", mnt).Run() })
	if !isMounted(t, p1Dev) {
		t.Fatalf("%s is not mounted; cannot guarantee the fallback path", p1Dev)
	}

	before := readSysPartitions(t, base)
	if _, ok := before[1]; !ok {
		t.Fatalf("kernel does not see partition 1 before resize: %v", before)
	}
	if _, ok := before[2]; !ok {
		t.Fatalf("kernel does not see partition 2 before resize: %v", before)
	}

	// Precondition: a plain whole-disk BLKRRPART must fail while p1 is mounted,
	// so the rewrite below is forced down the BLKPG fallback.
	df, err := os.OpenFile(loopdev, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", loopdev, err)
	}
	if _, rrErr := unix.IoctlGetInt(int(df.Fd()), blkrrpart); rrErr == nil {
		df.Close()
		t.Fatalf("BLKRRPART unexpectedly succeeded with %s mounted; cannot validate the BLKPG fallback", p1Dev)
	} else {
		t.Logf("BLKRRPART failed as expected on busy device: %v", rrErr)
	}
	df.Close()

	// Reopen the loop device the way the consumer does -- non-exclusive so the
	// open coexists with the mounted partition -- and shrink p2 to 4 MiB while
	// leaving p1 untouched.
	bk, err := file.OpenFromPathWithExclusive(loopdev, false, false)
	if err != nil {
		t.Fatalf("open loop device: %v", err)
	}
	d, err := diskfs.OpenBackend(bk, diskfs.WithSectorSize(diskfs.SectorSize512))
	if err != nil {
		t.Fatalf("OpenBackend loop device: %v", err)
	}
	newTable := &gpt.Table{
		LogicalSectorSize:  sectorBytes,
		PhysicalSectorSize: sectorBytes,
		ProtectiveMBR:      true,
		Partitions: []*gpt.Partition{
			mkPartition(1, "busy", p1Start, eightMiBSecs), // unchanged
			mkPartition(2, "resizeme", p2Start, fourMiBSecs),
		},
	}
	// Partition() writes the table and calls ReReadPartitionTable internally;
	// with p1 mounted that re-read takes the BLKPG fallback. A nil error here is
	// the fallback succeeding.
	if err := d.Partition(newTable); err != nil {
		t.Fatalf("Partition() on busy device failed (BLKPG fallback): %v", err)
	}
	if err := bk.Close(); err != nil {
		t.Fatalf("close loop device: %v", err)
	}

	if _, err := exec.LookPath("udevadm"); err == nil {
		_ = exec.Command("udevadm", "settle").Run()
	}

	after := readSysPartitions(t, base)

	// p1 must be byte-for-byte unchanged and still mounted.
	if after[1] != before[1] {
		t.Errorf("partition 1 geometry changed: before=%v after=%v", before[1], after[1])
	}
	if !isMounted(t, p1Dev) {
		t.Errorf("%s was unmounted by the re-read; the fallback disturbed an unchanged partition", p1Dev)
	}

	// p2 must reflect the new, smaller size at the same start.
	wantStart := int64(p2Start) * sectorBytes
	wantSize := int64(fourMiBSecs) * sectorBytes
	if after[2][0] != wantStart {
		t.Errorf("partition 2 start = %d, want %d", after[2][0], wantStart)
	}
	if after[2][1] != wantSize {
		t.Errorf("partition 2 size = %d, want %d (kernel did not pick up the shrink)", after[2][1], wantSize)
	}
}
