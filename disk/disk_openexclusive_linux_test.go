//go:build linux

package disk_test

// On-device regression test for the non-exclusive whole-disk open added as
// file.OpenFromPathWithExclusive. resize2fs/e2fsck open their target partition
// O_EXCL; if a consumer holds the *parent* whole disk open with O_EXCL (what
// file.OpenFromPath does by default), the kernel refuses that child-partition
// O_EXCL open with EBUSY ("device is in use"). Opening the disk non-exclusively
// (exclusive=false) lets the child open succeed, so the filesystem tools can
// run on a partition while go-diskfs still holds the disk for the GPT rewrite.
//
// The file-based tests cannot reach this: O_EXCL is a no-op on a regular file
// and a file has no child partition nodes, so the conflict only appears on a
// real block device. The two subtests prove both directions in one run -- the
// exclusive open reproduces the bug, the non-exclusive open is the fix. It
// requires root (losetup) and is skipped otherwise.

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/partition/gpt"
)

func TestOpenExclusiveBlocksChildPartitionFsck(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root for losetup")
	}
	requireCmd(t, "losetup")

	tmp := t.TempDir()
	imgPath := filepath.Join(tmp, "disk.img")

	// 64 MiB image with a single 8 MiB partition; the partition is the fsck
	// target, the whole disk is what go-diskfs holds open.
	const (
		diskSize     = int64(64 << 20)
		p1Start      = uint64(2048)
		eightMiBSecs = uint64(8 << 20 / sectorBytes)
	)
	f, err := os.Create(imgPath)
	if err != nil {
		t.Fatalf("create image: %v", err)
	}
	if err := f.Truncate(diskSize); err != nil {
		t.Fatalf("truncate image: %v", err)
	}
	f.Close()

	// Lay down the GPT on the file (regular file: no exclusive-open conflict).
	bk, err := file.OpenFromPathWithExclusive(imgPath, false, false)
	if err != nil {
		t.Fatalf("open image: %v", err)
	}
	d, err := diskfs.OpenBackend(bk, diskfs.WithSectorSize(diskfs.SectorSize512))
	if err != nil {
		t.Fatalf("OpenBackend image: %v", err)
	}
	table := &gpt.Table{
		LogicalSectorSize:  sectorBytes,
		PhysicalSectorSize: sectorBytes,
		ProtectiveMBR:      true,
		Partitions: []*gpt.Partition{
			mkPartition(1, "child", p1Start, eightMiBSecs),
		},
	}
	if err := d.Partition(table); err != nil {
		t.Fatalf("write table: %v", err)
	}
	if err := bk.Close(); err != nil {
		t.Fatalf("close image: %v", err)
	}

	// Expose as a loop device with partition scanning so the child node appears.
	loopdev := run(t, "losetup", "--find", "--partscan", "--show", imgPath)
	t.Cleanup(func() { _ = exec.Command("losetup", "--detach", loopdev).Run() })
	if _, err := exec.LookPath("udevadm"); err == nil {
		_ = exec.Command("udevadm", "settle").Run()
	}

	childDev := loopdev + "p1"
	if _, err := os.Stat(childDev); err != nil {
		t.Fatalf("child partition node %s did not appear: %v", childDev, err)
	}

	// openChildExclusive mimics e2fsck/resize2fs, which open their target
	// O_EXCL. Returns the open error (nil = the tool could run).
	openChildExclusive := func() error {
		cf, err := os.OpenFile(childDev, os.O_RDWR|os.O_EXCL, 0)
		if err != nil {
			return err
		}
		return cf.Close()
	}

	// Precondition: with nothing holding the parent, the child opens
	// exclusively. If even this fails, the environment cannot host the test
	// (e.g. an auto-mounter grabbed the partition) -- skip rather than fail.
	if err := openChildExclusive(); err != nil {
		t.Skipf("child O_EXCL open fails with no parent holder (%v); environment cannot host this test", err)
	}

	// The bug: holding the parent disk O_EXCL (the OpenFromPath default) makes
	// the kernel refuse the child's O_EXCL open.
	t.Run("ParentExclusiveBlocksChild", func(t *testing.T) {
		parent, err := file.OpenFromPathWithExclusive(loopdev, false, true)
		if err != nil {
			t.Fatalf("open parent O_EXCL: %v", err)
		}
		defer func() { _ = parent.Close() }()

		if err := openChildExclusive(); err == nil {
			t.Fatal("child O_EXCL open succeeded while parent held O_EXCL; the exclusive open did not claim the disk")
		} else {
			t.Logf("child O_EXCL open blocked as expected: %v", err)
		}
	})

	// The fix: holding the parent non-exclusively leaves the child openable, so
	// resize2fs/e2fsck can run on it.
	t.Run("ParentNonExclusiveAllowsChild", func(t *testing.T) {
		parent, err := file.OpenFromPathWithExclusive(loopdev, false, false)
		if err != nil {
			t.Fatalf("open parent non-exclusive: %v", err)
		}
		defer func() { _ = parent.Close() }()

		if err := openChildExclusive(); err != nil {
			t.Fatalf("child O_EXCL open failed while parent held non-exclusively: %v; resize2fs/e2fsck would be blocked", err)
		}
	})
}
