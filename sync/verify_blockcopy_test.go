package sync

import (
	"os"
	"path/filepath"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/partition/gpt"
)

// TestVerifyBlockCopyTargetLargerThanSource covers the grow case: a smaller
// source partition copied into a larger target. verifyBlockCopy must compare
// only the leading expectedSize bytes and accept a target that is larger than
// the source, while still catching a real mismatch within that region.
func TestVerifyBlockCopyTargetLargerThanSource(t *testing.T) {
	const (
		sectorSize = 512
		diskSize   = 64 * 1024 * 1024
		srcStart   = 2048             // sectors => 1 MiB in
		tgtStart   = srcStart + 32768 // 16 MiB after source start
		srcSize    = 8 * 1024 * 1024  // source partition
		tgtSize    = 24 * 1024 * 1024 // target is 3x larger
		expected   = int64(srcSize)
	)

	imgPath := filepath.Join(t.TempDir(), "disk.img")
	d, err := diskfs.Create(imgPath, diskSize, sectorSize)
	if err != nil {
		t.Fatalf("create disk: %v", err)
	}
	table := &gpt.Table{
		LogicalSectorSize:  sectorSize,
		PhysicalSectorSize: sectorSize,
		ProtectiveMBR:      true,
		Partitions: []*gpt.Partition{
			{Index: 1, Start: srcStart, Size: srcSize, Type: gpt.LinuxFilesystem, Name: "source"},
			{Index: 2, Start: tgtStart, Size: tgtSize, Type: gpt.LinuxFilesystem, Name: "target"},
		},
	}
	if err := d.Partition(table); err != nil {
		t.Fatalf("write partition table: %v", err)
	}

	// Write identical leading expectedSize bytes into both partitions; the
	// target's trailing bytes stay zero and must not affect the result.
	content := make([]byte, expected)
	for i := range content {
		content[i] = byte(i*7 + 1)
	}
	writeAt := func(off int64, b []byte) {
		t.Helper()
		img, err := os.OpenFile(imgPath, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open image for write: %v", err)
		}
		defer img.Close()
		if _, err := img.WriteAt(b, off); err != nil {
			t.Fatalf("write image at %d: %v", off, err)
		}
	}
	writeAt(srcStart*sectorSize, content)
	writeAt(tgtStart*sectorSize, content)

	openDisk := func() *disk.Disk {
		t.Helper()
		dd, err := diskfs.Open(imgPath, diskfs.WithSectorSize(sectorSize))
		if err != nil {
			t.Fatalf("open disk: %v", err)
		}
		if _, err := dd.GetPartitionTable(); err != nil {
			t.Fatalf("read partition table: %v", err)
		}
		return dd
	}

	// Grow case: larger target, identical leading bytes — must verify clean.
	if err := verifyBlockCopy(openDisk(), 1, 2, expected); err != nil {
		t.Errorf("grow with identical leading bytes: unexpected error: %v", err)
	}

	// Corrupt a byte inside the compared region of the target; must be caught.
	writeAt(tgtStart*sectorSize, []byte{^content[0]})
	if err := verifyBlockCopy(openDisk(), 1, 2, expected); err == nil {
		t.Error("corrupted target within compared region: expected mismatch error, got nil")
	}
}
