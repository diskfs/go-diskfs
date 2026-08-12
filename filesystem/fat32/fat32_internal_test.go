package fat32

import (
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/backend/file"
)

var sweepSectorSizes = []int64{int64(SectorSize512), int64(SectorSize4096)}

const (
	sweepMinMiB = 16
	sweepMaxMiB = 4096
)

// TestLayoutFatHoldsEveryCluster checks the defining invariant of the FAT: each
// FAT copy must have room for one entry per cluster plus entries 0 and 1, which
// are reserved and describe no cluster. The sizes that violate it are sparse —
// 64 MiB and 4 GiB are among them while their neighbours are fine — so the
// range is swept rather than sampled. fsck.vfat rejects a filesystem that fails
// this with "Filesystem has N clusters but only space for N-2 FAT entries".
func TestLayoutFatHoldsEveryCluster(t *testing.T) {
	for _, blocksize := range sweepSectorSizes {
		for mib := int64(sweepMinMiB); mib <= sweepMaxMiB; mib++ {
			l := layout(mib*MB, blocksize)
			entries := uint64(l.sectorsPerFat) * uint64(blocksize) / 4
			if want := uint64(l.clusterCount) + 2; entries < want {
				t.Errorf("size %d MiB, sector size %d: FAT holds %d entries, need %d for %d clusters",
					mib, blocksize, entries, want, l.clusterCount)
			}
		}
	}
}

// TestLayoutFatHoldsEveryClusterLargeVolumes runs the same invariant at GiB
// granularity up to the largest FAT32 volume, where a 32-bit sector count
// multiplied by four no longer fits in 32 bits.
func TestLayoutFatHoldsEveryClusterLargeVolumes(t *testing.T) {
	for _, blocksize := range sweepSectorSizes {
		for size := 4 * GB; size <= Fat32MaxSize; size += GB {
			l := layout(size, blocksize)
			entries := uint64(l.sectorsPerFat) * uint64(blocksize) / 4
			if want := uint64(l.clusterCount) + 2; entries < want {
				t.Errorf("size %d GiB, sector size %d: FAT holds %d entries, need %d for %d clusters",
					size/GB, blocksize, entries, want, l.clusterCount)
			}
		}
	}
}

// TestLayoutFitsInVolume checks the other half of the sizing rule: the reserved
// sectors, both FAT copies and the data area must fit within the volume.
func TestLayoutFitsInVolume(t *testing.T) {
	for _, blocksize := range sweepSectorSizes {
		for mib := int64(sweepMinMiB); mib <= sweepMaxMiB; mib++ {
			l := layout(mib*MB, blocksize)
			if used := layoutSectorsUsed(l); used > int64(l.totalSectors) {
				t.Errorf("size %d MiB, sector size %d: layout uses %d sectors of %d",
					mib, blocksize, used, l.totalSectors)
			}
		}
	}
}

// TestLayoutFitsInLargeVolume is TestLayoutFitsInVolume at GiB granularity up
// to the largest FAT32 volume.
func TestLayoutFitsInLargeVolume(t *testing.T) {
	for _, blocksize := range sweepSectorSizes {
		for size := 4 * GB; size <= Fat32MaxSize; size += GB {
			l := layout(size, blocksize)
			if used := layoutSectorsUsed(l); used > int64(l.totalSectors) {
				t.Errorf("size %d GiB, sector size %d: layout uses %d sectors of %d",
					size/GB, blocksize, used, l.totalSectors)
			}
		}
	}
}

func layoutSectorsUsed(l fsLayout) int64 {
	return int64(reservedSectors) + 2*int64(l.sectorsPerFat) +
		int64(l.clusterCount)*int64(l.sectorsPerCluster)
}

// TestCreateWritesConsistentFat runs the same invariant against the boot sector
// Create actually writes, at sizes where fsck.vfat reported a FAT two entries
// too small, and at a volume large enough to overflow a 32-bit sector count.
func TestCreateWritesConsistentFat(t *testing.T) {
	tests := []struct {
		size      int64
		blocksize int64
	}{
		{64 * MB, int64(SectorSize512)},
		{129 * MB, int64(SectorSize512)},
		{259 * MB, int64(SectorSize512)},
		{449 * MB, int64(SectorSize4096)},
		{4096 * MB, int64(SectorSize512)},
		{512 * GB, int64(SectorSize512)},
	}
	for _, tt := range tests {
		fs, err := Create(testBackend(t, tt.size), tt.size, 0, tt.blocksize, "TESTFS", true)
		if err != nil {
			t.Fatalf("size %d, sector size %d: Create: %v", tt.size, tt.blocksize, err)
		}
		bpb := fs.bpbFat32.Dos331BPB
		spf := uint64(fs.bpbFat32.sectorsPerFat)
		dataSectors := uint64(bpb.TotalSectors32) - uint64(bpb.Dos20BPB.ReservedSectors) - 2*spf
		clusters := dataSectors / uint64(bpb.Dos20BPB.SectorsPerCluster)
		entries := spf * uint64(tt.blocksize) / 4
		if entries < clusters+2 {
			t.Errorf("size %d, sector size %d: FAT holds %d entries, need %d for %d clusters",
				tt.size, tt.blocksize, entries, clusters+2, clusters)
		}
		if err := fs.Close(); err != nil {
			t.Fatalf("size %d, sector size %d: Close: %v", tt.size, tt.blocksize, err)
		}
	}
}

// testBackend returns a sparse image file of the given size to build a
// filesystem on.
func testBackend(t *testing.T, size int64) backend.Storage {
	t.Helper()
	b, err := file.CreateFromPath(filepath.Join(t.TempDir(), "fat32.img"), size)
	if err != nil {
		t.Fatalf("creating backend of %d bytes: %v", size, err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}
