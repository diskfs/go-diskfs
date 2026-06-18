//go:build linux

package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	blkpg = 0x1269

	blkpgAddPartition = 1
	blkpgDelPartition = 2

	blkpgDevNameLth = 64
)

// blkpgPartition mirrors the kernel's struct blkpg_partition (linux/blkpg.h):
// start and length are in bytes, pno is the partition number.
type blkpgPartition struct {
	start   int64
	length  int64
	pno     int32
	devname [blkpgDevNameLth]byte
	volname [blkpgDevNameLth]byte
}

// blkpgIoctlArg mirrors the kernel's struct blkpg_ioctl_arg. The explicit
// padding aligns the data pointer to an 8-byte boundary, matching the C layout
// on 64-bit platforms.
type blkpgIoctlArg struct {
	op      int32
	flags   int32
	datalen int32
	_       int32
	data    unsafe.Pointer
}

// reconcilePartitionsBLKPG brings the kernel's partition list in line with the
// table go-diskfs just wrote, touching only the entries that actually changed so
// that unrelated busy/mounted partitions are left alone.
func (d *Disk) reconcilePartitionsBLKPG(fd int) error {
	path := d.Backend.Path()
	if path == "" {
		return fmt.Errorf("backend has no path; cannot reconcile partitions")
	}
	existing, err := readKernelPartitions(filepath.Base(path))
	if err != nil {
		return err
	}

	// desired: partition number -> [startBytes, lengthBytes] from the table.
	desired := make(map[int][2]int64)
	if d.Table != nil {
		for _, p := range d.Table.GetPartitions() {
			desired[p.GetIndex()] = [2]int64{p.GetStart(), p.GetSize()}
		}
	}

	// Delete kernel partitions that are gone, or whose geometry changed (a
	// changed one is re-added below). Leave unchanged partitions untouched.
	for pno, have := range existing {
		if want, ok := desired[pno]; ok && want == have {
			continue
		}
		if err := blkpgCall(fd, blkpgDelPartition, pno, 0, 0); err != nil {
			return fmt.Errorf("del partition %d: %w", pno, err)
		}
		delete(existing, pno)
	}

	// Add partitions that are new or were just removed for a geometry change.
	for pno, want := range desired {
		if have, ok := existing[pno]; ok && have == want {
			continue
		}
		if err := blkpgCall(fd, blkpgAddPartition, pno, want[0], want[1]); err != nil {
			return fmt.Errorf("add partition %d: %w", pno, err)
		}
	}
	return nil
}

// blkpgCall issues a single BLKPG add/delete for partition pno.
func blkpgCall(fd, op, pno int, start, length int64) error {
	part := blkpgPartition{start: start, length: length, pno: int32(pno)}
	arg := blkpgIoctlArg{
		op:      int32(op),
		datalen: int32(unsafe.Sizeof(part)),
		data:    unsafe.Pointer(&part),
	}
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), uintptr(blkpg), uintptr(unsafe.Pointer(&arg)))
	// Deleting a partition the kernel never created is a no-op for our purpose.
	if errno == unix.ENXIO && op == blkpgDelPartition {
		return nil
	}
	if errno != 0 {
		return errno
	}
	return nil
}

// readKernelPartitions returns the kernel's current view of the partitions on
// the block device named base (e.g. "sda", "nbd0", "nvme0n1"), keyed by
// partition number, with start and length in bytes. /sys reports start and size
// in 512-byte sectors regardless of the device's logical sector size.
func readKernelPartitions(base string) (map[int][2]int64, error) {
	dir := "/sys/block/" + base
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
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
		start := readSysSectors(filepath.Join(dir, e.Name(), "start"))
		size := readSysSectors(filepath.Join(dir, e.Name(), "size"))
		if start < 0 || size < 0 {
			continue
		}
		out[pno] = [2]int64{start * 512, size * 512}
	}
	return out, nil
}

func readSysSectors(path string) int64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return -1
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return -1
	}
	return n
}
