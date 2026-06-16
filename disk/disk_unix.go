//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package disk

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// blkrrpart is the Linux BLKRRPART ioctl (uapi/linux/fs.h). The call compiles
// on every unix via unix.IoctlGetInt and simply fails at runtime on platforms
// that do not implement it.
const blkrrpart = 0x125f

// ReReadPartitionTable makes the kernel pick up the on-disk partition table.
//
// It first tries a whole-disk re-read via the BLKRRPART ioctl. That is
// all-or-nothing and fails with EBUSY whenever the kernel or udev is
// transiently holding any partition -- which is common immediately after a
// partition-table write, because the write triggers a udev re-probe -- or when
// a partition is mounted. When BLKRRPART fails, it falls back to per-partition
// BLKPG reconciliation (the same mechanism partx/parted use): it adds, removes,
// and re-creates only the entries whose geometry changed, via BLKPG ioctls that
// do not require a whole-disk exclusive re-read. This keeps the library free of
// any dependency on external tools such as partx. The fallback is Linux-only;
// on other platforms it reports that the re-read could not be completed.
func (d *Disk) ReReadPartitionTable() error {
	// the partition table needs to be re-read only if
	// the disk file is an actual block device
	devInfo, err := d.Backend.Stat()
	if err != nil {
		return err
	}
	if devInfo.Mode()&os.ModeDevice == 0 {
		return nil
	}

	osFile, err := d.Backend.Sys()
	if err != nil {
		return err
	}
	fd := int(osFile.Fd())

	if _, rrErr := unix.IoctlGetInt(fd, blkrrpart); rrErr != nil {
		if pgErr := d.reconcilePartitionsBLKPG(fd); pgErr != nil {
			return fmt.Errorf("unable to re-read the partition table. Kernel still uses old partition table (BLKRRPART: %v; BLKPG: %v)", rrErr, pgErr)
		}
	}

	return nil
}
