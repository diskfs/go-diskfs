//go:build (aix || darwin || dragonfly || freebsd || netbsd || openbsd || solaris) && !linux

package disk

import "fmt"

// reconcilePartitionsBLKPG is a Linux-only mechanism: it relies on the BLKPG
// ioctl and the /sys/block partition layout, neither of which exists on other
// unix platforms. There is no portable fallback, so report that the re-read
// could not be completed.
func (d *Disk) reconcilePartitionsBLKPG(_ int) error {
	return fmt.Errorf("BLKPG partition reconciliation is only supported on Linux")
}
