//go:build linux || darwin || freebsd || netbsd || openbsd

package sync

import (
	"io/fs"
	"time"

	"golang.org/x/sys/unix"
)

func getAccessTime(info fs.FileInfo) time.Time {
	sys := info.Sys()
	if sys == nil {
		// return zero time
		return time.Time{}
	}
	stat, ok := sys.(*unix.Stat_t)
	if !ok {
		return time.Time{}
	}
	//nolint:unconvert // normally not needed, but useful for 32-bit operating systems
	return time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
}
