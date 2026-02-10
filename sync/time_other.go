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
	return time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
}
