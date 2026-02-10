//go:build windows

package sync

import (
	"io/fs"
	"syscall"
	"time"
)

func getAccessTime(info fs.FileInfo) time.Time {
	sys := info.Sys()
	if sys == nil {
		// return zero time
		return time.Time{}
	}
	stat := sys.(*syscall.Win32FileAttributeData)
	return time.Unix(0, stat.LastAccessTime.Nanoseconds())
}
