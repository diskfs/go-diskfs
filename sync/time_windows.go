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
	stat, ok := sys.(*syscall.Win32FileAttributeData)
	if !ok {
		// return zero time if type assertion fails
		return time.Time{}
	}
	return time.Unix(0, stat.LastAccessTime.Nanoseconds())
}
