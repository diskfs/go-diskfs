package squashfs

import (
	"os"
	"syscall"
)

//nolint:revive // prefer named unused parameter
func getDeviceNumbers(path string) (major, minor uint32, err error) {
	return 0, 0, syscall.EWINDOWS
}

//nolint:revive // prefer named unused parameter
func getFileProperties(fi os.FileInfo) (links, uid, gid uint32) {
	return 0, 0, 0
}
