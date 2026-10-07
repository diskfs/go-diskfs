package diskfs

import (
	"errors"
	"os"
)

// getBlockDeviceSize get the size of an opened block device in Bytes.
//
//nolint:revive // prefer named unused parameter
func getBlockDeviceSize(f *os.File) (size int64, err error) {
	return 0, errors.New("block devices not supported on windows")
}

// getSectorSizes get the logical and physical sector sizes for a block device
//
//nolint:revive // prefer named unused parameter
func getSectorSizes(f *os.File) (logical, physical int64, err error) {
	return 0, 0, errors.New("block devices not supported on windows")
}
