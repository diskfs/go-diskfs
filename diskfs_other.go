//go:build !windows && !linux && !darwin

package diskfs

import (
	"errors"
	"os"
)

// getSectorSizes get the logical and physical sector sizes for a block device
func getSectorSizes(f *os.File) (logicalSectorSize, physicalSectorSize int64, err error) {
	return 0, 0, errors.New("block devices not supported on this platform")
}
