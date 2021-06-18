package diskfs

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// this constants should be part of "golang.org/x/sys/unix", but aren't, yet
const (
	DKIOCGETBLOCKSIZE         = 0x40046418
	DKIOCGETPHYSICALBLOCKSIZE = 0x4004644D
	DKIOCGETBLOCKCOUNT        = 0x40086419
)

// getSectorSizes get the logical and physical sector sizes for a block device
func getSectorSizes(f *os.File) (int64, int64, error) {
	/*
		ioctl(fd, BLKPBSZGET, &physicalsectsize);

	*/
	fd := f.Fd()

	logicalSectorSize, err := unix.IoctlGetInt(int(fd), DKIOCGETBLOCKSIZE)
	if err != nil {
		return 0, 0, fmt.Errorf("Unable to get device logical sector size: %v", err)
	}
	physicalSectorSize, err := unix.IoctlGetInt(int(fd), DKIOCGETPHYSICALBLOCKSIZE)
	if err != nil {
		return 0, 0, fmt.Errorf("Unable to get device physical sector size: %v", err)
	}
	return int64(logicalSectorSize), int64(physicalSectorSize), nil
}
