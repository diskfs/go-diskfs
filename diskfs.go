// Package diskfs implements methods for creating and manipulating disks and filesystems
//
// methods for creating and manipulating disks and filesystems, whether block devices
// in /dev or direct disk images. This does **not**
// mount any disks or filesystems, neither directly locally nor via a VM. Instead, it manipulates the
// bytes directly.
//
// This is not intended as a replacement for operating system filesystem and disk drivers. Instead,
// it is intended to make it easy to work with partitions, partition tables and filesystems directly
// without requiring operating system mounts.
//
// Some examples:
//
// 1. Create a disk image of size 10MB with a FAT32 filesystem spanning the entire disk.
//
//     import diskfs "github.com/diskfs/go-diskfs"
//     size := 10*1024*1024 // 10 MB
//
//     diskImg := "/tmp/disk.img"
//     disk := diskfs.Create(diskImg, size, diskfs.Raw)
//
//     fs, err := disk.CreateFilesystem(0, diskfs.TypeFat32)
//
// 2. Create a disk of size 20MB with an MBR partition table, a single partition beginning at block 2048 (1MB),
//    of size 10MB filled with a FAT32 filesystem.
//
//     import diskfs "github.com/diskfs/go-diskfs"
//
//     diskSize := 10*1024*1024 // 10 MB
//
//     diskImg := "/tmp/disk.img"
//     disk := diskfs.Create(diskImg, size, diskfs.Raw)
//
//     table := &mbr.Table{
//       LogicalSectorSize:  512,
//       PhysicalSectorSize: 512,
//       Partitions: []*mbr.Partition{
//         {
//           Bootable:      false,
//           Type:          Linux,
//           Start:         2048,
//           Size:          20480,
//         },
//       },
//     }
//
//     fs, err := disk.CreateFilesystem(1, diskfs.TypeFat32)
//
// 3. Create a disk of size 20MB with a GPT partition table, a single partition beginning at block 2048 (1MB),
//    of size 10MB, and fill with the contents from the 10MB file "/root/contents.dat"
//
//     import diskfs "github.com/diskfs/go-diskfs"
//
//     diskSize := 10*1024*1024 // 10 MB
//
//     diskImg := "/tmp/disk.img"
//     disk := diskfs.Create(diskImg, size, diskfs.Raw)
//
//     table := &gpt.Table{
//       LogicalSectorSize:  512,
//       PhysicalSectorSize: 512,
//       Partitions: []*gpt.Partition{
//         {
//           LogicalSectorSize:  512,
//           PhysicalSectorSize: 512,
//           ProtectiveMBR:      true,
//         },
//       },
//     }
//
//     f, err := os.Open("/root/contents.dat")
//     written, err := disk.WritePartitionContents(1, f)
//
// 4. Create a disk of size 20MB with an MBR partition table, a single partition beginning at block 2048 (1MB),
//    of size 10MB filled with a FAT32 filesystem, and create some directories and files in that filesystem.
//
//     import diskfs "github.com/diskfs/go-diskfs"
//
//     diskSize := 10*1024*1024 // 10 MB
//
//     diskImg := "/tmp/disk.img"
//     disk := diskfs.Create(diskImg, size, diskfs.Raw)
//
//     table := &mbr.Table{
//       LogicalSectorSize:  512,
//       PhysicalSectorSize: 512,
//       Partitions: []*mbr.Partition{
//         {
//           Bootable:      false,
//           Type:          Linux,
//           Start:         2048,
//           Size:          20480,
//         },
//       },
//     }
//
//     fs, err := disk.CreateFilesystem(1, diskfs.TypeFat32)
//     err := fs.Mkdir("/FOO/BAR")
//     rw, err := fs.OpenFile("/FOO/BAR/AFILE.EXE", os.O_CREATE|os.O_RDRWR)
//     b := make([]byte, 1024, 1024)
//     rand.Read(b)
//     err := rw.Write(b)
//
package diskfs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/diskfs/go-diskfs/disk"
)

// when we use a disk image with a GPT, we cannot get the logical sector size from the disk via the kernel
//    so we use the default sector size of 512, per Rod Smith
const (
	defaultBlocksize, firstblock int = 512, 2048
	blksszGet                        = 0x1268
	blkbszGet                        = 0x80081270
)

// Format represents the format of the disk
type Format int

const (
	// Raw disk format for basic raw disk
	Raw Format = iota
)

func initDisk(f *os.File) (*disk.Disk, error) {
	var (
		diskType disk.Type
		size     int64
		lblksize = int64(defaultBlocksize)
		pblksize = int64(defaultBlocksize)
	)

	// get device information
	devInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not get info for device %s: %x", f.Name(), err)
	}
	if devInfo.Size() <= 0 {
		return nil, fmt.Errorf("could not get file size for device %s", f.Name())
	}
	mode := devInfo.Mode()
	switch {
	case mode.IsRegular():
		diskType = disk.File
		size = devInfo.Size()
	case mode&os.ModeDevice != 0:
		diskType = disk.Device
		// until we find a better way, like using ioctl()
		devSizePath := fmt.Sprintf("/sys/class/block/%s/size", path.Base(f.Name()))
		sizeBytes, err := ioutil.ReadFile(devSizePath)
		if err != nil {
			return nil, fmt.Errorf("could not get size of device %s from kernel", f.Name())
		}
		// convert to integer and multiply by 512
		sizeString := strings.TrimSuffix(string(sizeBytes), "\n")
		size, err = strconv.ParseInt(sizeString, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Invalid size passed: %s", sizeString)
		}
		lblksize, pblksize, err = getSectorSizes(f)
		if err != nil {
			return nil, fmt.Errorf("Unable to get block sizes for device %s: %v", f.Name(), err)
		}
		/* ioctl method
		var stat syscall.Statfs_t
		syscall.Statfs(f.Name(), &stat)
		size = stat.Bsize * stat.Blocks
		*/
	default:
		return nil, fmt.Errorf("device %s is neither a block device nor a regular file", f.Name())
	}

	// how many good blocks do we have?
	//var goodBlocks, orphanedBlocks int
	//goodBlocks = size / lblksize

	return &disk.Disk{File: f, Info: devInfo, Type: diskType, Size: size, LogicalBlocksize: lblksize, PhysicalBlocksize: pblksize}, nil
}

// Open a Disk from a path to a device
// Should pass a path to a block device e.g. /dev/sda or a path to a file /tmp/foo.img
// The provided device must exist at the time you call Open()
func Open(device string) (*disk.Disk, error) {
	if device == "" {
		return nil, errors.New("must pass device name")
	}
	if _, err := os.Stat(device); os.IsNotExist(err) {
		return nil, fmt.Errorf("provided device %s does not exist", device)
	}
	f, err := os.OpenFile(device, os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("Could not open device %s exclusively for writing", device)
	}
	// return our disk
	return initDisk(f)
}

// Create a Disk from a path to a device
// Should pass a path to a block device e.g. /dev/sda or a path to a file /tmp/foo.img
// The provided device must not exist at the time you call Create()
func Create(device string, size int64, format Format) (*disk.Disk, error) {
	if device == "" {
		return nil, errors.New("must pass device name")
	}
	if size <= 0 {
		return nil, errors.New("must pass valid device size to create")
	}
	f, err := os.OpenFile(device, os.O_RDWR|os.O_EXCL|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("Could not create device %s", device)
	}
	err = os.Truncate(device, size)
	if err != nil {
		return nil, fmt.Errorf("Could not expand device %s to size %d", device, size)
	}
	// return our disk
	return initDisk(f)
}

// to get the logical and physical sector sizes
func getSectorSizes(f *os.File) (int64, int64, error) {
	/*
		ioctl(fd, BLKBSZGET, &physicalsectsize);

	*/
	fd := f.Fd()
	logicalSectorSize, err := unix.IoctlGetInt(int(fd), blksszGet)
	if err != nil {
		return 0, 0, fmt.Errorf("Unable to get device logical sector size: %v", err)
	}
	physicalSectorSize, err := unix.IoctlGetInt(int(fd), blkbszGet)
	if err != nil {
		return 0, 0, fmt.Errorf("Unable to get device physical sector size: %v", err)
	}
	return int64(logicalSectorSize), int64(physicalSectorSize), nil
}
