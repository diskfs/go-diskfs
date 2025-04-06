// The following example will create a fully bootable EFI disk image. It assumes you have a bootable EFI file (any modern Linux kernel compiled with `CONFIG_EFI_STUB=y` will work) available.

package examples

import (
	"fmt"
	"log"
	"os"

	diskfs "github.com/diskfs/go-diskfs"
	diskpkg "github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/partition/gpt"
)

func CreateEfi(diskImg string) {
	var (
		espSize          int64 = 100 * 1024 * 1024 // 100 MB
		blkSize          int64 = 512
		partitionStart   int64 = 2048
		diskSize               = espSize + 4*1024*1024 // 104 MB
		partitionSectors       = espSize / blkSize
		partitionEnd           = partitionSectors - partitionStart + 1
	)

	// create a disk image
	disk, err := diskfs.Create(diskImg, diskSize, diskfs.SectorSizeDefault)
	if err != nil {
		log.Panic(err)
	}
	// create a partition table
	table := &gpt.Table{
		Partitions: []*gpt.Partition{
			{Start: uint64(partitionStart), End: uint64(partitionEnd), Type: gpt.EFISystemPartition, Name: "EFI System"},
		},
	}
	// apply the partition table
	err = disk.Partition(table)
	if err != nil {
		log.Panic(err)
	}

	/*
	 * create an ESP partition with some contents
	 */
	kernel, err := os.ReadFile("/some/kernel/file")
	if err != nil {
		log.Panic(err)
	}

	spec := diskpkg.FilesystemSpec{Partition: 1, FSType: filesystem.TypeFat32}
	fs, err := disk.CreateFilesystem(spec)
	if err != nil {
		log.Panic(err)
	}

	// make our directories
	if err = fs.Mkdir("/EFI/BOOT"); err != nil {
		log.Panic(err)
	}
	rw, err := fs.OpenFile("/EFI/BOOT/BOOTX64.EFI", os.O_CREATE|os.O_RDWR)
	if err != nil {
		log.Panic(err)
	}

	n, err := rw.Write(kernel)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("wrote %d bytes\n", n)
}
