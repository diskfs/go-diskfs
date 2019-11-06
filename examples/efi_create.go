// The following example will create a fully bootable EFI disk image. It assumes you have a bootable EFI file (any modern Linux kernel compiled with `CONFIG_EFI_STUB=y` will work) available.

package example

import (
	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/partion/gpt"
)

func main() {

	var (
		espSize          int = 100 * 1024 * 1024     // 100 MB
		diskSize         int = espSize + 4*1024*1024 // 104 MB
		blkSize          int = 512
		partitionStart   int = 2048
		partitionSectors int = espSize / blkSize
		partitionEnd     int = partitionSectors - partitionStart + 1
	)

	// create a disk image
	diskImg := "/tmp/disk.img"
	disk := diskfs.Create(diskImg, diskSize, diskfs.Raw)
	// create a partition table
	table := gpt.Table{
		Partitions: []*gpt.Partition{
			gpt.Partition{Start: partitionStart, End: partitionEnd, Type: partition.EFISystemPartition, Name: "EFI System"},
		},
	}
	// apply the partition table
	err = disk.Partition(table)

	/*
	 * create an ESP partition with some contents
	 */
	kernel, err := ioutil.ReadFile("/some/kernel/file")

	fs, err := disk.CreateFilesystem(0, diskfs.TypeFat32)

	// make our directories
	err = fs.Mkdir("/EFI/BOOT")
	rw, err := fs.OpenFile("/EFI/BOOT/BOOTX64.EFI", os.O_CREATE|os.O_RDRWR)

	err = rw.Write(kernel)
}
