package diskfs_test

import (
	"crypto/rand"
	"log"
	"os"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/partition/gpt"
	"github.com/diskfs/go-diskfs/partition/mbr"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func unused(_ ...any) {
}

// Create a disk image of size 10MB with a FAT32 filesystem spanning the entire disk.
func ExampleCreate_fat32() {
	var size int64 = 10 * 1024 * 1024 // 10 MB

	diskImg := "/tmp/disk.img"
	defer os.Remove(diskImg)
	theDisk, _ := diskfs.Create(diskImg, size, diskfs.Raw, diskfs.SectorSizeDefault)

	fs, err := theDisk.CreateFilesystem(disk.FilesystemSpec{
		Partition: 0,
		FSType:    filesystem.TypeFat32,
	})
	check(err)
	unused(fs)
}

// Create a disk of size 20MB with an MBR partition table, a single partition beginning at block 2048 (1MB),
// of size 10MB filled with a FAT32 filesystem.
func ExampleCreate_mbr() {
	var size int64 = 20 * 1024 * 1024 // 20 MB

	diskImg := "/tmp/disk.img"
	defer os.Remove(diskImg)
	theDisk, _ := diskfs.Create(diskImg, size, diskfs.Raw, diskfs.SectorSizeDefault)

	table := &mbr.Table{
		LogicalSectorSize:  512,
		PhysicalSectorSize: 512,
		Partitions: []*mbr.Partition{
			{
				Bootable: false,
				Type:     mbr.Linux,
				Start:    2048,
				Size:     20480,
			},
		},
	}

	check(theDisk.Partition(table))

	fs, err := theDisk.CreateFilesystem(disk.FilesystemSpec{
		Partition: 1,
		FSType:    filesystem.TypeFat32,
	})
	check(err)
	unused(fs)
}

// Create a disk of size 20MB with a GPT partition table, a single partition beginning at block 2048 (1MB), of size 10MB, and fill with the contents from the 10MB file "/root/contents.dat"
func ExampleCreate_gpt() {
	var size int64 = 20 * 1024 * 1024 // 20 MB

	diskImg := "/tmp/disk.img"
	defer os.Remove(diskImg)
	theDisk, _ := diskfs.Create(diskImg, size, diskfs.Raw, diskfs.SectorSizeDefault)

	table := &gpt.Table{
		LogicalSectorSize:  512,
		PhysicalSectorSize: 512,
		ProtectiveMBR:      true,
		Partitions: []*gpt.Partition{
			{
				Start: 1 * 1024 * 1024 / 512,
				Size:  10 * 1024 * 1024,
			},
		},
	}

	check(theDisk.Partition(table))

	f, err := os.Open("/root/contents.dat")
	check(err)

	written, err := theDisk.WritePartitionContents(1, f)
	check(err)
	unused(written)
}

// Create a disk of size 20MB with an MBR partition table, a single partition beginning at block 2048 (1MB),
// of size 10MB filled with a FAT32 filesystem, and create some directories and files in that filesystem.
func ExampleCreate_fat32WithDirsAndFiles() {
	var size int64 = 20 * 1024 * 1024 // 20 MB

	diskImg := "/tmp/disk.img"
	defer os.Remove(diskImg)
	theDisk, _ := diskfs.Create(diskImg, size, diskfs.Raw, diskfs.SectorSizeDefault)

	table := &mbr.Table{
		LogicalSectorSize:  512,
		PhysicalSectorSize: 512,
		Partitions: []*mbr.Partition{
			{
				Bootable: false,
				Type:     mbr.Linux,
				Start:    2048,
				Size:     20480,
			},
		},
	}

	check(theDisk.Partition(table))

	fs, err := theDisk.CreateFilesystem(disk.FilesystemSpec{
		Partition: 1,
		FSType:    filesystem.TypeFat32,
	})
	check(err)

	err = fs.Mkdir("/FOO/BAR")
	check(err)

	rw, err := fs.OpenFile("/FOO/BAR/AFILE.EXE", os.O_CREATE|os.O_RDWR)
	check(err)
	b := make([]byte, 1024)

	_, err = rand.Read(b)
	check(err)

	written, err := rw.Write(b)
	check(err)
	unused(written)
}
