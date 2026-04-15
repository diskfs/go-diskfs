package examples

import (
	"fmt"
	"log"
	"os"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
)

func CreateBootableIso(diskImg string) {
	if diskImg == "" {
		log.Fatal("must have a valid path for diskImg")
	}
	var diskSize int64 = 10 * 1024 * 1024 // 10 MB
	mydisk, err := diskfs.Create(diskImg, diskSize, diskfs.SectorSizeDefault)
	check(err)

	// the following line is required for an ISO, which may have logical block sizes
	// only of 2048, 4096, 8192
	mydisk.LogicalBlocksize = 2048
	fspec := disk.FilesystemSpec{Partition: 0, FSType: filesystem.TypeISO9660, VolumeLabel: "label"}
	fs, err := mydisk.CreateFilesystem(fspec)
	check(err)
	// write contents to the disk
	rw, err := fs.OpenFile("demo.txt", os.O_CREATE|os.O_RDWR)
	check(err)
	content := []byte("demo")
	_, err = rw.Write(content)
	check(err)
	iso, ok := fs.(*iso9660.FileSystem)
	if !ok {
		check(fmt.Errorf("not an iso9660 filesystem"))
	}

	// the below assumes that you have the boot files isolinux/isolinux.bin,
	// isolinux/ldlinux.c32, images/efiboot.img already loaded in the files to
	// be added to the iso.
	biosEntry := &iso9660.ElToritoEntry{
		Emulation: iso9660.NoEmulation,
		BootFile:  "isolinux/isolinux.bin",
		BootTable: true,
		Platform:  iso9660.BIOS,
	}
	biosEntry.SetLoadSize(4)

	options := iso9660.FinalizeOptions{
		VolumeIdentifier: "my-volume",
		ElTorito: &iso9660.ElTorito{
			BootCatalog: "isolinux/boot.cat",
			Entries: []*iso9660.ElToritoEntry{
				biosEntry,
				{
					Platform:  iso9660.EFI,
					Emulation: iso9660.NoEmulation,
					BootFile:  "images/efiboot.img",
				},
			},
		},
	}
	err = iso.Finalize(options)
	check(err)
}
