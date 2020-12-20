package examples

import (
	"fmt"
	"log"
	"os"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
)

func CreateSquashfs(diskImg string) {
	if diskImg == "" {
		log.Fatal("must have a valid path for diskImg")
	}
	var diskSize int64 = 10 * 1024 * 1024 // 10 MB
	mydisk, err := diskfs.Create(diskImg, diskSize, diskfs.Raw)
	check(err)

	fspec := disk.FilesystemSpec{Partition: 0, FSType: filesystem.TypeSquashfs, VolumeLabel: "label"}
	fs, err := mydisk.CreateFilesystem(fspec)
	check(err)
	rw, err := fs.OpenFile("demo.txt", os.O_CREATE|os.O_RDWR)
	content := []byte("demo")
	_, err = rw.Write(content)
	check(err)
	sqs, ok := fs.(*squashfs.FileSystem)
	if !ok {
		check(fmt.Errorf("not a squashfs filesystem"))
	}
	err = sqs.Finalize(squashfs.FinalizeOptions{})
	check(err)
}
