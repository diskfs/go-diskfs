package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
)

func check(err error) {
	if err == nil {
		return
	}

	log.Fatal(err)
}

func FolderSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func CreateIsoFromFolder(srcFolder string, outputFileName string) {
	// We need to know the size of the folder before we can create a disk image
	// TODO: Are we able to create a disk image with a dynamic size?
	folderSize, err := FolderSize(srcFolder)
	check(err)

	// TODO: Explain why we need to set the logical block size and which values should be used
	var LogicalBlocksize diskfs.SectorSize = 2048

	// Create the disk image
	// TODO: Explain why we need to use Raw here
	mydisk, err := diskfs.Create(outputFileName, folderSize, diskfs.Raw, LogicalBlocksize)
	check(err)

	// Create the ISO filesystem on the disk image
	fspec := disk.FilesystemSpec{
		Partition:   0,
		FSType:      filesystem.TypeISO9660,
		VolumeLabel: "label",
	}
	fs, err := mydisk.CreateFilesystem(fspec)
	check(err)

	// Walk the source folder to copy all files and folders to the ISO filesystem
	err = filepath.Walk(srcFolder, func(path string, info os.FileInfo, err error) error {
		check(err)

		relPath, err := filepath.Rel(srcFolder, path)
		check(err)

		// If the current path is a folder, create the folder in the ISO filesystem
		if info.IsDir() {
			// Create the directory in the ISO file
			err = fs.Mkdir(relPath)
			check(err)
			return nil
		}

		// If the current path is a file, copy the file to the ISO filesystem
		if !info.IsDir() {
			// Open the file in the ISO file for writing
			rw, err := fs.OpenFile(relPath, os.O_CREATE|os.O_RDWR)
			check(err)

			// Open the source file for reading
			in, errorOpeningFile := os.Open(path)
			if errorOpeningFile != nil {
				return errorOpeningFile
			}
			defer in.Close()

			// Copy the contents of the source file to the ISO file
			_, err = io.Copy(rw, in)
			check(err)
		}

		return nil
	})
	check(err)

	iso, ok := fs.(*iso9660.FileSystem)
	if !ok {
		check(fmt.Errorf("not an iso9660 filesystem"))
	}
	err = iso.Finalize(iso9660.FinalizeOptions{})
	check(err)
}

func main() {
	CreateIsoFromFolder("my-folder", "my-image.iso")
}
