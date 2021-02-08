package examples

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
)

func PrintIsoInfo(isoPath string) {
	disk, err := diskfs.Open(isoPath)
	if err != nil {
		log.Fatal(err)
	}
	fs, err := disk.GetFilesystem(0)
	if err != nil {
		log.Fatal(err)
	}

	err = fileInfoFor("/", fs)
	if err != nil {
		log.Fatalf("Failed to get file info: %s\n", err)
	}
}

func fileInfoFor(path string, fs filesystem.FileSystem) error {
	files, err := fs.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {
		fullPath := filepath.Join(path, file.Name())
		if file.IsDir() {
			err = fileInfoFor(fullPath, fs)
			if err != nil {
				return err
			}
			continue
		}
		isoFile, err := fs.OpenFile(fullPath, os.O_RDONLY)
		if err != nil {
			fmt.Printf("Failed to open file %s: %v\n", fullPath, err)
			continue
		}

		myFile := isoFile.(*iso9660.File)
		fmt.Printf("%s\n Size: %d\n Location: %d\n\n", fullPath, file.Size(), myFile.Location())
	}
	return nil
}
