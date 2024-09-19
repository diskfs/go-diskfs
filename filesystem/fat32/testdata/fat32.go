package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
)

func main() {
	filename := "test_file.img"
	r := rand.New(rand.NewSource(37))
	os.Remove(filename)
	fs := mkfs(filename)
	mkfile(fs, "/testfile")
	mkdir(fs, "/A")
	mkdir(fs, "/b")
	for i := 0; i < 100; i++ {
		inc := strconv.Itoa(i)
		mkdir(fs, "/b/sub"+inc)
		mkdir(fs, "/b/sub"+inc+"/blob/")
		mkfile(fs, "/b/sub"+inc+"/blob/testfile1")
		mkRandFile(fs, "/b/sub"+inc+"/blob/randFileSize", r.Intn(73))
		mkSmallFile(fs, "/b/sub"+inc+"/blob/testfile3")
	}
	mkGigFile(fs, "/b/sub49/blob/testfile4")
	mkGigFile(fs, "/b/sub50/blob/testfile4")
	mkSmallFile(fs, "/b/sub55/blob/testfile4")
	mkGigFile(fs, "/b/sub55/blob/testfile5")
	entries, err := fs.ReadDir("/b/sub50/blob")
	if err != nil {
		panic(err)
	}
	fmt.Println("/b/sub50/blob/:\n\n", entries)
	entries, err = fs.ReadDir("/b/sub55/blob")
	if err != nil {
		panic(err)
	}
	fmt.Println("/b/sub55/blob/:\n\n", entries)
}
func mkfs(name string) filesystem.FileSystem {
	size := int64(6 * 1024 * 1024 * 1024)
	d, err := diskfs.Create(name, size, diskfs.Raw, diskfs.SectorSizeDefault)
	if err != nil {
		fmt.Printf("error creating disk: %v", err)
		os.Exit(1)
	}

	spec := disk.FilesystemSpec{
		Partition: 0,
		FSType:    filesystem.TypeFat32,
	}

	fs, err := d.CreateFilesystem(spec)
	if err != nil {
		panic(err)
	}
	return fs
}

func mkfile(fs filesystem.FileSystem, name string) {
	rw, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
	if err != nil {
		panic(err)
	}

	_, err = rw.Write([]byte("hello World"))
	if err != nil {
		panic(err)
	}
}
func mkRandFile(fs filesystem.FileSystem, name string, rSize int) {
	rw, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
	if err != nil {
		panic(err)
	}

	size := rSize * 1024 * 1024
	smallFile := make([]byte, size, size)
	_, err = rw.Write(smallFile)
	if err != nil {
		panic(err)
	}
}
func mkSmallFile(fs filesystem.FileSystem, name string) {
	rw, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
	if err != nil {
		panic(err)
	}

	size := 5 * 1024 * 1024
	smallFile := make([]byte, size, size)
	_, err = rw.Write(smallFile)
	if err != nil {
		panic(err)
	}
}
func mkMedFile(fs filesystem.FileSystem, name string) {
	rw, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
	if err != nil {
		panic(err)
	}

	size := 50 * 1024 * 1024
	medFile := make([]byte, size, size)
	_, err = rw.Write(medFile)
	if err != nil {
		panic(err)
	}
}

func mkBigFile(fs filesystem.FileSystem, name string) {
	rw, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
	if err != nil {
		panic(err)
	}

	size := 550 * 1024 * 1024
	bigFile := make([]byte, size, size)
	_, err = rw.Write(bigFile)
	if err != nil {
		panic(err)
	}
}

func mkGigFile(fs filesystem.FileSystem, name string) {
	rw, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
	if err != nil {
		panic(err)
	}

	size := 1024 * 1024 * 1024
	gigFile := make([]byte, size, size)
	_, err = rw.Write(gigFile)
	if err != nil {
		panic(err)
	}
}

func mkdir(fs filesystem.FileSystem, name string) {
	err := fs.Mkdir(name)
	if err != nil {
		panic(err)
	}
}
