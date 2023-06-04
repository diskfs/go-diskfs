package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat32"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
)

func main() {
	filename := flag.String("filename", "", "File to serve")
	addr := flag.String("addr", ":8100", "address & port to server on")
	fsType := flag.String("type", "iso9660", "Filesystem type (iso9660, fat32, squashfs)")
	flag.Parse()

	f, err := os.Open(*filename)
	if err != nil {
		log.Fatalf("Cannot open %q: %s", *filename, err)
	}
	defer f.Close()
	var fs filesystem.FileSystem
	switch *fsType {
	case "iso9660":
		fs, err = iso9660.Read(f, 0, 0, 0)
	case "fat32":
		fs, err = fat32.Read(f, 0, 0, 0)
	case "squashfs":
		fs, err = squashfs.Read(f, 0, 0, 0)
	default:
		log.Fatalf("Unknown filesystem type %q", *fsType)
	}
	if err != nil {
		log.Fatalf("Cannot open %s image in %q: %s", *fsType, *filename, err)
	}

	http.Handle("/", http.FileServer(http.FS(filesystem.FS(fs))))

	log.Printf("Serving %q on HTTP port: %s\n", *filename, *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))

}
