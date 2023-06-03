package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/diskfs/go-diskfs/converter"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
)

func main() {
	filename := flag.String("filename", "", "File to serve")
	addr := flag.String("addr", ":8100", "address & port to server on")
	flag.Parse()

	f, err := os.Open(*filename)
	if err != nil {
		log.Fatalf("Cannot open %q: %s", *filename, err)
	}
	defer f.Close()
	fs, err := iso9660.Read(f, 0, 0, 0)
	if err != nil {
		log.Fatalf("Cannot open iso9660 image in %q: %s", *filename, err)
	}

	c := converter.FS(fs)
	entries, err := c.ReadDir("/")
	if err != nil {
		log.Fatalf("/: %s", err)
	}
	for _, e := range entries {
		log.Printf("%s", e.Name())
	}

	http.Handle("/", http.FileServer(converter.HTTPFS(fs)))

	log.Printf("Serving %q on HTTP port: %s\n", *filename, *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))

}
