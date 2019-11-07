// The following example will read a filesystem, assuming it to cover the entire image

package example

import (
	"fmt"
	"log"

	diskfs "github.com/diskfs/go-diskfs"
)

func main() {
	disk, err := diskfs.Open("./disk.raw")
	if err != nil {
		log.Panic(err)
	}
	fs, err := disk.GetFilesystem(0) // assuming it is the whole disk, so partition = 0
	if err != nil {
		log.Panic(err)
	}
	files, err := fs.ReadDir("/") // this should list everything
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(files)
}
