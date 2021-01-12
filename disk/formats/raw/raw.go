// raw package represents a raw disk. Everything pretty much is pass-through.
package raw

import (
	"os"

	"github.com/diskfs/go-diskfs/disk/formats"
)

// Raw a raw disk
type Raw struct {
	file *os.File
}

func NewRaw(file *os.File, create bool, size int64) (*Raw, error) {
	if create && size > 0 {
		if err := os.Truncate(file.Name(), size); err != nil {
			return nil, err
		}
	}
	return &Raw{file}, nil
}

func (r Raw) Format() formats.Format {
	return formats.Raw
}
func (r Raw) File() *os.File {
	return r.file
}

func (r Raw) ReadAt(b []byte, offset int64) (int, error) {
	return r.file.ReadAt(b, offset)
}

func (r Raw) WriteAt(b []byte, offset int64) (int, error) {
	return r.file.WriteAt(b, offset)
}
