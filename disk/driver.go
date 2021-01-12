package disk

import (
	"errors"
	"fmt"
	"os"

	"github.com/diskfs/go-diskfs/disk/formats"
	"github.com/diskfs/go-diskfs/disk/formats/qcow2"
	"github.com/diskfs/go-diskfs/disk/formats/raw"
	"github.com/diskfs/go-diskfs/util"
)

// Driver driver to a particular disk format
type Driver interface {
	Format() formats.Format
	File() *os.File
	util.File
}

// GetDriver given a format, get a driver for the given format. If the format is
// formats.Unknown, then try to determine it.
func GetDriver(f *os.File, create bool, size int64, format formats.Format) (Driver, error) {
	if create && format == formats.Unknown {
		return nil, errors.New("cannot create a new disk image with an unknown format")
	}
	switch format {
	case formats.Unknown:
		// need to determine the format
		if driver, err := qcow2.NewQcow2(f, create, size); err == nil {
			return driver, nil
		}
		if driver, err := raw.NewRaw(f, create, size); err == nil {
			return driver, nil
		}
	case formats.Raw:
		return raw.NewRaw(f, create, size)
	case formats.Qcow2:
		return qcow2.NewQcow2(f, create, size)
	}
	return nil, fmt.Errorf("unknown disk format: %v", format)
}
