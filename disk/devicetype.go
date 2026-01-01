package disk

import (
	"fmt"
	iofs "io/fs"
	"os"
)

type DeviceType int

const (
	DeviceTypeUnknown DeviceType = iota
	DeviceTypeFile
	DeviceTypeBlockDevice
)

func DetermineDeviceType(f iofs.File) (DeviceType, error) {
	info, err := f.Stat()
	if err != nil {
		return DeviceTypeUnknown, fmt.Errorf("could not stat file: %v", err)
	}
	mode := info.Mode()
	var dt DeviceType
	switch {
	case mode.IsRegular():
		dt = DeviceTypeFile
	case mode&os.ModeDevice != 0:
		dt = DeviceTypeBlockDevice
	default:
		return DeviceTypeUnknown, fmt.Errorf("device %s is neither a block device nor a regular file", info.Name())
	}
	return dt, nil
}
