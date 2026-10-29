package erofs

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

func TestDeviceIDPreservesEncoding(t *testing.T) {
	for _, extended := range []bool{false, true} {
		for _, mode := range []uint16{linuxModeChar, linuxModeBlock} {
			// Include the colliding pair from the review and both maximum fields.
			for _, encoded := range []uint32{0x100, 0x100000, 0x10000, math.MaxUint32} {
				t.Run(fmt.Sprintf("extended=%t/mode=%x/device=%x", extended, mode, encoded), func(t *testing.T) {
					image := buildImage(false)
					inodeBytes := image[4128:]
					if extended {
						binary.LittleEndian.PutUint16(inodeBytes, 1)
					}
					binary.LittleEndian.PutUint16(inodeBytes[4:], mode|0o600)
					binary.LittleEndian.PutUint32(inodeBytes[16:], encoded)
					filesystem, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
					if err != nil {
						t.Fatal(err)
					}
					info, err := filesystem.Stat("hello")
					if err != nil {
						t.Fatal(err)
					}
					stat := info.Sys().(*StatT)
					if stat.DeviceID != encoded {
						t.Fatalf("DeviceID = %#x, want %#x", stat.DeviceID, encoded)
					}
				})
			}
		}
	}
}
