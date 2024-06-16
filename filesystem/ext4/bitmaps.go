package ext4

import "fmt"

// bitmap is a structure holding a bitmap
type bitmap struct {
	bits []byte
}

// bitmapFromBytes create a bitmap struct from bytes
func bitmapFromBytes(b []byte) *bitmap {
	// just copy them over
	bits := make([]byte, len(b))
	copy(bits, b)
	bm := bitmap{
		bits: bits,
	}

	return &bm
}

// toBytes returns raw bytes ready to be written to disk
func (bm *bitmap) toBytes() []byte {
	b := make([]byte, len(bm.bits))
	copy(b, bm.bits)

	return b
}

func (bm *bitmap) checkFree(location int) (bool, error) {
	byteNumber, bitNumber := findBitForIndex(location)
	if byteNumber > len(bm.bits) {
		return false, fmt.Errorf("location %d is not in %d size bitmap", location, len(bm.bits)*8)
	}
	mask := byte(0x1) << bitNumber
	return bm.bits[byteNumber]&mask == mask, nil
}

func (bm *bitmap) free(location int) error {
	byteNumber, bitNumber := findBitForIndex(location)
	if byteNumber > len(bm.bits) {
		return fmt.Errorf("location %d is not in %d size bitmap", location, len(bm.bits)*8)
	}
	mask := byte(0x1) << bitNumber
	mask = ^mask
	bm.bits[byteNumber] &= mask
	return nil
}

func (bm *bitmap) use(location int) error {
	byteNumber, bitNumber := findBitForIndex(location)
	if byteNumber > len(bm.bits) {
		return fmt.Errorf("location %d is not in %d size bitmap", location, len(bm.bits)*8)
	}
	mask := byte(0x1) << bitNumber
	bm.bits[byteNumber] |= mask
	return nil
}

func (bm *bitmap) findFirstFree() int {
	var location = -1
	for i, b := range bm.bits {
		// if all used, continue to next
		if b&0xff == 0xff {
			continue
		}
		// not all used, so find first bit set to 0
		for j := uint8(0); j < 8; j++ {
			mask := byte(0x1) << j
			if b&mask != mask {
				location = 8*i + (8 - int(j))
				break
			}
		}
		break
	}
	return location
}

//nolint:revive // params are unused as of yet, but will be used in the future
func (bm *bitmap) findFirstUsed() int {
	var location int = -1
	for i, b := range bm.bits {
		// if all free, continue to next
		if b == 0x00 {
			continue
		}
		// not all free, so find first bit set to 1
		for j := uint8(0); j < 8; j++ {
			mask := byte(0x1) << j
			mask = ^mask
			if b|mask != mask {
				location = 8*i + (8 - int(j))
				break
			}
		}
		break
	}
	return location
}

func findBitForIndex(index int) (byteNumber int, bitNumber uint8) {
	return index / 8, uint8(index % 8)
}
