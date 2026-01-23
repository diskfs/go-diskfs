package bitmap

import "fmt"

// Bitmap is a structure holding a bitmap
type Bitmap struct {
	bits []byte
}

// Contiguous a position and count of contiguous bits, either free or set
type Contiguous struct {
	Position int
	Count    int
}

// FromBytes create a bitmap struct from bytes
func FromBytes(b []byte) *Bitmap {
	// just copy them over
	bits := make([]byte, len(b))
	copy(bits, b)
	bm := Bitmap{
		bits: bits,
	}

	return &bm
}

// NewBytes creates a new bitmap of size bytes; it is not in bits to force the caller to have
// a complete set
func NewBytes(nbytes int) *Bitmap {
	if nbytes < 0 {
		nbytes = 0
	}
	bm := Bitmap{
		bits: make([]byte, nbytes),
	}
	return &bm
}

// NewBits creates a new bitmap that can address nBits entries.
// All bits are initially 0 (free).
func NewBits(nBits int) *Bitmap {
	if nBits < 0 {
		nBits = 0
	}
	nBytes := (nBits + 7) / 8
	return NewBytes(nBytes)
}

// ToBytes returns raw bytes underlying the bitmap
func (bm *Bitmap) ToBytes() []byte {
	b := make([]byte, len(bm.bits))
	copy(b, bm.bits)

	return b
}

// FromBytes overwrite the existing map with the contents of the bytes.
// It is the equivalent of BitmapFromBytes, but uses an existing Bitmap.
func (bm *Bitmap) FromBytes(b []byte) {
	bm.bits = make([]byte, len(b))
	copy(bm.bits, b)
}

// IsSet check if a specific bit location is set
func (bm *Bitmap) IsSet(location int) (bool, error) {
	if location < 0 {
		return false, fmt.Errorf("location %d is negative", location)
	}
	byteNumber, bitNumber := findBitForIndex(location)
	if byteNumber > len(bm.bits) {
		return false, fmt.Errorf("location %d is not in %d size bitmap", location, len(bm.bits)*8)
	}
	mask := byte(0x1) << bitNumber
	return bm.bits[byteNumber]&mask == mask, nil
}

// Clear a specific bit location
func (bm *Bitmap) Clear(location int) error {
	if location < 0 {
		return fmt.Errorf("location %d is negative", location)
	}
	byteNumber, bitNumber := findBitForIndex(location)
	if byteNumber >= len(bm.bits) {
		return fmt.Errorf("location %d is not in %d size bitmap", location, len(bm.bits)*8)
	}
	mask := byte(0x1) << bitNumber
	mask = ^mask
	bm.bits[byteNumber] &= mask
	return nil
}

// Set a specific bit location
func (bm *Bitmap) Set(location int) error {
	if location < 0 {
		return fmt.Errorf("location %d is negative", location)
	}
	byteNumber, bitNumber := findBitForIndex(location)
	if byteNumber >= len(bm.bits) {
		return fmt.Errorf("location %d is not in %d size bitmap", location, len(bm.bits)*8)
	}
	mask := byte(0x1) << bitNumber
	bm.bits[byteNumber] |= mask
	return nil
}

// FirstFree returns the first free bit in the bitmap
// Begins at start, so if you want to find the first free bit, pass start=1.
// Returns -1 if none found.
func (bm *Bitmap) FirstFree(start int) int {
	if start < 0 {
		start = 0
	}
	totalBits := len(bm.bits) * 8
	if start >= totalBits {
		return -1
	}
	// Start scanning at the relevant byte, but ensure we don't return a bit < start.
	byteIdx := start / 8
	bitStart := uint8(start % 8)

	// First partial byte
	b := bm.bits[byteIdx]
	if b != 0xff {
		for j := bitStart; j < 8; j++ {
			if (b & (byte(1) << j)) == 0 {
				return byteIdx*8 + int(j)
			}
		}
	}

	// Remaining full bytes
	for i := byteIdx + 1; i < len(bm.bits); i++ {
		b = bm.bits[i]
		if b == 0xff {
			continue
		}
		for j := uint8(0); j < 8; j++ {
			if (b & (byte(1) << j)) == 0 {
				return i*8 + int(j)
			}
		}
	}

	return -1
}

// FirstSet returns location of first set bit in the bitmap
func (bm *Bitmap) FirstSet() int {
	for i, b := range bm.bits {
		// if all free, continue to next
		if b == 0x00 {
			continue
		}
		// not all free, so find first bit set to 1
		for j := uint8(0); j < 8; j++ {
			if (b & (byte(1) << j)) != 0 {
				return i*8 + int(j)
			}
		}
	}
	return -1
}

// FreeList returns a slicelist of contiguous free locations by location.
// It is sorted by location. If you want to sort it by size, uses sort.Slice
// for example, if the bitmap is 10010010 00100000 10000010, it will return
//
//		 1: 2, // 2 free bits at position 1
//		 4: 2, // 2 free bits at position 4
//		 8: 3, // 3 free bits at position 8
//		11: 5  // 5 free bits at position 11
//	    17: 5  // 5 free bits at position 17
//		23: 1, // 1 free bit at position 23
//
// if you want it in reverse order, just reverse the slice.
func (bm *Bitmap) FreeList() []Contiguous {
	var list []Contiguous
	var location = -1
	var count = 0
	for i, b := range bm.bits {
		for j := uint8(0); j < 8; j++ {
			mask := byte(0x1) << j
			switch {
			case b&mask != mask:
				if location == -1 {
					location = 8*i + int(j)
				}
				count++
			case location != -1:
				list = append(list, Contiguous{location, count})
				location = -1
				count = 0
			}
		}
	}
	if location != -1 {
		list = append(list, Contiguous{location, count})
	}
	return list
}

func findBitForIndex(index int) (byteNumber int, bitNumber uint8) {
	return index / 8, uint8(index % 8)
}
