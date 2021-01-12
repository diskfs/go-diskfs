package qcow2

// mask64 create a uint64 mask that starts at bit start and goes for len bits.
// e.g. mask64(2, 5) returns 0b0011111000000000....
func mask64(start, len int) uint64 {
	var nMask uint64
	// first create a mask of the right length
	for i := 0; i < len; i++ {
		nMask |= (1 << (63 - uint(i)))
	}
	// then shift it to the right start bits
	nMask >>= uint(start)
	return nMask
}

// zeropad return a slice of bytes to pad an existing length such that it is a multiple of a given number
func zeropad(len, multiple int) []byte {
	remainder := len % multiple
	if remainder == multiple {
		return nil
	}
	pad := multiple - remainder
	return make([]byte, pad)
}
