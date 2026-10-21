package fat12

import "encoding/binary"

// EntryFormat is how one width of FAT stores its entries. fat12, fat16 and
// fat32 each describe their own; the lazy table and the writing of changed
// entries work from it.
type EntryFormat struct {
	// Bits is the width of one entry: 12, 16 or 32.
	Bits int
	// FATID returns the FAT ID, given the first 8 bytes of the table.
	FATID func(head []byte) uint32
	// EOCMarker returns the end-of-chain value to write, given the first 8
	// bytes of the table.
	EOCMarker func(head []byte) uint32
	// IsEOC reports whether val ends a cluster chain.
	IsEOC func(val uint32) bool
}

// FATRun is consecutive bytes of one copy of the FAT, Offset bytes from its start.
type FATRun struct {
	Offset int64
	Bytes  []byte
}

// Entries returns how many entries a table of size bytes holds.
func (f EntryFormat) Entries(size uint32) uint32 {
	return size * 8 / uint32(f.Bits)
}

// group returns how many entries share bytes, and how many bytes they take:
// two 12-bit entries share three bytes, wider entries stand alone.
func (f EntryFormat) group() (entries, size uint32) {
	if f.Bits == 12 {
		return 2, 3
	}
	return 1, uint32(f.Bits / 8)
}

// widen returns first and last widened to whole groups.
func (f EntryFormat) widen(first, last uint32) (from, to uint32) {
	entries, _ := f.group()
	return first / entries * entries, last/entries*entries + entries - 1
}

// span returns where the bytes holding entries first to last start, and how
// many there are. first and last must bound whole groups.
func (f EntryFormat) span(first, last uint32) (offset int64, length int) {
	entries, size := f.group()
	return int64(first/entries) * int64(size), int((last - first + 1) / entries * size)
}

// get decodes entry n from b, which holds whole groups from entry first on.
func (f EntryFormat) get(b []byte, first, n uint32) uint32 {
	i := n - first
	switch f.Bits {
	case 12:
		return fat12ReadEntry(b, i)
	case 16:
		return uint32(binary.LittleEndian.Uint16(b[i*2:]))
	default:
		return binary.LittleEndian.Uint32(b[i*4:])
	}
}

// put encodes val as entry n into b, which holds whole groups from entry first on.
func (f EntryFormat) put(b []byte, first, n, val uint32) {
	i := n - first
	switch f.Bits {
	case 12:
		fat12WriteEntry(b, i, val)
	case 16:
		binary.LittleEndian.PutUint16(b[i*2:], uint16(val))
	default:
		binary.LittleEndian.PutUint32(b[i*4:], val)
	}
}

// EncodeRuns encodes the changed entries, given in ascending order, as runs of
// bytes of a table of size bytes, one run per stretch of consecutive groups.
// value is asked for every entry in those groups, changed or not.
func (f EntryFormat) EncodeRuns(size uint32, changed []uint32, value func(uint32) (uint32, error)) ([]FATRun, error) {
	entries, _ := f.group()
	var runs []FATRun
	for i := 0; i < len(changed); {
		j := i + 1
		for j < len(changed) && changed[j]/entries <= changed[j-1]/entries+1 {
			j++
		}
		run, err := f.encodeRun(size, changed[i], changed[j-1], value)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
		i = j
	}
	return runs, nil
}

// encodeRun encodes every entry in the groups holding entries first to last,
// cut off where the table ends.
func (f EntryFormat) encodeRun(size, first, last uint32, value func(uint32) (uint32, error)) (FATRun, error) {
	first, last = f.widen(first, last)
	offset, length := f.span(first, last)
	b := make([]byte, length)
	for n := first; n <= last; n++ {
		val, err := value(n)
		if err != nil {
			return FATRun{}, err
		}
		f.put(b, first, n, val)
	}
	return FATRun{Offset: offset, Bytes: b[:min(int64(length), int64(size)-offset)]}, nil
}
