package ext4

import (
	"bytes"
	"fmt"
	"testing"
)

func TestStringToASCIIBytes(t *testing.T) {
	tests := []struct {
		s        string
		size     int
		expected []byte
		err      error
	}{
		// Test case 1: Empty string
		{"", 16, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nil},

		// Test case 2: Short string
		{"EXT4", 5, []byte{'E', 'X', 'T', '4', 0}, nil},

		// Test case 3: Long string
		{"EXT4 filesystem", 8, []byte{'E', 'X', 'T', '4', ' ', 'f', 'i', 'l'}, nil},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result, err := stringToASCIIBytes(tt.s, tt.size)
			if err != tt.err {
				t.Fatalf("stringToASCIIBytes(%q, %d) error = %v; want %v", tt.s, tt.size, err, tt.err)
			}
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("stringToASCIIBytes(%q, %d) = %v; want %v", tt.s, tt.size, result, tt.expected)
			}
		})
	}
}

func TestMinString(t *testing.T) {
	tests := []struct {
		b        []byte
		expected string
	}{
		// Test case 1: Empty byte slice
		{[]byte{}, ""},

		// Test case 2: Short byte slice
		{[]byte{'E', 'X', 'T', '4', 0}, "EXT4"},

		// Test case 3: Long byte slice
		{[]byte{'E', 'X', 'T', '4', ' ', 'f', 'i', 'l'}, "EXT4 fil"},

		{[]byte{'E', 'X', 'T', '4', ' ', 'f', 'i', 'l', 0}, "EXT4 fil"},

		{[]byte{'E', 'X', 'T', '4', ' ', 'f', 'i', 'l', 0, 0, 0, 0, 0}, "EXT4 fil"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := minString(tt.b)
			if result != tt.expected {
				t.Errorf("minString(%v) = %q; want %q", tt.b, result, tt.expected)
			}
		})
	}
}

// dumpByteSlice dump a byte slice in hex and optionally ASCII format.
// Optionally but position at the beginning of each row, like xxd.
// Optionally convert to ASCII at end of each row, like xxd.
// Can show positions at beginning of each row in hex, decimal or both.
// Can filter out all rows except those containing given positions in showOnlyBytes. If showOnlyBytes is nil, all rows are shown.
// If showOnlyBytes is not nil, even an empty slice, will only show those rows that contain the given positions.
func dumpByteSlice(b []byte, bytesPerRow int, showASCII, showPosHex, showPosDec bool, showOnlyBytes []int) (out string) {
	var ascii []byte
	// go through each byte.
	// At each position:
	// - if we are at the end of a row, print the ASCII representation of the row.
	// - if we are at the middle of a row, add an extra space
	// - if we are still in the byte slice, print the byte in hex with a space before it.
	// - if we are past the end of the row, print spaces.
	showOnlyMap := make(map[int]bool)
	for _, v := range showOnlyBytes {
		showOnlyMap[v] = true
	}
	// run by rows
	numRows := len(b) / bytesPerRow
	if len(b)%bytesPerRow != 0 {
		numRows++
	}
	for i := 0; i < numRows; i++ {
		firstByte := i * bytesPerRow
		lastByte := firstByte + bytesPerRow
		var row string
		// row header includes optional position numbers
		if showPosHex {
			row += fmt.Sprintf("%08x ", firstByte)
		}
		if showPosDec {
			row += fmt.Sprintf("%4d ", firstByte)
		}
		row += ": "
		for j := firstByte; j < lastByte; j++ {
			// every 8 bytes add extra spacing to make it easier to read
			if j%8 == 0 {
				row += " "
			}
			// regular byte, print in hex
			if j < len(b) {
				hex := fmt.Sprintf(" %02x", b[j])
				if showOnlyBytes != nil && showOnlyMap[j] {
					hex = "\033[1m\033[31m" + hex + "\033[0m"
				}
				row += hex
			} else {
				row += "   "
			}
			switch {
			case j >= len(b):
				// past end of byte slice, print spaces
				ascii = append(ascii, ' ')
			case b[j] < 32 || b[j] > 126:
				// unprintable characters, print a dot
				ascii = append(ascii, '.')
			default:
				// printable characters, print the character
				ascii = append(ascii, b[j])
			}
		}
		// end of row, print the ASCII representation and a newline
		if showASCII {
			row += fmt.Sprintf("  %s", string(ascii))
			ascii = ascii[:0]
		}
		row += "\n"

		// calculate if we should include this row
		var includeRow = true
		if showOnlyBytes != nil {
			includeRow = false
			for j := firstByte; j < lastByte; j++ {
				if showOnlyMap[j] {
					includeRow = true
					break
				}
			}
		}
		if includeRow {
			out += row
		}
	}
	return out
}

// diff
type diff struct {
	Offset int
	ByteA  byte
	ByteB  byte
}

// compareByteSlices compares two byte slices position by position. If the byte slices are identical, diffs is length 0,
// otherwise it contains the positions of the differences.
func compareByteSlices(a, b []byte) (diffs []diff) {
	maxSize := len(a)
	if len(b) > maxSize {
		maxSize = len(b)
	}
	for i := 0; i < maxSize; i++ {
		switch {
		case i >= len(a):
			diffs = append(diffs, diff{Offset: i, ByteA: 0, ByteB: b[i]})
		case i >= len(b):
			diffs = append(diffs, diff{Offset: i, ByteA: a[i], ByteB: 0})
		case a[i] != b[i]:
			diffs = append(diffs, diff{Offset: i, ByteA: a[i], ByteB: b[i]})
		}
	}
	return diffs
}

// dumpByteSlicesWithDiffs show two byte slices in hex and ASCII format, with differences highlighted.
//
//nolint:unparam // sure, bytesPerRow always is 32, but it could be something else
func dumpByteSlicesWithDiffs(a, b []byte, bytesPerRow int, showASCII, showPosHex, showPosDec bool) (different bool, out string) {
	diffs := compareByteSlices(a, b)
	// if there are no differences, just return an empty string
	if len(diffs) == 0 {
		return false, ""
	}

	showOnlyBytes := make([]int, len(diffs))
	for i, d := range diffs {
		showOnlyBytes[i] = d.Offset
	}
	out = dumpByteSlice(a, bytesPerRow, showASCII, showPosHex, showPosDec, showOnlyBytes)
	out += "\n"
	out += dumpByteSlice(b, bytesPerRow, showASCII, showPosHex, showPosDec, showOnlyBytes)
	return true, out
}
