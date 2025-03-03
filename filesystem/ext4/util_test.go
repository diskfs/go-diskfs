package ext4

import (
	"bytes"
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
