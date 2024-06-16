package md4

import (
	"testing"
)

// Test rotateLeft function
func TestRotateLeft(t *testing.T) {
	tests := []struct {
		x      uint32
		s      uint
		expect uint32
	}{
		{x: 0x12345678, s: 0, expect: 0x12345678},
		{x: 0x12345678, s: 4, expect: 0x23456781},
		{x: 0x12345678, s: 16, expect: 0x56781234},
		{x: 0x12345678, s: 32, expect: 0x12345678},
	}

	for _, tt := range tests {
		result := rotateLeft(tt.x, tt.s)
		if result != tt.expect {
			t.Errorf("rotateLeft(%#x, %d) = %#x; want %#x", tt.x, tt.s, result, tt.expect)
		}
	}
}

// Test f function
func TestF(t *testing.T) {
	tests := []struct {
		x, y, z uint32
		expect  uint32
	}{
		{x: 0xFFFFFFFF, y: 0xAAAAAAAA, z: 0x55555555, expect: 0xAAAAAAAA},
		{x: 0x0, y: 0xAAAAAAAA, z: 0x55555555, expect: 0x55555555},
		{x: 0x12345678, y: 0x9ABCDEF0, z: 0x0FEDCBA9, expect: 0x1ffddff1},
	}

	for _, tt := range tests {
		result := f(tt.x, tt.y, tt.z)
		if result != tt.expect {
			t.Errorf("f(%#x, %#x, %#x) = %#x; want %#x", tt.x, tt.y, tt.z, result, tt.expect)
		}
	}
}

// Test g function
func TestG(t *testing.T) {
	tests := []struct {
		x, y, z uint32
		expect  uint32
	}{
		{x: 0xFFFFFFFF, y: 0xAAAAAAAA, z: 0x55555555, expect: 0xffffffff},
		{x: 0x0, y: 0xAAAAAAAA, z: 0x55555555, expect: 0x0},
		{x: 0x12345678, y: 0x9ABCDEF0, z: 0x0FEDCBA9, expect: 0x1abcdef8},
	}

	for _, tt := range tests {
		result := g(tt.x, tt.y, tt.z)
		if result != tt.expect {
			t.Errorf("g(%#x, %#x, %#x) = %#x; want %#x", tt.x, tt.y, tt.z, result, tt.expect)
		}
	}
}

// Test h function
func TestH(t *testing.T) {
	tests := []struct {
		x, y, z uint32
		expect  uint32
	}{
		{x: 0xFFFFFFFF, y: 0xAAAAAAAA, z: 0x55555555, expect: 0x0},
		{x: 0x0, y: 0xAAAAAAAA, z: 0x55555555, expect: 0xFFFFFFFF},
		{x: 0x12345678, y: 0x9ABCDEF0, z: 0x0FEDCBA9, expect: 0x87654321},
	}

	for _, tt := range tests {
		result := h(tt.x, tt.y, tt.z)
		if result != tt.expect {
			t.Errorf("h(%#x, %#x, %#x) = %#x; want %#x", tt.x, tt.y, tt.z, result, tt.expect)
		}
	}
}

// Test round function
func TestRound(t *testing.T) {
	tests := []struct {
		name       string
		f          func(x, y, z uint32) uint32
		a, b, c, d uint32
		x          uint32
		s          uint
		expect     uint32
	}{
		{"f", f, 0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0x12345678, 3, 0x91a2b3b8},
		{"g", g, 0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0x12345678, 5, 0x468acee2},
		{"h", h, 0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0x12345678, 7, 0x5f4e3d70},
	}

	for _, tt := range tests {
		a, b, c, d := tt.a, tt.b, tt.c, tt.d
		result := round(tt.f, a, b, c, d, tt.x, tt.s)
		if result != tt.expect {
			t.Errorf("round(%s, %d) = %#x; want %#x", tt.name, tt.s, result, tt.expect)
		}
	}
}

func TestHalfMD4Transform(t *testing.T) {
	var buf = [4]uint32{0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476}
	tests := []struct {
		name   string
		in     [8]uint32
		expect uint32
	}{
		{
			name:   "Test Case 1",
			in:     [8]uint32{0, 1, 2, 3, 4, 5, 6, 7},
			expect: 0xF254F422,
		},
		{
			name:   "Test Case 2",
			in:     [8]uint32{0x12345678, 0x9ABCDEF0, 0x0FEDCBA9, 0x87654321, 0x11223344, 0xAABBCCDD, 0x55667788, 0x99AABBCC},
			expect: 0xA4405E22,
		},
		{
			name:   "Test Case 3",
			in:     [8]uint32{0x00000000, 0xFFFFFFFF, 0xAAAAAAAA, 0x55555555, 0x33333333, 0x66666666, 0x99999999, 0xCCCCCCCC},
			expect: 0x35B92DEF,
		},
		{
			name:   "Test Case 4 (Empty Input)",
			in:     [8]uint32{0, 0, 0, 0, 0, 0, 0, 0},
			expect: 0x5B0AA4BE,
		},
		{
			name:   "Test Case 5 (Random Input)",
			in:     [8]uint32{0x89ABCDEF, 0x01234567, 0xFEDCBA98, 0x76543210, 0xA1B2C3D4, 0x0BADC0DE, 0xDEADBEEF, 0xCAFEBABE},
			expect: 0x2748FDB6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HalfMD4Transform(buf, tt.in[:])
			if result != tt.expect {
				t.Errorf("halfMD4Transform(%#v, %#v) = %#x; want %#x", buf, tt.in, result, tt.expect)
			}
		})
	}
}
