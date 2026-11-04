package md

import "testing"

func TestBooleanFunctions(t *testing.T) {
	// Exhaust the truth tables at every bit position, including the high bit.
	for bit := 0; bit < 32; bit++ {
		for combination := 0; combination < 8; combination++ {
			x, y, z := uint32(combination>>2), uint32((combination>>1)&1), uint32(combination&1)
			selection := z
			if x == 1 {
				selection = y
			}
			var majority uint32
			if x+y+z >= 2 {
				majority = 1
			}
			for _, tc := range []struct {
				name string
				fn   func(uint32, uint32, uint32) uint32
				want uint32
			}{
				{"selection", f, selection},
				{"majority", g, majority},
				{"parity", h, (x + y + z) % 2},
			} {
				if got := tc.fn(x<<bit, y<<bit, z<<bit); got != tc.want<<bit {
					t.Errorf("%s bit %d combination %d = %#x, want %#x", tc.name, bit, combination, got, tc.want<<bit)
				}
			}
		}
	}
}

func TestHalfMD4Transform(t *testing.T) {
	// Numeric transform vectors calculated from the specification's schedules.
	tests := []struct {
		name          string
		state         [4]uint32
		input         [8]uint32
		want, chained [4]uint32
	}{
		{"zero", [4]uint32{}, [8]uint32{},
			[4]uint32{0x910d1f0b, 0xc544d760, 0xafb1bb7a, 0x37c10ba4},
			[4]uint32{0xbe953f0f, 0x45eaf424, 0xaee71267, 0x998012ef}},
		{"abc", [4]uint32{0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476},
			[8]uint32{0x03616263, 0x03030303, 0x03030303, 0x03030303, 0x03030303, 0x03030303, 0x03030303, 0x03030303},
			[4]uint32{0x3c253978, 0xd196a869, 0xc420eb28, 0xd1fefc5a},
			[4]uint32{0x420f8f54, 0x63d078e9, 0x41561917, 0xa19c9b13}},
		{"mixed", [4]uint32{0xffffffff, 0, 0x12345678, 0x87654321}, [8]uint32{0, 1, 2, 3, 4, 5, 6, 7},
			[4]uint32{0xe319969c, 0x851eba41, 0xa20cb81a, 0x489d0f2b},
			[4]uint32{0x0694187e, 0xbcf9c18e, 0xe37e0b87, 0x2b14ad84}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := tc.state
			if got := HalfMD4Transform(&state, tc.input); got != tc.want[1] || state != tc.want {
				t.Fatalf("return %#x, state %08x; want return %#x, state %08x", got, state, tc.want[1], tc.want)
			}
			if got := HalfMD4Transform(&state, tc.input); got != tc.chained[1] || state != tc.chained {
				t.Errorf("chained return %#x, state %08x; want return %#x, state %08x", got, state, tc.chained[1], tc.chained)
			}
		})
	}
}
