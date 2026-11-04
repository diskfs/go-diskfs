package ext4

import (
	"bytes"
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestExt4fsDirhashConformance(t *testing.T) {
	seed := &[4]uint32{0x11111111, 0x22222222, 0x33333333, 0x44444444}
	tests := []struct {
		version      hashVersion
		name         string
		seed         *[4]uint32
		major, minor uint32
	}{
		{0, "abc", nil, 0x75afd992, 0},
		{3, "abc", nil, 0x75afd992, 0},
		{1, "abc", nil, 0xd196a868, 0xc420eb28},
		{4, "abc", nil, 0xd196a868, 0xc420eb28},
		{2, "abc", nil, 0xb1435ec4, 0x3f7eaa0e},
		{5, "abc", nil, 0xb1435ec4, 0x3f7eaa0e},
		{0, "", nil, 0x2547fc5a, 0},
		{3, "", nil, 0x2547fc5a, 0},
		{1, "", nil, 0xefcdab88, 0x98badcfe},
		{4, "", nil, 0xefcdab88, 0x98badcfe},
		{2, "", nil, 0x67452300, 0xefcdab89},
		{5, "", nil, 0x67452300, 0xefcdab89},
		{1, "1234567890123456789012345678901", nil, 0xc4db1f78, 0xea23921b},
		{1, "12345678901234567890123456789012", nil, 0xfa6cc63e, 0x2f77bd1c},
		{1, "123456789012345678901234567890123", nil, 0xdc0c2dec, 0x5ca23365},
		{4, "1234567890123456789012345678901", nil, 0xc4db1f78, 0xea23921b},
		{4, "12345678901234567890123456789012", nil, 0xfa6cc63e, 0x2f77bd1c},
		{4, "123456789012345678901234567890123", nil, 0xdc0c2dec, 0x5ca23365},
		{2, "123456789abcdef", nil, 0xa562903a, 0x6174a00f},
		{2, "1234567890abcdef", nil, 0x8449f258, 0x49a16d46},
		{2, "123456789abcdefgh", nil, 0xf32ec10c, 0x58ceae61},
		{1, "same-name", seed, 0x8aebf604, 0x66ce48fe},
		{1, "same-name", &[4]uint32{0, 1, 0, 0xffffffff}, 0xb7b44c20, 0x3cb6a3d3},
		{2, "same-name", &[4]uint32{0, 1, 0, 0xffffffff}, 0xdcbbc302, 0x76d1a477},
		{1, "\x80\x81\x82\x83\x84", nil, 0x8bab0498, 0xc326632d},
		{4, "\x80\x81\x82\x83\x84", nil, 0xbc48596e, 0xde0fad41},
		{2, "\x80\x81\x82\x83\x84", nil, 0x21e3a154, 0x90112c3d},
		{5, "\x80\x81\x82\x83\x84", nil, 0x9b648616, 0x011dd507},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d/%x", tc.version, tc.name), func(t *testing.T) {
			info := dirHashInfo{version: tc.version, seed: tc.seed}
			if err := ext4fsDirhash([]byte(tc.name), &info); err != nil {
				t.Fatal(err)
			}
			if info.hash != tc.major || info.minorHash != tc.minor {
				t.Errorf("got (%08x, %08x), want (%08x, %08x)", info.hash, info.minorHash, tc.major, tc.minor)
			}
		})
	}
}

func TestHashWords(t *testing.T) {
	tests := []struct {
		name     string
		unsigned bool
		want     [4]uint32
	}{
		{"", false, [4]uint32{}},
		{"a", false, [4]uint32{0x01010161, 0x01010101, 0x01010101, 0x01010101}},
		{"ab", false, [4]uint32{0x02026162, 0x02020202, 0x02020202, 0x02020202}},
		{"abc", false, [4]uint32{0x03616263, 0x03030303, 0x03030303, 0x03030303}},
		{"abcd", false, [4]uint32{0x61626364, 0x04040404, 0x04040404, 0x04040404}},
		{"\x80\x81\x82\x83\x84", false, [4]uint32{0x7f808183, 0x05050484, 0x05050505, 0x05050505}},
		{"\x80\x81\x82\x83\x84", true, [4]uint32{0x80818283, 0x05050584, 0x05050505, 0x05050505}},
		{"\xff", false, [4]uint32{0x010100ff, 0x01010101, 0x01010101, 0x01010101}},
		{"\x00\xff", false, [4]uint32{0x0201ffff, 0x02020202, 0x02020202, 0x02020202}},
		{"\x00\x00\xff", false, [4]uint32{0x02ffffff, 0x03030303, 0x03030303, 0x03030303}},
		{"\x80\x00\x00\x00", false, [4]uint32{0x80000000, 0x04040404, 0x04040404, 0x04040404}},
		{"\x00\x80\x00\x00", false, [4]uint32{0xff800000, 0x04040404, 0x04040404, 0x04040404}},
		{"\x00\x00\x80\x00", false, [4]uint32{0xffff8000, 0x04040404, 0x04040404, 0x04040404}},
		{"\x00\x00\x00\x80", false, [4]uint32{0xffffff80, 0x04040404, 0x04040404, 0x04040404}},
		{"abcdefghijklmnopq", false, [4]uint32{0x61626364, 0x65666768, 0x696a6b6c, 0x6d6e6f70}},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%x/unsigned=%t", tc.name, tc.unsigned), func(t *testing.T) {
			got := [4]uint32{0xdeadbeef, 0xdeadbeef, 0xdeadbeef, 0xdeadbeef}
			hashWords([]byte(tc.name), got[:], tc.unsigned)
			if got != tc.want {
				t.Errorf("got %08x, want %08x", got, tc.want)
			}
		})
	}
	// Spare output words expose padding for a length above 255 without clamping
	// the remaining length to a byte or to a transform block's capacity.
	var words [80]uint32
	hashWords(make([]byte, 256), words[:], true)
	if words[64] != 0x01010100 || words[79] != 0x01010100 {
		t.Fatalf("256-byte padding = %#x, %#x; want 0x01010100", words[64], words[79])
	}
}

func TestTEATransform(t *testing.T) {
	tests := []struct{ state, input, want [4]uint32 }{
		{[4]uint32{}, [4]uint32{}, [4]uint32{0xa889f798, 0x182d8083, 0, 0}},
		{[4]uint32{0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476},
			[4]uint32{0x03616263, 0x03030303, 0x03030303, 0x03030303},
			[4]uint32{0xb1435ec5, 0x3f7eaa0e, 0x98badcfe, 0x10325476}},
		{[4]uint32{0xffffffff, 0, 0x12345678, 0x87654321},
			[4]uint32{0xffffffff, 0x12345678, 0x87654321, 0},
			[4]uint32{0xcf2c03bb, 0x2e8aeeda, 0x12345678, 0x87654321}},
	}
	for _, tc := range tests {
		state := tc.state
		TEATransform(&state, tc.input)
		if state != tc.want {
			t.Errorf("state %08x, input %08x: got %08x, want %08x", tc.state, tc.input, state, tc.want)
		}
	}
}

func TestDirhashSeedsAndAdjustment(t *testing.T) {
	for version := hashVersion(0); version <= 5; version++ {
		for _, name := range [][]byte{nil, []byte("same-name"), bytes.Repeat([]byte{0x80, 0, 0xff}, 23)} {
			plain := dirHashInfo{version: version}
			zero := dirHashInfo{version: version, seed: &[4]uint32{}}
			if err := ext4fsDirhash(name, &plain); err != nil {
				t.Fatal(err)
			}
			if err := ext4fsDirhash(name, &zero); err != nil {
				t.Fatal(err)
			}
			if plain.hash != zero.hash || plain.minorHash != zero.minorHash {
				t.Errorf("version %d: zero seed differs from absent seed", version)
			}
			seed := [4]uint32{0, 1, 0, 0xffffffff}
			custom := dirHashInfo{version: version, seed: &seed}
			if err := ext4fsDirhash(name, &custom); err != nil {
				t.Fatal(err)
			}
			if seed != [4]uint32{0, 1, 0, 0xffffffff} {
				t.Fatal("seed mutated")
			}
			if version == 0 || version == 3 {
				if custom.hash != plain.hash || custom.minorHash != plain.minorHash {
					t.Error("legacy depends on seed")
				}
			}
			if version == 2 || version == 5 {
				other := dirHashInfo{version: version, seed: &[4]uint32{0, 1, 2, 3}}
				if err := ext4fsDirhash(name, &other); err != nil {
					t.Fatal(err)
				}
				if custom.hash != other.hash || custom.minorHash != other.minorHash {
					t.Error("TEA depends on seed words 2 and 3")
				}
			}
		}
	}
	for _, raw := range []uint32{0, 1, 2, 0x80000001, 0xfffffffd, 0xfffffffe, 0xffffffff} {
		want := raw &^ 1
		if want == 0xfffffffe {
			want = 0xfffffffc
		}
		for _, version := range []hashVersion{1, 2, 4, 5} {
			seed := [4]uint32{raw, raw, 0xffffffff, 0}
			info := dirHashInfo{version: version, seed: &seed}
			if err := ext4fsDirhash(nil, &info); err != nil {
				t.Fatal(err)
			}
			minor := uint32(0xffffffff)
			if version == 2 || version == 5 {
				minor = raw
			}
			if info.hash != want || info.minorHash != minor {
				t.Errorf("version %d raw %#x: got (%#x, %#x), want (%#x, %#x)", version, raw, info.hash, info.minorHash, want, minor)
			}
		}
	}
}

func TestDirhashErrors(t *testing.T) {
	for _, version := range []hashVersion{7, 255} {
		info := dirHashInfo{version: version, hash: 123, minorHash: 456}
		if err := ext4fsDirhash(nil, &info); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("version %d: error %v", version, err)
		}
		if info.hash != 0 || info.minorHash != 0 {
			t.Error("unsupported version did not clear outputs")
		}
	}
	for _, name := range [][]byte{nil, []byte("abc")} {
		info := dirHashInfo{version: HashVersionSIP, hash: 123, minorHash: 456}
		if err := ext4fsDirhash(name, &info); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("missing key: error %v", err)
		}
		if info.hash != 123 || info.minorHash != 456 {
			t.Error("missing key modified outputs")
		}
	}
}

func TestDirhashBlockBoundaries(t *testing.T) {
	// Independently calculated from the spec using the repeating bytes below.
	tests := []struct {
		length int
		want   [6][2]uint32
	}{
		{1, [6][2]uint32{{0x949fce4c, 0}, {0x8ab2d53a, 0x85fa26f2}, {0xa6122120, 0x637dfbd4}, {0x949fce4c, 0}, {0x8ab2d53a, 0x85fa26f2}, {0xa6122120, 0x637dfbd4}}},
		{2, [6][2]uint32{{0x1e1f5db6, 0}, {0x5103ab52, 0x899387f7}, {0xa4ac7f02, 0x567c49aa}, {0x1e1f5db6, 0}, {0x5103ab52, 0x899387f7}, {0xa4ac7f02, 0x567c49aa}}},
		{3, [6][2]uint32{{0x21622502, 0}, {0xacd6be64, 0x4bfb34b5}, {0x6d1420d8, 0xb00d5b13}, {0x07dd7704, 0}, {0x799c1aa0, 0x7c1ea45a}, {0x3bc23a08, 0xf2b2e4f2}}},
		{4, [6][2]uint32{{0xfc66fccc, 0}, {0xa6d20e80, 0x20aeceff}, {0xc832b2fe, 0x58072e3d}, {0xfcd630c8, 0}, {0x17498c48, 0x987b75a7}, {0xc50a41de, 0x7048d523}}},
		{15, [6][2]uint32{{0x005150c6, 0}, {0x778339c8, 0x89e5671f}, {0x0c02437a, 0x310d9e3f}, {0x254b3944, 0}, {0x4d12b838, 0xc6781b32}, {0xf7c1b13c, 0xcba26694}}},
		{16, [6][2]uint32{{0x8a0f4e48, 0}, {0x04b3b2f0, 0xaca227eb}, {0x0ca505ec, 0xdbb492bc}, {0xb8619fb4, 0}, {0xdf09a868, 0x4026d991}, {0x6ee2ddae, 0x6ca612cb}}},
		{17, [6][2]uint32{{0xe6993224, 0}, {0xd5ab1870, 0x57343c16}, {0x791f2cf8, 0x2ea51591}, {0xf97469e6, 0}, {0xc35ce184, 0xc8b93ea5}, {0x9e78ce4e, 0xe8b9d5d5}}},
		{31, [6][2]uint32{{0x3713a346, 0}, {0xec70c2b6, 0x6125052e}, {0x5eb472f8, 0x499f612f}, {0x6a6d690a, 0}, {0xf5e48eb0, 0x05306e8e}, {0x34ef6dd6, 0x7125becd}}},
		{32, [6][2]uint32{{0x4c194ad4, 0}, {0x95a86b20, 0xe6fcf333}, {0x748b9602, 0xccc39c51}, {0xfa47d9d2, 0}, {0xb1f8c71e, 0x86557017}, {0x5c6efaa4, 0xfe6f8daf}}},
		{33, [6][2]uint32{{0x15d7e51a, 0}, {0xa7c20a9a, 0x68449ffb}, {0x7de0ba1a, 0x2c3c5b6e}, {0x01d295de, 0}, {0xb26ba11c, 0xd3d47e1d}, {0x1c5dd3d4, 0xa3b13cfb}}},
		{63, [6][2]uint32{{0x3d3de7f4, 0}, {0x5fa04676, 0x2ba356d9}, {0x72073bd4, 0x3be928ec}, {0x88149f48, 0}, {0xce52caa4, 0xc9bd3d59}, {0xbf58314e, 0xbb3430c2}}},
		{64, [6][2]uint32{{0x9860bb4e, 0}, {0x3a611734, 0x56da916f}, {0xae9a732c, 0xefcb683b}, {0xba27acc0, 0}, {0xafb72ff0, 0x2038e23a}, {0x959a56bc, 0x26e57cb8}}},
		{65, [6][2]uint32{{0x4915d39a, 0}, {0x7d855388, 0xe0058d1e}, {0xbdd8b4c2, 0x09c15a61}, {0x5fef3574, 0}, {0xd51c3678, 0x340dc813}, {0x40e39a6a, 0x02ebfbb6}}},
	}
	for _, tc := range tests {
		for version := hashVersion(0); version <= 5; version++ {
			t.Run(fmt.Sprintf("%d/length=%d", version, tc.length), func(t *testing.T) {
				name := bytes.Repeat([]byte{0, 0x7f, 0x80, 0xff, 0x81}, 13)[:tc.length]
				original := bytes.Clone(name)
				info := dirHashInfo{version: version}
				for repeat := 0; repeat < 2; repeat++ {
					if err := ext4fsDirhash(name, &info); err != nil {
						t.Fatal(err)
					}
					if got := [2]uint32{info.hash, info.minorHash}; got != tc.want[version] {
						t.Fatalf("call %d: got %08x, want %08x", repeat, got, tc.want[version])
					}
				}
				if !bytes.Equal(name, original) {
					t.Fatal("input mutated")
				}
				// ASCII (including embedded NUL) is identical in both byte modes.
				for i := range name {
					name[i] &= 0x7f
				}
				signed := dirHashInfo{version: version % 3}
				unsigned := dirHashInfo{version: version%3 + 3}
				if err := ext4fsDirhash(name, &signed); err != nil {
					t.Fatal(err)
				}
				if err := ext4fsDirhash(name, &unsigned); err != nil {
					t.Fatal(err)
				}
				if signed.hash != unsigned.hash || signed.minorHash != unsigned.minorHash {
					t.Fatal("ASCII signed and unsigned results differ")
				}
			})
		}
	}
}

func TestSipHash24(t *testing.T) {
	// Independent numeric vectors from the specified 64-bit round operations.
	// Input bytes and key bytes are their indices modulo 256.
	tests := []struct {
		length int
		want   uint64
	}{
		{0, 0x726fdb47dd0e0e31}, {1, 0x74f839c593dc67fd},
		{2, 0x0d6c8009d9a94f5a}, {3, 0x85676696d7fb7e2d},
		{4, 0xcf2794e0277187b7}, {5, 0x18765564cd99a68d},
		{6, 0xcbc9466e58fee3ce}, {7, 0xab0200f58b01d137},
		{8, 0x93f5f5799a932462}, {9, 0x9e0082df0ba9e4b0},
		{10, 0x7a5dbbc594ddb9f3}, {11, 0xf4b32f46226bada7},
		{12, 0x751e8fbc860ee5fb}, {13, 0x14ea5627c0843d90},
		{14, 0xf723ca908e7af2ee}, {15, 0xa129ca6149be45e5},
		{16, 0x3f2acc7f57c29bdb}, {17, 0x699ae9f52cbe4794},
		{31, 0x32d892fad841c342}, {32, 0x7127512f72f27cce},
		{33, 0xa7f32346f95978e3}, {63, 0x958a324ceb064572},
		{64, 0xacd2c40b8502cad8}, {65, 0x36b6bedb91a25924},
		{255, 0xa9c169fec74db21a}, {256, 0x999d0526d2a7bfd7},
		{257, 0x8a817b8d55b29748},
	}
	var key [16]byte
	for i := range key {
		key[i] = byte(i)
	}
	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.length), func(t *testing.T) {
			for offset := 0; offset < 8; offset++ {
				name := make([]byte, offset+tc.length)[offset:]
				for i := range name {
					name[i] = byte(i)
				}
				if got := sipHash24(name, key); got != tc.want {
					t.Fatalf("offset %d: got %016x, want %016x", offset, got, tc.want)
				}
				info := dirHashInfo{version: HashVersionSIP, sipHashKey: &key, seed: &[4]uint32{1, 2, 3, 4}}
				if err := ext4fsDirhash(name, &info); err != nil {
					t.Fatal(err)
				}
				if uint64(info.hash) != (tc.want>>32)&^1 || uint64(info.minorHash) != tc.want&0xffffffff {
					t.Fatalf("incorrect major/minor selection: (%08x, %08x)", info.hash, info.minorHash)
				}
			}
		})
	}
	info := dirHashInfo{version: HashVersionSIP, sipHashKey: &[16]byte{}}
	if err := ext4fsDirhash(nil, &info); err != nil {
		t.Fatal(err)
	}
	if info.hash != 0x1e924b9c || info.minorHash != 0x737700d7 {
		t.Fatalf("zero key empty hash: (%08x, %08x)", info.hash, info.minorHash)
	}
}
