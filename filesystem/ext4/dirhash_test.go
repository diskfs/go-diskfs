package ext4

import (
	"fmt"
	"strings"
	"testing"
)

// TestTEATransform tests the TEA (Tiny Encryption Algorithm) transform function
// with known inputs and verifies deterministic output.
func TestTEATransform(t *testing.T) {
	tests := []struct {
		name string
		buf  [4]uint32
		in   []uint32
	}{
		{"zero buf zero in", [4]uint32{0, 0, 0, 0}, []uint32{0, 0, 0, 0}},
		{"nonzero buf zero in", [4]uint32{1, 2, 3, 4}, []uint32{0, 0, 0, 0}},
		{"zero buf nonzero in", [4]uint32{0, 0, 0, 0}, []uint32{1, 2, 3, 4}},
		{"all ones", [4]uint32{0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff}, []uint32{0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff}},
		{"mixed values", [4]uint32{0x12345678, 0x9abcdef0, 0x0fedcba9, 0x87654321}, []uint32{0xdeadbeef, 0xcafebabe, 0xfeedface, 0x0d15ea5e}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TEATransform(tt.buf, tt.in)
			// TEA is deterministic — calling with the same input must return the same output
			result2 := TEATransform(tt.buf, tt.in)
			if result != result2 {
				t.Errorf("TEATransform is not deterministic: first %v, second %v", result, result2)
			}
			// Verify it actually changed from the initial buf (except for zero case which still shifts)
			// TEATransform adds to buf[0] and buf[1], so the result should differ from the original
			// unless in some degenerate case. We just verify it doesn't panic and is deterministic.
		})
	}
}

// TestStr2hashbuf tests the str2hashbuf helper function that converts strings to uint32 slices
func TestStr2hashbuf(t *testing.T) {
	tests := []struct {
		name   string
		msg    string
		num    int
		signed bool
	}{
		{"empty string 8 words", "", 8, false},
		{"short string 8 words", "hello", 8, false},
		{"exact 32 bytes", "abcdefghijklmnopqrstuvwxyz012345", 8, false},
		{"longer than num*4", "this is a very long string that exceeds the limit", 4, false},
		{"single char", "a", 8, false},
		{"signed mode", "hello", 8, true},
		{"num 1", "hello", 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := str2hashbuf(tt.msg, tt.num, tt.signed)
			if result == nil {
				t.Fatalf("str2hashbuf returned nil")
			}
			// The returned slice is from a fixed [8]uint32 array
			if len(result) != 8 {
				t.Errorf("expected length 8, got %d", len(result))
			}
		})
	}
}

// TestDxHackHash tests the legacy directory hash function
func TestDxHackHash(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		signed bool
	}{
		{"empty string unsigned", "", false},
		{"single char unsigned", "a", false},
		{"short name unsigned", "hello", false},
		{"typical filename unsigned", "testfile.txt", false},
		{"max length name unsigned", strings.Repeat("a", 255), false},
		{"empty string signed", "", true},
		{"short name signed", "hello", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := dxHackHash(tt.input, tt.signed)
			// Verify determinism
			hash2 := dxHackHash(tt.input, tt.signed)
			if hash != hash2 {
				t.Errorf("dxHackHash is not deterministic: first %d, second %d", hash, hash2)
			}
			// Verify the result is left-shifted by 1 (i.e. lowest bit is 0)
			if hash&1 != 0 {
				t.Errorf("dxHackHash result should have lowest bit 0 (shifted left by 1), got 0x%08x", hash)
			}
		})
	}

	// Different inputs should (generally) produce different hashes
	t.Run("different inputs differ", func(t *testing.T) {
		h1 := dxHackHash("file1.txt", false)
		h2 := dxHackHash("file2.txt", false)
		h3 := dxHackHash("completely_different_name", false)
		// It's theoretically possible for collisions, but these specific strings should differ
		if h1 == h2 && h2 == h3 {
			t.Errorf("all three different inputs produced the same hash: 0x%08x", h1)
		}
	})
}

// TestExt4fsDirhash tests the main directory hash entry point with all supported hash versions
func TestExt4fsDirhash(t *testing.T) {
	noSeed := []uint32{0, 0, 0, 0}
	customSeed := []uint32{0x12345678, 0x9abcdef0, 0x0fedcba9, 0x87654321}

	hashVersions := []struct {
		version hashVersion
		name    string
	}{
		{HashVersionLegacy, "Legacy"},
		{HashVersionHalfMD4, "HalfMD4"},
		{HashVersionTEA, "TEA"},
		{HashVersionLegacyUnsigned, "LegacyUnsigned"},
		{HashVersionHalfMD4Unsigned, "HalfMD4Unsigned"},
		{HashVersionTEAUnsigned, "TEAUnsigned"},
	}

	filenames := []string{
		"",
		"a",
		"hello.txt",
		"my_document.pdf",
		"very_long_filename_that_is_used_to_test_how_the_hash_function_handles_longer_inputs.data",
		strings.Repeat("x", 255), // max ext4 filename length
	}

	for _, hv := range hashVersions {
		t.Run(hv.name, func(t *testing.T) {
			for _, name := range filenames {
				displayName := name
				if len(displayName) > 30 {
					displayName = displayName[:30] + "..."
				}
				t.Run(fmt.Sprintf("name=%q", displayName), func(t *testing.T) {
					// Test with no seed
					hash, minor := ext4fsDirhash(name, hv.version, noSeed)

					// Verify determinism
					hash2, minor2 := ext4fsDirhash(name, hv.version, noSeed)
					if hash != hash2 || minor != minor2 {
						t.Errorf("ext4fsDirhash not deterministic: (%d,%d) vs (%d,%d)", hash, hash2, minor, minor2)
					}

					// The hash should have the lowest bit cleared (hash &= ^uint32(1))
					if hash&1 != 0 {
						t.Errorf("hash should have lowest bit cleared, got 0x%08x", hash)
					}

					// Test with custom seed
					hashS, minorS := ext4fsDirhash(name, hv.version, customSeed)
					hashS2, minorS2 := ext4fsDirhash(name, hv.version, customSeed)
					if hashS != hashS2 || minorS != minorS2 {
						t.Errorf("ext4fsDirhash not deterministic with seed: (%d,%d) vs (%d,%d)", hashS, hashS2, minorS, minorS2)
					}

					// For non-legacy versions, different seeds should generally produce different hashes
					// (Legacy ignores seed since it uses dxHackHash)
					if hv.version != HashVersionLegacy && hv.version != HashVersionLegacyUnsigned && name != "" {
						if hash == hashS && minor == minorS {
							// Collisions are possible but unlikely for non-trivial inputs
							t.Logf("WARNING: same hash with different seeds for %q: hash=0x%08x minor=0x%08x", displayName, hash, minor)
						}
					}
				})
			}
		})
	}
}

// TestExt4fsDirhashUnknownVersion tests that an unknown hash version returns zero
func TestExt4fsDirhashUnknownVersion(t *testing.T) {
	hash, minor := ext4fsDirhash("test", hashVersion(99), []uint32{0, 0, 0, 0})
	if hash != 0 || minor != 0 {
		t.Errorf("expected (0,0) for unknown hash version, got (%d,%d)", hash, minor)
	}
}

// TestExt4fsDirhashSIPVersion tests that SIP hash version (unimplemented) returns zero
func TestExt4fsDirhashSIPVersion(t *testing.T) {
	hash, minor := ext4fsDirhash("test", HashVersionSIP, []uint32{0, 0, 0, 0})
	if hash != 0 || minor != 0 {
		t.Errorf("expected (0,0) for SIP hash version (unimplemented), got (%d,%d)", hash, minor)
	}
}

// TestExt4fsDirhashDifferentNamesProduceDifferentHashes verifies that typical different filenames
// produce different hash values for each hash version (collision avoidance check)
func TestExt4fsDirhashDifferentNamesProduceDifferentHashes(t *testing.T) {
	noSeed := []uint32{0, 0, 0, 0}
	names := []string{
		"file1.txt",
		"file2.txt",
		"README.md",
		"main.go",
		"config.json",
		"data.bin",
		"image.png",
		"test_file",
		".hidden",
		"Makefile",
	}

	versions := []struct {
		version hashVersion
		name    string
	}{
		{HashVersionHalfMD4, "HalfMD4"},
		{HashVersionTEA, "TEA"},
		{HashVersionHalfMD4Unsigned, "HalfMD4Unsigned"},
		{HashVersionTEAUnsigned, "TEAUnsigned"},
		{HashVersionLegacy, "Legacy"},
		{HashVersionLegacyUnsigned, "LegacyUnsigned"},
	}

	for _, hv := range versions {
		t.Run(hv.name, func(t *testing.T) {
			hashes := make(map[uint32]string)
			collisions := 0
			for _, name := range names {
				hash, _ := ext4fsDirhash(name, hv.version, noSeed)
				if existingName, exists := hashes[hash]; exists {
					collisions++
					t.Logf("collision: %q and %q both hash to 0x%08x", existingName, name, hash)
				}
				hashes[hash] = name
			}
			// We expect very few if any collisions among 10 distinct filenames
			if collisions > 2 {
				t.Errorf("too many collisions (%d out of %d names) for hash version %s", collisions, len(names), hv.name)
			}
		})
	}
}

// TestExt4fsDirhashMinorHash verifies that HalfMD4 and TEA variants produce non-trivial minor hashes
func TestExt4fsDirhashMinorHash(t *testing.T) {
	noSeed := []uint32{0, 0, 0, 0}
	names := []string{"file1.txt", "file2.txt", "README.md", "main.go", "config.json"}

	// HalfMD4 variants should produce minor hashes from buf[2]
	t.Run("HalfMD4", func(t *testing.T) {
		allZero := true
		for _, name := range names {
			_, minor := ext4fsDirhash(name, HashVersionHalfMD4, noSeed)
			if minor != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			t.Errorf("all minor hashes are zero for HalfMD4 — expected some non-zero values")
		}
	})

	// TEA variants should produce minor hashes from buf[1]
	t.Run("TEA", func(t *testing.T) {
		allZero := true
		for _, name := range names {
			_, minor := ext4fsDirhash(name, HashVersionTEA, noSeed)
			if minor != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			t.Errorf("all minor hashes are zero for TEA — expected some non-zero values")
		}
	})

	// Legacy versions do NOT produce minor hashes (always 0)
	t.Run("Legacy", func(t *testing.T) {
		for _, name := range names {
			_, minor := ext4fsDirhash(name, HashVersionLegacy, noSeed)
			if minor != 0 {
				t.Errorf("expected minor hash 0 for Legacy version, got %d for %q", minor, name)
			}
		}
	})
}

// TestExt4fsDirhashLongNameChunking tests that long names are properly chunked in the hash computation.
// HalfMD4 processes in 32-byte chunks, TEA in 16-byte chunks.
func TestExt4fsDirhashLongNameChunking(t *testing.T) {
	noSeed := []uint32{0, 0, 0, 0}

	// Names that differ only after the first chunk boundary should produce different hashes
	t.Run("HalfMD4 differ after 32 bytes", func(t *testing.T) {
		prefix := strings.Repeat("a", 32)
		name1 := prefix + "XXXX"
		name2 := prefix + "YYYY"
		h1, _ := ext4fsDirhash(name1, HashVersionHalfMD4, noSeed)
		h2, _ := ext4fsDirhash(name2, HashVersionHalfMD4, noSeed)
		if h1 == h2 {
			t.Errorf("expected different hashes for names differing after 32 bytes, both got 0x%08x", h1)
		}
	})

	t.Run("TEA differ after 16 bytes", func(t *testing.T) {
		prefix := strings.Repeat("b", 16)
		name1 := prefix + "XXXX"
		name2 := prefix + "YYYY"
		h1, _ := ext4fsDirhash(name1, HashVersionTEA, noSeed)
		h2, _ := ext4fsDirhash(name2, HashVersionTEA, noSeed)
		if h1 == h2 {
			t.Errorf("expected different hashes for names differing after 16 bytes, both got 0x%08x", h1)
		}
	})
}
