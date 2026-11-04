package ext4

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"syscall"

	"github.com/diskfs/go-diskfs/filesystem/ext4/md"
)

type hashVersion uint8

const (
	HashVersionLegacy          = 0
	HashVersionHalfMD4         = 1
	HashVersionTEA             = 2
	HashVersionLegacyUnsigned  = 3
	HashVersionHalfMD4Unsigned = 4
	HashVersionTEAUnsigned     = 5
	HashVersionSIP             = 6
)

// dirHashInfo holds an effective hash version (with any filesystem unsigned
// offset already applied), numeric seed words, and the two output hashes.
// A non-nil sipHashKey means the directory encryption key is available and its
// separate directory hash key has been initialized. An all-zero key is valid.
type dirHashInfo struct {
	version    hashVersion
	seed       *[4]uint32
	sipHashKey *[16]byte
	hash       uint32
	minorHash  uint32
}

// ext4fsDirhash hashes exactly len(name) opaque bytes, including embedded zeros.
// Callers needing case folding must prepare the name using the filesystem's
// Unicode map before calling. On an unsupported version both outputs are zero;
// on a missing SipHash key both outputs retain their incoming values.
func ext4fsDirhash(name []byte, info *dirHashInfo) error {
	var major, minor uint32
	unsigned := info.version >= HashVersionLegacyUnsigned && info.version <= HashVersionTEAUnsigned
	switch info.version {
	case HashVersionLegacy, HashVersionLegacyUnsigned:
		current, previous := uint32(0x12a3fe2d), uint32(0x37abe8f9)
		for _, b := range name {
			next := previous + (current ^ (hashByte(b, unsigned) * 7152373))
			if next&0x80000000 != 0 {
				next -= 0x7fffffff
			}
			previous, current = current, next
		}
		major = current << 1
	case HashVersionHalfMD4, HashVersionHalfMD4Unsigned, HashVersionTEA, HashVersionTEAUnsigned:
		state := [4]uint32{0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476}
		if info.seed != nil && *info.seed != [4]uint32{} {
			state = *info.seed
		}
		halfMD4 := info.version == HashVersionHalfMD4 || info.version == HashVersionHalfMD4Unsigned
		for len(name) > 0 {
			if halfMD4 {
				var input [8]uint32
				hashWords(name, input[:], unsigned)
				md.HalfMD4Transform(&state, input)
				name = name[min(len(name), 32):]
			} else {
				var input [4]uint32
				hashWords(name, input[:], unsigned)
				TEATransform(&state, input)
				name = name[min(len(name), 16):]
			}
		}
		if halfMD4 {
			major, minor = state[1], state[2]
		} else {
			major, minor = state[0], state[1]
		}
	case HashVersionSIP:
		if info.sipHashKey == nil {
			return fmt.Errorf("ext4 directory SipHash requires a directory hash key: %w", syscall.EINVAL)
		}
		hash := sipHash24(name, *info.sipHashKey)
		major, minor = uint32(hash>>32), uint32(hash)
	default:
		info.hash, info.minorHash = 0, 0
		return fmt.Errorf("unsupported ext4 directory hash version %d: %w", info.version, syscall.EINVAL)
	}
	major &^= 1
	if major == 0xfffffffe {
		major = 0xfffffffc
	}
	info.hash, info.minorHash = major, minor
	return nil
}

func hashByte(b byte, unsigned bool) uint32 {
	if unsigned {
		return uint32(b)
	}
	return uint32(int32(int8(b)))
}

// hashWords converts one block. name includes all remaining bytes so padding
// uses the remaining length before truncation to the output's capacity.
func hashWords(name []byte, output []uint32, unsigned bool) {
	pad := uint32(len(name))
	pad |= pad << 8
	pad |= pad << 16
	name = name[:min(len(name), len(output)*4)]
	for i := range output {
		word := pad
		n := min(len(name), 4)
		if n == 4 {
			word = 0
		}
		for _, b := range name[:n] {
			word = (word << 8) + hashByte(b, unsigned)
		}
		output[i] = word
		name = name[n:]
	}
}

// TEATransform mixes four input words into state using sixteen TEA iterations
// and feed-forward addition. State words 2 and 3 are left unchanged.
func TEATransform(state *[4]uint32, input [4]uint32) {
	x, y := state[0], state[1]
	var sum uint32
	for i := 0; i < 16; i++ {
		sum += 0x9e3779b9
		x += ((y << 4) + input[0]) ^ (y + sum) ^ ((y >> 5) + input[1])
		y += ((x << 4) + input[2]) ^ (x + sum) ^ ((x >> 5) + input[3])
	}
	state[0] += x
	state[1] += y
}

func sipHashRound(v *[4]uint64) {
	v[0] += v[1]
	v[1] = bits.RotateLeft64(v[1], 13) ^ v[0]
	v[0] = bits.RotateLeft64(v[0], 32)
	v[2] += v[3]
	v[3] = bits.RotateLeft64(v[3], 16) ^ v[2]
	v[0] += v[3]
	v[3] = bits.RotateLeft64(v[3], 21) ^ v[0]
	v[2] += v[1]
	v[1] = bits.RotateLeft64(v[1], 17) ^ v[2]
	v[2] = bits.RotateLeft64(v[2], 32)
}

func sipHash24(name []byte, key [16]byte) uint64 {
	k0, k1 := binary.LittleEndian.Uint64(key[:8]), binary.LittleEndian.Uint64(key[8:])
	v := [4]uint64{0x736f6d6570736575 ^ k0, 0x646f72616e646f6d ^ k1,
		0x6c7967656e657261 ^ k0, 0x7465646279746573 ^ k1}
	compress := func(word uint64) {
		v[3] ^= word
		sipHashRound(&v)
		sipHashRound(&v)
		v[0] ^= word
	}
	last := uint64(len(name)&0xff) << 56
	for len(name) >= 8 {
		compress(binary.LittleEndian.Uint64(name[:8]))
		name = name[8:]
	}
	for i, b := range name {
		last |= uint64(b) << (8 * i)
	}
	compress(last)
	v[2] ^= 0xff
	for i := 0; i < 4; i++ {
		sipHashRound(&v)
	}
	return v[0] ^ v[1] ^ v[2] ^ v[3]
}
