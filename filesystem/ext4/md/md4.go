// Package md implements ext4's reduced MD4 directory-hash transform.
package md

import "math/bits"

func f(x, y, z uint32) uint32 { return (x & y) | (^x & z) }
func g(x, y, z uint32) uint32 { return (x & y) | (x & z) | (y & z) }
func h(x, y, z uint32) uint32 { return x ^ y ^ z }

// HalfMD4Transform compresses eight numeric input words into state in place and
// returns the updated state word 1. It performs 24 operations with feed-forward;
// it does not implement standard MD4 padding or finalization.
func HalfMD4Transform(state *[4]uint32, input [8]uint32) uint32 {
	a, b, c, d := state[0], state[1], state[2], state[3]
	rounds := [...]struct {
		function  func(uint32, uint32, uint32) uint32
		constant  uint32
		indices   [8]int
		rotations [4]int
	}{
		{f, 0, [8]int{0, 1, 2, 3, 4, 5, 6, 7}, [4]int{3, 7, 11, 19}},
		{g, 0x5a827999, [8]int{1, 3, 5, 7, 0, 2, 4, 6}, [4]int{3, 5, 9, 13}},
		{h, 0x6ed9eba1, [8]int{3, 7, 2, 6, 1, 5, 0, 4}, [4]int{3, 9, 11, 15}},
	}
	for _, round := range rounds {
		for i, index := range round.indices {
			word := input[index] + round.constant
			rotation := round.rotations[i%4]
			switch i % 4 {
			case 0:
				a = bits.RotateLeft32(a+round.function(b, c, d)+word, rotation)
			case 1:
				d = bits.RotateLeft32(d+round.function(a, b, c)+word, rotation)
			case 2:
				c = bits.RotateLeft32(c+round.function(d, a, b)+word, rotation)
			case 3:
				b = bits.RotateLeft32(b+round.function(c, d, a)+word, rotation)
			}
		}
	}
	state[0] += a
	state[1] += b
	state[2] += c
	state[3] += d
	return state[1]
}
