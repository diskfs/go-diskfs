# ext4 directory filename hashing specification

## Source and scope

This specification describes the Linux kernel source at revision **05441d697f7f9a927264f4058cbbc6a03dfcac9d**, resolved from torvalds/linux master during this review on 4 November 2026. All algorithm requirements below refer to that revision, rather than to a moving branch or a general-purpose hash bearing the same name.

The primary source is [fs/ext4/hash.c](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/ext4/hash.c), reviewed in full. Supporting sources are linked where their behavior is described. This document contains prose, mathematical definitions, and data tables, but no implementation code. It is based on the referenced Linux sources, without consulting another implementation.

The subject is the computation of a directory filename's major and minor hashes, including all supported versions, byte conversion, compression transforms, seed selection, output adjustment, errors, and the optional case-folding wrapper. Directory index traversal, block splitting, filesystem checksums, encryption of filenames, and provisioning encryption keys are separate operations. A caller using encrypted directories must supply an already established directory hash key; the four-word filesystem hash seed is not that key.

## Inputs, arithmetic, and output contract

The core operation takes an ordered sequence of filename bytes, an explicit nonnegative byte length, a hash version, and an optional seed consisting of exactly four 32-bit words. SipHash additionally requires the parent directory's available encryption key and initialized directory hash key. The outer wrapper also depends on the directory's case-fold and encryption flags, the filesystem's Unicode map, and whether Unicode support is enabled.

The result consists of two separate unsigned 32-bit numbers: the **major hash** and the **minor hash**. Their order and selection are specified for each algorithm. They are not a generic serialized digest. Successful kernel calls return status zero.

Lengths count bytes, not Unicode characters. The core consumes exactly the stated number of bytes, with no trailing terminator added and no special interpretation of zero bytes. Filename validity and directory entry length limits are responsibilities of the surrounding filesystem. Negative lengths and insufficient input storage are outside this specification's valid input domain.

Unless a section explicitly says 64-bit, arithmetic on hash words is unsigned modulo 2³². Discard carries beyond bit 31 after addition, subtraction, multiplication, or left shifts. Bit numbers start at zero at the least significant bit. Right shifts insert zeros. Left rotations wrap the high bits back into the low positions within the stated word width. Bitwise complements are limited to that width. Numeric constants beginning with 0x are hexadecimal; other numbers in this document are decimal.

A signed-byte version interprets each byte below 128 as its ordinary value and each byte at least 128 as its value minus 256. Thus 0x80 means −128 and 0xff means −1. When such a value enters word arithmetic it is represented modulo 2³², preserving sign extension. Unsigned-byte versions interpret every byte as a value from 0 through 255. Version selection must determine this explicitly, independently of the implementation language's character type or host architecture.

## Hash versions and filesystem selection

The version identifiers are defined in [fs/ext4/ext4.h](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/ext4/ext4.h#L2659).

| Identifier | Algorithm | Byte interpretation | Raw major | Raw minor |
| --- | --- | --- | --- | --- |
| 0 | Legacy | Signed | Final legacy accumulator shifted left one bit | Zero |
| 1 | Half MD4 | Signed | State word 1 | State word 2 |
| 2 | TEA | Signed | State word 0 | State word 1 |
| 3 | Legacy | Unsigned | Final legacy accumulator shifted left one bit | Zero |
| 4 | Half MD4 | Unsigned | State word 1 | State word 2 |
| 5 | TEA | Unsigned | State word 0 | State word 1 |
| 6 | SipHash-2-4 | Raw bytes | High 32 bits of the 64-bit result | Low 32 bits of the 64-bit result |

Every successful version then applies the common major-hash adjustment described below. Minor hashes are never adjusted.

The identifiers accepted by the computational routine differ from the identifiers accepted in an indexed directory's root. In [fs/ext4/namei.c](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/ext4/namei.c#L779), root identifiers 0, 1, 2, and 6 are accepted. For 0 through 2, a filesystem-wide unsigned-hash offset of either zero or three is added before hashing. Identifier 6 is accepted only for directories whose entries store hashes, which are encrypted, case-folded directories. Such directories must use identifier 6.

[fs/ext4/super.c](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/ext4/super.c#L5296) converts each of the four on-disk seed words from little-endian into its numeric 32-bit value. It selects offset three when the filesystem's unsigned-hash flag, 0x0002, is set. Otherwise the signed-hash flag, 0x0001, selects offset zero. If neither flag is set, the kernel uses its native character signedness to choose the offset and records the corresponding flag on a writable filesystem. The unsigned flag takes precedence if both flags are set. These rules concern resolving filesystem metadata; a caller that already has an effective version from the table must not add the offset again.

The mount initialization accepts default version values 0 through 5 and rejects 6 and larger values. This does not change the narrower root validation above. A newly indexed encrypted, case-folded directory selects SipHash independently of the ordinary default.

## Seed selection

Half MD4 and TEA initialize a four-word state, indexed from zero, to the following numeric values in order:

| State word | Default value |
| --- | --- |
| 0 | 0x67452301 |
| 1 | 0xefcdab89 |
| 2 | 0x98badcfe |
| 3 | 0x10325476 |

An absent seed and an all-zero four-word seed both select this default. If **any** supplied seed word is nonzero, replace **all four** default words with the supplied seed, retaining any zero words it contains. Do this once for the entire filename. Do not reinitialize between blocks.

Legacy ignores the seed. SipHash ignores this four-word seed and instead uses the separate 128-bit directory hash key. TEA's transform uses only state words 0 and 1; words 2 and 3 cannot affect its result.

## Converting bytes into transform input words

Half MD4 and TEA use the same conversion procedure, with a capacity of eight words / 32 bytes for Half MD4 and four words / 16 bytes for TEA. This conversion is not MD4 message padding.

For each block, let L be the number of filename bytes remaining **before** limiting consumption to the block capacity. Construct a 32-bit padding word in two stages: bitwise-OR L with L shifted left eight bits, then bitwise-OR that result with itself shifted left sixteen bits. All shifts and results use the 32-bit rules above. For L from 0 through 255, this repeats L in each of the four bytes. Do not first truncate L to eight bits for a larger input.

Consume the smaller of L and the block capacity. Process each complete group of four bytes into one word, starting at zero and, for each byte in order, shifting the accumulator left eight bits and adding the interpreted byte value. For unsigned bytes this is ordinary big-endian word assembly. For signed bytes the additions of negative values can borrow across byte positions; simply assembling unsigned bytes and reinterpreting the final word as signed would be incorrect.

If capacity remains after all complete groups, start the next word at the padding word. Incorporate each of the remaining zero to three bytes by the same shift-left-eight-and-add operation. Emit this word even when there are zero remaining bytes. Fill every subsequent unused word with the unmodified padding word. If complete groups already filled the capacity, emit no additional word.

Consequently:

- A completely full block has no padding word and no extra block.
- A partial block whose consumed byte count is divisible by four has a padding-only word immediately after its complete words, followed by more padding words as necessary.
- With one to three trailing bytes, those bytes occupy the low positions of the next word after repeated shifting of the padding word.
- Padding depends on the current remaining length, not the number of bytes consumed, the original total length on every block, or the final short block's length on every block.
- The core does not perform this conversion at all for an empty filename.

Examples of conversion data, with words written as hexadecimal numbers:

| Input and remaining length | Byte interpretation | Beginning of resulting words |
| --- | --- | --- |
| ASCII abc, L = 3 | Either | 0x03616263, then padding words 0x03030303 |
| ASCII abcd, L = 4 | Either | 0x61626364, then padding words 0x04040404 |
| Bytes 80 81 82 83 84, L = 5 | Unsigned | 0x80818283, 0x05050584, then 0x05050505 |
| Bytes 80 81 82 83 84, L = 5 | Signed | 0x7f808183, 0x05050484, then 0x05050505 |

For a 33-byte Half MD4 input, conversion uses L = 33 for the first 32 bytes and L = 1 for the second block. For a 17-byte TEA input it uses L = 17 and then L = 1. Conversion produces numeric words and is independent of host byte order or alignment.

## Legacy hash

Initialize the current accumulator to 0x12a3fe2d and the previous accumulator to 0x37abe8f9.

For each byte, multiply its signed or unsigned interpreted value by 7,152,373 and represent the product as a 32-bit word. XOR that product with the current accumulator, then add the previous accumulator to obtain a tentative word. If its high bit is set, subtract 0x7fffffff once. This test applies to the tentative word before subtraction; it is not an instruction to repeatedly clear a sign bit.

Replace the previous accumulator with the old current accumulator and replace the current accumulator with the adjusted tentative word. After the final byte, shift the current accumulator left one bit to obtain the raw major hash. The minor hash is zero. Apply the common output adjustment.

For an empty input no recurrence is performed, giving major 0x2547fc5a and minor zero.

## Half MD4

### Block processing and state

Initialize the four state words using the seed rules. Process the filename in successive blocks of at most 32 bytes. Convert each block into eight words using the conversion rules and the current remaining byte count. Compress it into the existing state. Continue with the next 32 bytes until no bytes remain. A final full block does not cause another padding block.

The compression transform takes eight input words, indexed 0 through 7, and a four-word state. At the start of each transform copy the state into four working registers A, B, C, D, preserving the incoming state for feed-forward.

Define these functions independently at every bit position:

- **Selection F(x, y, z):** select the bit of y where x is one and the bit of z where x is zero.
- **Majority G(x, y, z):** produce one if at least two of the three bits are one.
- **Parity H(x, y, z):** XOR the three bits.

There are three rounds of eight operations each. Each operation adds the current target register, the round's function applied to three ordered registers, the scheduled input word, and the round constant. Rotate that sum left by the scheduled rotation and replace the target with the result. Only that register changes in the operation. Later operations use all earlier updates.

The register roles for operations 1 through 8 of **each** round are:

| Operation | Target | Ordered function arguments |
| --- | --- | --- |
| 1 | A | B, C, D |
| 2 | D | A, B, C |
| 3 | C | D, A, B |
| 4 | B | C, D, A |
| 5 | A | B, C, D |
| 6 | D | A, B, C |
| 7 | C | D, A, B |
| 8 | B | C, D, A |

The complete round schedules are:

| Round | Function | Constant | Input word indices, in operation order | Left rotations, in operation order |
| --- | --- | --- | --- | --- |
| 1 | F | 0x00000000 | 0, 1, 2, 3, 4, 5, 6, 7 | 3, 7, 11, 19, 3, 7, 11, 19 |
| 2 | G | 0x5a827999 | 1, 3, 5, 7, 0, 2, 4, 6 | 3, 5, 9, 13, 3, 5, 9, 13 |
| 3 | H | 0x6ed9eba1 | 3, 7, 2, 6, 1, 5, 0, 4 | 3, 9, 11, 15, 3, 9, 11, 15 |

Do not reset the registers between rounds. After all 24 operations, add A, B, C, D respectively to the four preserved incoming state words, modulo 2³². These four sums are the outgoing state for the next block. The compression routine's individual return value is outgoing state word 1, but all four outgoing state words must be retained.

### Final result

After all blocks, take state word 1 as the raw major hash and state word 2 as the minor hash, then apply the common output adjustment. An empty filename causes no transform; its output is derived directly from the selected initial state. With the default seed it yields major 0xefcdab88 and minor 0x98badcfe.

This is a reduced MD4-derived construction with eight-word blocks and 24 operations. There is no 0x80 suffix, appended bit length, standard MD4 finalization, or full MD4 digest truncation.

## TEA

Initialize the same four-word state under the same seed rules. Process successive blocks of at most 16 bytes, converting each to four words using the current remaining length. A full final block produces no additional block.

For each compression, preserve incoming state words 0 and 1. Copy them to working words X and Y. Name the four converted input words K0, K1, K2, K3 in their original order. Initialize a 32-bit running sum to zero for this block.

Perform exactly sixteen iterations, in this order within each iteration:

1. Add 0x9e3779b9 to the running sum.
2. Form three terms: Y shifted left four bits plus K0; Y plus the updated running sum; and Y shifted right five bits plus K1. XOR the three terms and add the result to X.
3. Using the **updated X**, form three terms: X shifted left four bits plus K2; X plus the same running sum; and X shifted right five bits plus K3. XOR these terms and add the result to Y.

All terms and updates are 32-bit modular quantities. After the sixteenth iteration, add the final X to preserved incoming state word 0 and the final Y to preserved incoming state word 1. Those sums replace state words 0 and 1. State words 2 and 3 remain unchanged. Reset the running sum to zero at the start of the next block, but retain the updated state.

After the last block, state word 0 is the raw major hash and state word 1 is the minor hash. Apply the common output adjustment. Empty input performs no transform; with the default seed its major is 0x67452300 and its minor is 0xefcdab89.

The transform uses sixteen iterations and feed-forward addition to the incoming state. The converted filename block supplies the four mixing words; the filesystem seed supplies the initial state.

## SipHash

### Key and ext4 integration

The kernel's [fscrypt filename hashing helper](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/crypto/fname.c#L457) applies SipHash-2-4 to the given bytes with the directory's initialized secret 128-bit hash key. There is no signed-byte variant and no use of the four-word seed or the conversion procedure above.

[fs/crypto/keysetup.c](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/crypto/keysetup.c#L285) obtains this key from fscrypt's key derivation, using its directory-hash context and the directory's nonce. It interprets the resulting sixteen bytes as two little-endian 64-bit words, K0 and K1. The hash operation assumes that key setup has already completed; it does not derive a key from the filesystem seed. Encryption policy and the fscrypt key derivation protocol are outside this filename-hash operation.

### Exact SipHash-2-4 operation

The supporting algorithm is defined by [include/linux/siphash.h](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/include/linux/siphash.h) and [lib/crypto/siphash.c](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/lib/crypto/siphash.c). In this section only, use 64-bit modular addition, shifts, rotations, and bitwise operations.

Initialize four working words V0 through V3 by XORing these constants with the indicated key word:

| Word | Constant | Key word |
| --- | --- | --- |
| V0 | 0x736f6d6570736575 | K0 |
| V1 | 0x646f72616e646f6d | K1 |
| V2 | 0x6c7967656e657261 | K0 |
| V3 | 0x7465646279746573 | K1 |

One SipHash round consists of the following sequential updates. Each update uses the latest values:

1. Add V1 to V0. Rotate V1 left 13 bits, then XOR it with V0. Rotate V0 left 32 bits.
2. Add V3 to V2. Rotate V3 left 16 bits, then XOR it with V2.
3. Add V3 to V0. Rotate V3 left 21 bits, then XOR it with V0.
4. Add V1 to V2. Rotate V1 left 17 bits, then XOR it with V2. Rotate V2 left 32 bits.

Consume every complete eight-byte group as a little-endian 64-bit numeric word M. For each group, XOR M into V3, perform two rounds, then XOR M into V0.

Construct a final word with the low eight bits of the total input byte length in its most significant byte. Place the remaining zero to seven input bytes into its lowest byte positions in little-endian order; all other positions are zero. This final word is always processed, even for empty input or an exact multiple of eight bytes.

XOR the final word into V3, perform two rounds, and XOR the final word into V0. Next XOR 0xff into V2, perform four rounds, and XOR V0, V1, V2, V3 together to produce the 64-bit result.

ext4 selects the high 32 bits as the raw major hash and the low 32 bits as the minor hash, then applies the common major-hash adjustment. Do not exchange those halves based on host byte order.

## Common major-hash adjustment

For every successful version, clear bit zero of the raw major hash. If the resulting value is 0xfffffffe, replace it with 0xfffffffc. Leave all other bits and the minor hash unchanged.

The reserved value follows from the 32-bit htree end-of-directory constant 0x7fffffff, defined in [fs/ext4/ext4.h](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/ext4/ext4.h#L2685): shifting it left once produces 0xfffffffe. Thus raw values 0xfffffffe and 0xffffffff both become 0xfffffffc. Raw 0xfffffffd also becomes 0xfffffffc by ordinary bit clearing. Neither major nor minor is restricted to a signed 31-bit value.

The resulting major is always even and never 0xfffffffe. The low bit is available to directory index collision bookkeeping: [namei.c](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/ext4/namei.c#L1987) can set it on a split boundary when a hash continues from the preceding block. That bookkeeping does not change the filename hash returned by this operation.

## Optional Unicode case-folding wrapper

When Unicode support is compiled in, the public operation first considers case folding. Fold only when the input length is nonzero, the parent directory has its case-fold flag set, and either the directory is unencrypted or its encryption key is available. In all other circumstances, pass the original bytes and length to the core hash operation.

For folding, allocate a buffer of PATH_MAX bytes; the Linux value is 4096. Failure to allocate returns an out-of-memory error without hashing. Use the filesystem's configured Unicode map, rather than the host runtime's current Unicode version or locale.

The [Unicode normalization definitions](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/include/linux/unicode.h#L39) and [UTF-8 case-fold routine](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/unicode/utf8-core.c#L100) specify NFDICF: canonical decomposition, removal of default-ignorable code points, and full Unicode case folding using the common and full mappings. This can change byte length and can produce an empty sequence. It is more than ASCII lowercasing or one-character-to-one-character conversion. The filesystem's versioned Unicode tables are an external input to this wrapper; a standalone core byte-hash implementation need not implement those tables.

On successful folding, hash the folded UTF-8 bytes using their resulting byte length, excluding their terminator. Every block's padding length is based on those resulting bytes. On a negative folding result, including malformed UTF-8 or insufficient buffer capacity, discard the folding attempt and hash the **entire original sequence** with its original length. Do not hash a partially folded prefix or turn this fallback into a hash error. A folded length of zero is a successful fold and uses the core's empty-input behavior.

Without Unicode support the wrapper always hashes the original sequence. Broader name validation, including any strict encoding policy enforced elsewhere, is separate from this wrapper's fallback behavior.

Encrypted, case-folded callers can prepare a folded name and precompute its SipHash before directory traversal; [namei.c](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/ext4/namei.c#L1360) then reuses the prepared result. A directory lacking the encryption key may instead use already stored hash values through surrounding filesystem operations. That does not authorize computing a fresh SipHash without the key.

## Errors and empty-input behavior

An unsupported effective version returns an invalid-argument error, sets both output hashes to zero, and skips the common adjustment. The kernel also emits a warning identifying the unsupported version.

SipHash without an available directory encryption key returns an invalid-argument error and emits a warning. This path does **not** assign either output field, so their incoming contents remain unchanged. It does not select another algorithm or produce a zero-key SipHash. Buffer allocation failure in the folding wrapper likewise occurs before output assignments. On any error, callers must use the status rather than treating the output fields as a computed hash.

For versions 0 through 5, zero-length input is accepted, requires no input-byte access, and follows the empty-input results described above. It can therefore be used to probe support. A zero-length input does not bypass SipHash's key requirement; with a key it produces SipHash of the empty sequence. The comment describing a null-name, zero-length support probe must be understood with this version-specific condition.

## Conformance data and review cases

The following data comes from the pinned upstream [fs/ext4/hash-test.c](https://github.com/torvalds/linux/blob/05441d697f7f9a927264f4058cbbc6a03dfcac9d/fs/ext4/hash-test.c). Results already include the common major adjustment. Unless specified otherwise, use the default seed and disable case folding. ASCII inputs contain exactly the displayed characters, with no terminating byte. Hex-byte inputs are literal bytes, including bytes that are invalid UTF-8.

| Version | Input | Seed | Major | Minor |
| --- | --- | --- | --- | --- |
| 0 | ASCII abc | Default | 0x75afd992 | 0x00000000 |
| 3 | ASCII abc | Default | 0x75afd992 | 0x00000000 |
| 1 | ASCII abc | Default | 0xd196a868 | 0xc420eb28 |
| 4 | ASCII abc | Default | 0xd196a868 | 0xc420eb28 |
| 2 | ASCII abc | Default | 0xb1435ec4 | 0x3f7eaa0e |
| 5 | ASCII abc | Default | 0xb1435ec4 | 0x3f7eaa0e |
| 1 | Empty sequence | Default | 0xefcdab88 | 0x98badcfe |
| 1 | ASCII 1234567890123456789012345678901 | Default | 0xc4db1f78 | 0xea23921b |
| 1 | ASCII 12345678901234567890123456789012 | Default | 0xfa6cc63e | 0x2f77bd1c |
| 1 | ASCII 123456789012345678901234567890123 | Default | 0xdc0c2dec | 0x5ca23365 |
| 4 | ASCII 1234567890123456789012345678901 | Default | 0xc4db1f78 | 0xea23921b |
| 4 | ASCII 12345678901234567890123456789012 | Default | 0xfa6cc63e | 0x2f77bd1c |
| 4 | ASCII 123456789012345678901234567890123 | Default | 0xdc0c2dec | 0x5ca23365 |
| 2 | ASCII 123456789abcdef | Default | 0xa562903a | 0x6174a00f |
| 2 | ASCII 1234567890abcdef | Default | 0x8449f258 | 0x49a16d46 |
| 2 | ASCII 123456789abcdefgh | Default | 0xf32ec10c | 0x58ceae61 |
| 1 | ASCII same-name | 0x11111111, 0x22222222, 0x33333333, 0x44444444 | 0x8aebf604 | 0x66ce48fe |
| 1 | Hex bytes 80 81 82 83 84 | Default | 0x8bab0498 | 0xc326632d |
| 4 | Hex bytes 80 81 82 83 84 | Default | 0xbc48596e | 0xde0fad41 |
| 2 | Hex bytes 80 81 82 83 84 | Default | 0x21e3a154 | 0x90112c3d |
| 5 | Hex bytes 80 81 82 83 84 | Default | 0x9b648616 | 0x011dd507 |

The upstream tests also establish that different nonzero seeds can change Half MD4 results, all three algorithm families distinguish signed and unsigned high-byte input, unsupported versions clear both output fields, SipHash fails without a key, case-folded ASCII Alpha and aLPHa produce identical hash pairs, and invalid UTF-8 bytes c3 28 take the opaque-byte fallback.

Additional consequences of the algorithm definitions should be covered when validating a new implementation:

- Absent and all-zero seeds are equivalent; a partly zero, nonzero seed replaces the entire state. Legacy is seed-independent, and changing only seed words 2 and 3 cannot change TEA.
- Empty Half MD4 and TEA results come directly from the selected initial state. No block is compressed.
- Inputs at lengths 1, 2, 3, 4, 15, 16, 17, 31, 32, 33, 63, 64, and 65 exercise partial words, exact block boundaries, and multiple blocks. Include high bytes in each of the four positions of a complete word and in every partial-word position.
- Signed and unsigned counterparts are identical when every input byte is below 128. Include zero bytes within explicitly sized input to confirm length-based processing.
- Repeated calls start with fresh state; block chaining occurs only within one filename. A digest cannot be obtained by independently hashing blocks and combining the results.
- Common output adjustment affects only the major word, including when an empty-input seed deliberately selects a raw major near 0xffffffff.
- Unicode tests need canonical-equivalent names, full case-fold expansions, default-ignorable characters, invalid sequences, and a fold that becomes empty, all under the same configured Unicode map.
- SipHash tests must use the separate 128-bit key, exercise zero and nonzero byte lengths around multiples of eight, and check high-half versus low-half output selection before major adjustment. Buffer alignment and CPU byte order must not change results.

These review cases are derived requirements and boundary checks, not additional upstream expected-value vectors.
