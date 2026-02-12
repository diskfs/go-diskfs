package ext4

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/diskfs/go-diskfs/filesystem/ext4/crc"
	"github.com/google/uuid"
)

// Journal block types
type journalBlockType uint32

const (
	journalBlockTypeDescriptor   journalBlockType = 1
	journalBlockTypeCommit       journalBlockType = 2
	journalBlockTypeSuperblockV1 journalBlockType = 3
	journalBlockTypeSuperblockV2 journalBlockType = 4
	journalBlockTypeRevoke       journalBlockType = 5

	// Journal magic number
	journalMagic uint32 = 0xC03B3998

	// Checksum types
	checksumTypeCRC32  = 1
	checksumTypeMD5    = 2
	checksumTypeSHA1   = 3
	checksumTypeCRC32C = 4

	// Feature flags for jbd2 journal
	jbd2CompatFeatureChecksum      uint32 = 0x1
	jbd2IncompatFeatureRevoke      uint32 = 0x1
	jbd2IncompatFeature64Bit       uint32 = 0x2
	jbd2IncompatFeatureAsyncCommit uint32 = 0x4
	jbd2IncompatFeatureChecksumV2  uint32 = 0x8
	jbd2IncompatFeatureChecksumV3  uint32 = 0x10
	jbd2IncompatFeatureFastCommit  uint32 = 0x20

	// Tag flags
	tagFlagEscaped  uint16 = 0x1
	tagFlagSameUUID uint16 = 0x2
	tagFlagDeleted  uint16 = 0x4
	tagFlagLast     uint16 = 0x8

	// Journal superblock size
	JournalSuperblockSize = 1024
)

// journalHeader is the common 12-byte header for all journal blocks
type journalHeader struct {
	magic     uint32 // Should be journalMagic (0xC03B3998)
	blockType journalBlockType
	sequence  uint32
}

// JournalSuperblock represents the jbd2 journal superblock
type JournalSuperblock struct {
	header           *journalHeader
	blockSize        uint32
	maxLen           uint32
	first            uint32
	sequence         uint32
	start            uint32
	errno            uint32
	compatFeatures   uint32
	incompatFeatures uint32
	roCompatFeatures uint32
	uuid             *uuid.UUID
	nrUsers          uint32
	dynsuper         uint32
	maxTransaction   uint32
	maxTransData     uint32
	checksumType     byte
	maxFCBlocks      uint32
	head             uint32
	checksum         uint32
}

// journalBlockTag represents a block tag in a descriptor block (v3 format)
type journalBlockTag struct {
	blockNr  uint64 // 32-bit lower, 32-bit upper
	flags    uint32
	checksum uint32
	uuid     []byte // 16 bytes, only present if tagFlagSameUUID is not set
}

// journalBlockTagV2 represents a block tag in v2 format (variable size)
type journalBlockTagV2 struct {
	blockNr     uint32 // lower 32 bits
	checksum    uint16
	flags       uint16
	blockNrHigh uint32 // upper 32 bits (if 64-bit feature enabled)
	uuid        []byte // 16 bytes, only present if tagFlagSameUUID is not set
}

// journalDescriptorBlock represents a descriptor block containing block tags
type journalDescriptorBlock struct {
	header *journalHeader
	tags   []*journalBlockTag
	tail   *journalBlockTail // If checksum features enabled
}

// journalBlockTail is appended to descriptor and revoke blocks when checksums are enabled
type journalBlockTail struct {
	checksum uint32
}

// journalCommitBlock represents a commit block
type journalCommitBlock struct {
	header       *journalHeader
	checksumType byte
	checksumSize byte
	checksums    [32]byte // Space for checksums
	commitSec    uint64   // Seconds since epoch
	commitNsec   uint32   // Nanoseconds component
	checksumTail *journalBlockTail
}

// journalRevokeBlock represents a revocation block
type journalRevokeBlock struct {
	header *journalHeader
	count  uint32
	blocks []uint64 // Variable length array of block numbers
	tail   *journalBlockTail
}

// journalHeader methods

// journalHeaderFromBytes creates a journalHeader from bytes
func journalHeaderFromBytes(b []byte) (*journalHeader, error) {
	if len(b) < 12 {
		return nil, fmt.Errorf("cannot read journal header from %d bytes, need at least 12", len(b))
	}

	magic := binary.BigEndian.Uint32(b[0x0:0x4])
	if magic != journalMagic {
		return nil, fmt.Errorf("invalid journal magic: 0x%x (expected 0x%x)", magic, journalMagic)
	}

	return &journalHeader{
		magic:     magic,
		blockType: journalBlockType(binary.BigEndian.Uint32(b[0x4:0x8])),
		sequence:  binary.BigEndian.Uint32(b[0x8:0xc]),
	}, nil
}

// toBytes converts journalHeader to bytes
func (jh *journalHeader) toBytes() []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b[0x0:0x4], jh.magic)
	binary.BigEndian.PutUint32(b[0x4:0x8], uint32(jh.blockType))
	binary.BigEndian.PutUint32(b[0x8:0xc], jh.sequence)
	return b
}

// JournalSuperblock methods

// JournalSuperblockFromBytes creates a JournalSuperblock from bytes
func JournalSuperblockFromBytes(b []byte) (*JournalSuperblock, error) {
	if len(b) != JournalSuperblockSize {
		return nil, fmt.Errorf("cannot read journal superblock from %d bytes, expected %d", len(b), JournalSuperblockSize)
	}

	// Parse the header (first 12 bytes)
	header, err := journalHeaderFromBytes(b[0x0:0xc])
	if err != nil {
		return nil, fmt.Errorf("invalid journal superblock header: %v", err)
	}

	if header.blockType != journalBlockTypeSuperblockV1 && header.blockType != journalBlockTypeSuperblockV2 {
		return nil, fmt.Errorf("expected journal superblock type (3 or 4), got %d", header.blockType)
	}

	js := &JournalSuperblock{
		header:    header,
		blockSize: binary.BigEndian.Uint32(b[0xc:0x10]),
		maxLen:    binary.BigEndian.Uint32(b[0x10:0x14]),
		first:     binary.BigEndian.Uint32(b[0x14:0x18]),
		sequence:  binary.BigEndian.Uint32(b[0x18:0x1c]),
		start:     binary.BigEndian.Uint32(b[0x1c:0x20]),
		errno:     binary.BigEndian.Uint32(b[0x20:0x24]),
	}

	// V2 superblock fields
	if header.blockType == journalBlockTypeSuperblockV2 {
		js.compatFeatures = binary.BigEndian.Uint32(b[0x24:0x28])
		js.incompatFeatures = binary.BigEndian.Uint32(b[0x28:0x2c])
		js.roCompatFeatures = binary.BigEndian.Uint32(b[0x2c:0x30])

		// UUID (16 bytes)
		uuidBytes := make([]byte, 16)
		copy(uuidBytes, b[0x30:0x40])
		parsedUUID, err := uuid.FromBytes(uuidBytes)
		if err == nil {
			js.uuid = &parsedUUID
		}

		js.nrUsers = binary.BigEndian.Uint32(b[0x40:0x44])
		js.dynsuper = binary.BigEndian.Uint32(b[0x44:0x48])
		js.maxTransaction = binary.BigEndian.Uint32(b[0x48:0x4c])
		js.maxTransData = binary.BigEndian.Uint32(b[0x4c:0x50])
		js.checksumType = b[0x50]
		// 3 bytes padding at 0x51:0x54
		js.maxFCBlocks = binary.BigEndian.Uint32(b[0x54:0x58])
		js.head = binary.BigEndian.Uint32(b[0x58:0x5c])
		// 160 bytes padding at 0x5c:0xfc
		js.checksum = binary.BigEndian.Uint32(b[0xfc:0x100])
	}

	return js, nil
}

// ToBytes converts JournalSuperblock to bytes
func (js *JournalSuperblock) ToBytes() ([]byte, error) {
	b := make([]byte, JournalSuperblockSize)

	// Write header
	if js.header == nil {
		js.header = &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeSuperblockV2,
			sequence:  0,
		}
	}
	headerBytes := js.header.toBytes()
	copy(b[0x0:0xc], headerBytes)

	// Write basic fields
	binary.BigEndian.PutUint32(b[0xc:0x10], js.blockSize)
	binary.BigEndian.PutUint32(b[0x10:0x14], js.maxLen)
	binary.BigEndian.PutUint32(b[0x14:0x18], js.first)
	binary.BigEndian.PutUint32(b[0x18:0x1c], js.sequence)
	binary.BigEndian.PutUint32(b[0x1c:0x20], js.start)
	binary.BigEndian.PutUint32(b[0x20:0x24], js.errno)

	// V2 superblock fields
	binary.BigEndian.PutUint32(b[0x24:0x28], js.compatFeatures)
	binary.BigEndian.PutUint32(b[0x28:0x2c], js.incompatFeatures)
	binary.BigEndian.PutUint32(b[0x2c:0x30], js.roCompatFeatures)

	// UUID
	if js.uuid != nil {
		copy(b[0x30:0x40], js.uuid[:])
	}

	binary.BigEndian.PutUint32(b[0x40:0x44], js.nrUsers)
	binary.BigEndian.PutUint32(b[0x44:0x48], js.dynsuper)
	binary.BigEndian.PutUint32(b[0x48:0x4c], js.maxTransaction)
	binary.BigEndian.PutUint32(b[0x4c:0x50], js.maxTransData)
	b[0x50] = js.checksumType
	// 3 bytes padding at 0x51:0x54
	binary.BigEndian.PutUint32(b[0x54:0x58], js.maxFCBlocks)
	binary.BigEndian.PutUint32(b[0x58:0x5c], js.head)
	// 160 bytes padding at 0x5c:0xfc

	// Calculate and write checksum.
	// Per the kernel/e2fsprogs implementation, the journal superblock
	// checksum (s_checksum at offset 0xfc) is CRC32c(~0, jsb, sizeof(jsb))
	// with the checksum field itself zeroed. This covers the entire 1024-byte
	// journal superblock struct. The UUID is NOT used as a separate seed for
	// the superblock checksum (it is used as a seed for other journal block
	// checksums like descriptor/commit blocks, but not for the superblock).
	switch {
	case js.incompatFeatures&jbd2IncompatFeatureChecksumV3 != 0:
		// V3 checksum: CRC32C of entire superblock with checksum field zeroed
		binary.BigEndian.PutUint32(b[0xfc:0x100], 0)
		checksum := crc.CRC32c(0xffffffff, b[:JournalSuperblockSize])
		binary.BigEndian.PutUint32(b[0xfc:0x100], checksum)
	case js.compatFeatures&jbd2CompatFeatureChecksum != 0:
		// V1 compat checksum: same calculation
		binary.BigEndian.PutUint32(b[0xfc:0x100], 0)
		checksum := crc.CRC32c(0xffffffff, b[:JournalSuperblockSize])
		binary.BigEndian.PutUint32(b[0xfc:0x100], checksum)
	default:
		binary.BigEndian.PutUint32(b[0xfc:0x100], js.checksum)
	}

	// 768 bytes of user IDs at 0x100:0x400 (not used currently)

	return b, nil
}

// SupportsFeature checks if a given incompatible feature is set
func (js *JournalSuperblock) SupportsFeature(feature uint32) bool {
	return js.incompatFeatures&feature != 0
}

// SupportsCompatFeature checks if a given compatible feature is set
func (js *JournalSuperblock) SupportsCompatFeature(feature uint32) bool {
	return js.compatFeatures&feature != 0
}

// HasChecksums returns true if journal maintains checksums
func (js *JournalSuperblock) HasChecksums() bool {
	return js.compatFeatures&jbd2CompatFeatureChecksum != 0 ||
		js.incompatFeatures&jbd2IncompatFeatureChecksumV2 != 0 ||
		js.incompatFeatures&jbd2IncompatFeatureChecksumV3 != 0
}

// Uses64BitBlockNumbers returns true if 64-bit block numbers are supported
func (js *JournalSuperblock) Uses64BitBlockNumbers() bool {
	return js.incompatFeatures&jbd2IncompatFeature64Bit != 0
}

// journalDescriptorBlock methods

// journalDescriptorBlockFromBytes creates a journalDescriptorBlock from bytes
func journalDescriptorBlockFromBytes(b []byte, superblock *JournalSuperblock) (*journalDescriptorBlock, error) {
	if len(b) < 12 {
		return nil, fmt.Errorf("cannot read descriptor block from %d bytes, need at least 12", len(b))
	}

	header, err := journalHeaderFromBytes(b[0x0:0xc])
	if err != nil {
		return nil, fmt.Errorf("invalid descriptor block header: %v", err)
	}

	if header.blockType != journalBlockTypeDescriptor {
		return nil, fmt.Errorf("expected descriptor block type (1), got %d", header.blockType)
	}

	dblock := &journalDescriptorBlock{
		header: header,
		tags:   make([]*journalBlockTag, 0),
	}

	// Parse block tags
	offset := 12
	for offset < len(b) {
		tag, err := parseBlockTag(b[offset:], superblock)
		if err != nil {
			break // End of tags
		}
		dblock.tags = append(dblock.tags, tag)

		// Check if this is the last tag
		if tag.flags&uint32(tagFlagLast) != 0 {
			break
		}

		// Move to next tag
		tagSize := getBlockTagSize(superblock, tag)
		offset += tagSize
	}

	// Parse block tail if checksums are enabled
	if superblock != nil && (superblock.incompatFeatures&jbd2IncompatFeatureChecksumV2 != 0 ||
		superblock.incompatFeatures&jbd2IncompatFeatureChecksumV3 != 0) {
		if len(b) >= 4 {
			tail := &journalBlockTail{
				checksum: binary.BigEndian.Uint32(b[len(b)-4:]),
			}
			dblock.tail = tail
		}
	}

	return dblock, nil
}

// parseBlockTag parses a single block tag from bytes
func parseBlockTag(b []byte, superblock *JournalSuperblock) (*journalBlockTag, error) {
	if len(b) < 16 {
		return nil, fmt.Errorf("not enough bytes for block tag")
	}

	tag := &journalBlockTag{}

	// Always present: blockNr (lower), flags (upper)
	blockNrLower := binary.BigEndian.Uint32(b[0x0:0x4])
	tag.flags = binary.BigEndian.Uint32(b[0x4:0x8])
	tag.blockNr = uint64(blockNrLower)

	// If 64-bit support
	offset := 8
	if superblock != nil && superblock.Uses64BitBlockNumbers() {
		if len(b) < offset+4 {
			return nil, fmt.Errorf("not enough bytes for 64-bit block tag")
		}
		blockNrHigh := binary.BigEndian.Uint32(b[offset : offset+4])
		tag.blockNr |= uint64(blockNrHigh) << 32
		offset += 4
	}

	// Checksum
	if len(b) >= offset+4 {
		tag.checksum = binary.BigEndian.Uint32(b[offset : offset+4])
		offset += 4
	}

	// UUID (if not same as previous)
	if tag.flags&uint32(tagFlagSameUUID) == 0 {
		if len(b) >= offset+16 {
			tag.uuid = make([]byte, 16)
			copy(tag.uuid, b[offset:offset+16])
		}
	}

	return tag, nil
}

// getBlockTagSize returns the size of a block tag in bytes
func getBlockTagSize(superblock *JournalSuperblock, tag *journalBlockTag) int {
	size := 8 // Base: blockNr (4) + flags (4)

	if superblock != nil && superblock.Uses64BitBlockNumbers() {
		size += 4 // blockNrHigh
	}

	size += 4 // checksum

	if tag.flags&uint32(tagFlagSameUUID) == 0 {
		size += 16 // UUID
	}

	return size
}

// ToBytes converts journalDescriptorBlock to bytes
func (dblock *journalDescriptorBlock) ToBytes(superblock *JournalSuperblock, blockSize uint32) ([]byte, error) {
	b := make([]byte, blockSize)

	// Write header
	if dblock.header == nil {
		dblock.header = &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeDescriptor,
			sequence:  0,
		}
	}
	headerBytes := dblock.header.toBytes()
	copy(b[0x0:0xc], headerBytes)

	// Write tags
	offset := 12
	for i, tag := range dblock.tags {
		tagBytes := tag.toBytes(i == len(dblock.tags)-1, superblock)
		if offset+len(tagBytes) > len(b)-4 { // Reserve 4 bytes for tail if needed
			break
		}
		copy(b[offset:], tagBytes)
		offset += len(tagBytes)
	}

	// Write block tail if checksums are enabled
	if dblock.tail != nil {
		if offset+4 <= len(b) {
			binary.BigEndian.PutUint32(b[len(b)-4:], dblock.tail.checksum)
		}
	}

	return b, nil
}

// toBytes converts a journalBlockTag to bytes
func (tag *journalBlockTag) toBytes(isLast bool, superblock *JournalSuperblock) []byte {
	size := getBlockTagSize(superblock, tag)
	b := make([]byte, size)

	// Write lower 32 bits of block number
	binary.BigEndian.PutUint32(b[0x0:0x4], uint32(tag.blockNr&0xffffffff))

	// Write flags
	flags := tag.flags
	if isLast {
		flags |= uint32(tagFlagLast)
	}
	binary.BigEndian.PutUint32(b[0x4:0x8], flags)

	// Write upper 32 bits if 64-bit
	offset := 8
	if superblock != nil && superblock.Uses64BitBlockNumbers() {
		binary.BigEndian.PutUint32(b[offset:offset+4], uint32((tag.blockNr>>32)&0xffffffff))
		offset += 4
	}

	// Write checksum
	binary.BigEndian.PutUint32(b[offset:offset+4], tag.checksum)
	offset += 4

	// Write UUID if present
	if tag.flags&uint32(tagFlagSameUUID) == 0 && tag.uuid != nil {
		copy(b[offset:offset+16], tag.uuid)
	}

	return b
}

// journalCommitBlock methods

// journalCommitBlockFromBytes creates a journalCommitBlock from bytes
func journalCommitBlockFromBytes(b []byte) (*journalCommitBlock, error) {
	if len(b) < 32 {
		return nil, fmt.Errorf("cannot read commit block from %d bytes, need at least 32", len(b))
	}

	header, err := journalHeaderFromBytes(b[0x0:0xc])
	if err != nil {
		return nil, fmt.Errorf("invalid commit block header: %v", err)
	}

	if header.blockType != journalBlockTypeCommit {
		return nil, fmt.Errorf("expected commit block type (2), got %d", header.blockType)
	}

	cblock := &journalCommitBlock{
		header:       header,
		checksumType: b[0xc],
		checksumSize: b[0xd],
		commitSec:    binary.BigEndian.Uint64(b[0x30:0x38]),
		commitNsec:   binary.BigEndian.Uint32(b[0x38:0x3c]),
	}

	// Copy checksums array
	copy(cblock.checksums[:], b[0x10:0x30])

	return cblock, nil
}

// ToBytes converts journalCommitBlock to bytes
func (cblock *journalCommitBlock) ToBytes(blockSize uint32) ([]byte, error) {
	b := make([]byte, blockSize)

	// Write header
	if cblock.header == nil {
		cblock.header = &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeCommit,
			sequence:  0,
		}
	}
	headerBytes := cblock.header.toBytes()
	copy(b[0x0:0xc], headerBytes)

	b[0xc] = cblock.checksumType
	b[0xd] = cblock.checksumSize
	// 2 bytes padding at 0xe:0x10

	// Write checksums
	copy(b[0x10:0x30], cblock.checksums[:])

	// Write timestamp
	binary.BigEndian.PutUint64(b[0x30:0x38], cblock.commitSec)
	binary.BigEndian.PutUint32(b[0x38:0x3c], cblock.commitNsec)

	return b, nil
}

// SetCommitTime sets the commit block timestamp to the current time
func (cblock *journalCommitBlock) SetCommitTime(t time.Time) {
	cblock.commitSec = uint64(t.Unix())
	cblock.commitNsec = uint32(t.Nanosecond())
}

// journalRevokeBlock methods

// journalRevokeBlockFromBytes creates a journalRevokeBlock from bytes
func journalRevokeBlockFromBytes(b []byte, superblock *JournalSuperblock) (*journalRevokeBlock, error) {
	if len(b) < 16 {
		return nil, fmt.Errorf("cannot read revoke block from %d bytes, need at least 16", len(b))
	}

	header, err := journalHeaderFromBytes(b[0x0:0xc])
	if err != nil {
		return nil, fmt.Errorf("invalid revoke block header: %v", err)
	}

	if header.blockType != journalBlockTypeRevoke {
		return nil, fmt.Errorf("expected revoke block type (5), got %d", header.blockType)
	}

	rblock := &journalRevokeBlock{
		header: header,
		count:  binary.BigEndian.Uint32(b[0xc:0x10]),
		blocks: make([]uint64, 0),
	}

	// Parse block numbers
	offset := 16
	blockSize := uint32(4)
	if superblock != nil && superblock.Uses64BitBlockNumbers() {
		blockSize = 8
	}

	numBlocks := (rblock.count - 16) / blockSize
	for i := uint32(0); i < numBlocks && offset < len(b); i++ {
		if blockSize == 8 {
			if offset+8 <= len(b) {
				rblock.blocks = append(rblock.blocks, binary.BigEndian.Uint64(b[offset:offset+8]))
				offset += 8
			}
		} else {
			if offset+4 <= len(b) {
				rblock.blocks = append(rblock.blocks, uint64(binary.BigEndian.Uint32(b[offset:offset+4])))
				offset += 4
			}
		}
	}

	// Parse block tail if checksums are enabled
	if superblock != nil && (superblock.incompatFeatures&jbd2IncompatFeatureChecksumV2 != 0 ||
		superblock.incompatFeatures&jbd2IncompatFeatureChecksumV3 != 0) {
		if len(b) >= 4 {
			tail := &journalBlockTail{
				checksum: binary.BigEndian.Uint32(b[len(b)-4:]),
			}
			rblock.tail = tail
		}
	}

	return rblock, nil
}

// ToBytes converts journalRevokeBlock to bytes
func (rblock *journalRevokeBlock) ToBytes(superblock *JournalSuperblock, blockSize uint32) ([]byte, error) {
	b := make([]byte, blockSize)

	// Write header
	if rblock.header == nil {
		rblock.header = &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeRevoke,
			sequence:  0,
		}
	}
	headerBytes := rblock.header.toBytes()
	copy(b[0x0:0xc], headerBytes)

	// Calculate count
	blockNumSize := uint32(4)
	if superblock != nil && superblock.Uses64BitBlockNumbers() {
		blockNumSize = 8
	}
	count := 16 + uint32(len(rblock.blocks))*blockNumSize
	binary.BigEndian.PutUint32(b[0xc:0x10], count)

	// Write block numbers
	offset := 16
	for _, blockNum := range rblock.blocks {
		if blockNumSize == 8 {
			if offset+8 > len(b)-4 { // Reserve space for tail
				break
			}
			binary.BigEndian.PutUint64(b[offset:offset+8], blockNum)
			offset += 8
		} else {
			if offset+4 > len(b)-4 { // Reserve space for tail
				break
			}
			binary.BigEndian.PutUint32(b[offset:offset+4], uint32(blockNum&0xffffffff))
			offset += 4
		}
	}

	// Write block tail if checksums are enabled
	if rblock.tail != nil && offset+4 <= len(b) {
		binary.BigEndian.PutUint32(b[len(b)-4:], rblock.tail.checksum)
	}

	return b, nil
}

// AddBlock adds a block number to the revoke list
func (rblock *journalRevokeBlock) AddBlock(blockNum uint64) {
	rblock.blocks = append(rblock.blocks, blockNum)
}

// Helper function to create a new empty journal superblock
// NewJournalSuperblock creates a new empty journal superblock with default values
// blockSize is the filesystem block size in bytes
// journalBlocks is the total number of blocks in the journal
func NewJournalSuperblock(blockSize, journalBlocks uint32) *JournalSuperblock {
	newUUID, _ := uuid.NewRandom()
	return &JournalSuperblock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeSuperblockV2,
			sequence:  0,
		},
		blockSize:        blockSize,
		maxLen:           journalBlocks,
		first:            1,
		sequence:         1,
		start:            0,
		errno:            0,
		compatFeatures:   0,
		incompatFeatures: 0,
		roCompatFeatures: 0,
		uuid:             &newUUID,
		nrUsers:          1,
		dynsuper:         0,
		maxTransaction:   32768, // Default value
		maxTransData:     32768, // Default value
		checksumType:     checksumTypeCRC32C,
		maxFCBlocks:      0,
		head:             0,
		checksum:         0,
	}
}

// newJournalDescriptorBlock creates a new descriptor block with the given sequence number
func newJournalDescriptorBlock(sequence uint32) *journalDescriptorBlock {
	return &journalDescriptorBlock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeDescriptor,
			sequence:  sequence,
		},
		tags: make([]*journalBlockTag, 0),
	}
}

// newJournalCommitBlock creates a new commit block with the given sequence number
func newJournalCommitBlock(sequence uint32) *journalCommitBlock {
	return &journalCommitBlock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeCommit,
			sequence:  sequence,
		},
		checksumType: checksumTypeCRC32C,
		checksumSize: 4,
	}
}

// newJournalRevokeBlock creates a new revoke block with the given sequence number
func newJournalRevokeBlock(sequence uint32) *journalRevokeBlock {
	return &journalRevokeBlock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeRevoke,
			sequence:  sequence,
		},
		blocks: make([]uint64, 0),
	}
}
