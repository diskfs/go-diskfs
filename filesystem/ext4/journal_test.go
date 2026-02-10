package ext4

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestJournalHeaderFromBytes tests parsing a journal header from bytes
func TestJournalHeaderFromBytes(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
		check   func(*journalHeader)
	}{
		{
			name: "valid header with descriptor block",
			input: func() []byte {
				b := make([]byte, 12)
				binary.BigEndian.PutUint32(b[0x0:0x4], journalMagic)
				binary.BigEndian.PutUint32(b[0x4:0x8], uint32(journalBlockTypeDescriptor))
				binary.BigEndian.PutUint32(b[0x8:0xc], 42)
				return b
			}(),
			wantErr: false,
			check: func(jh *journalHeader) {
				if jh.magic != journalMagic {
					t.Errorf("magic = %x, want %x", jh.magic, journalMagic)
				}
				if jh.blockType != journalBlockTypeDescriptor {
					t.Errorf("blockType = %d, want %d", jh.blockType, journalBlockTypeDescriptor)
				}
				if jh.sequence != 42 {
					t.Errorf("sequence = %d, want 42", jh.sequence)
				}
			},
		},
		{
			name: "valid header with commit block",
			input: func() []byte {
				b := make([]byte, 12)
				binary.BigEndian.PutUint32(b[0x0:0x4], journalMagic)
				binary.BigEndian.PutUint32(b[0x4:0x8], uint32(journalBlockTypeCommit))
				binary.BigEndian.PutUint32(b[0x8:0xc], 100)
				return b
			}(),
			wantErr: false,
			check: func(jh *journalHeader) {
				if jh.blockType != journalBlockTypeCommit {
					t.Errorf("blockType = %d, want %d", jh.blockType, journalBlockTypeCommit)
				}
			},
		},
		{
			name:    "invalid magic number",
			input:   make([]byte, 12),
			wantErr: true,
		},
		{
			name:    "insufficient bytes",
			input:   make([]byte, 11),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jh, err := journalHeaderFromBytes(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("journalHeaderFromBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.check != nil {
				tt.check(jh)
			}
		})
	}
}

// TestJournalHeaderToBytes tests serializing a journal header to bytes
func TestJournalHeaderToBytes(t *testing.T) {
	jh := &journalHeader{
		magic:     journalMagic,
		blockType: journalBlockTypeDescriptor,
		sequence:  123,
	}

	b := jh.toBytes()

	if len(b) != 12 {
		t.Errorf("toBytes() returned %d bytes, want 12", len(b))
	}

	if magic := binary.BigEndian.Uint32(b[0x0:0x4]); magic != journalMagic {
		t.Errorf("magic = %x, want %x", magic, journalMagic)
	}

	if blockType := binary.BigEndian.Uint32(b[0x4:0x8]); blockType != uint32(journalBlockTypeDescriptor) {
		t.Errorf("blockType = %d, want %d", blockType, journalBlockTypeDescriptor)
	}

	if sequence := binary.BigEndian.Uint32(b[0x8:0xc]); sequence != 123 {
		t.Errorf("sequence = %d, want 123", sequence)
	}
}

// TestJournalHeaderRoundTrip tests that header can be serialized and deserialized
func TestJournalHeaderRoundTrip(t *testing.T) {
	original := &journalHeader{
		magic:     journalMagic,
		blockType: journalBlockTypeSuperblockV2,
		sequence:  456,
	}

	b := original.toBytes()
	restored, err := journalHeaderFromBytes(b)

	if err != nil {
		t.Fatalf("journalHeaderFromBytes() error = %v", err)
	}

	if restored.magic != original.magic {
		t.Errorf("magic mismatch: %x != %x", restored.magic, original.magic)
	}
	if restored.blockType != original.blockType {
		t.Errorf("blockType mismatch: %d != %d", restored.blockType, original.blockType)
	}
	if restored.sequence != original.sequence {
		t.Errorf("sequence mismatch: %d != %d", restored.sequence, original.sequence)
	}
}

// TestJournalSuperblockFromBytes tests parsing a journal superblock
func TestJournalSuperblockFromBytes(t *testing.T) {
	// Create valid superblock bytes
	b := make([]byte, JournalSuperblockSize)

	// Header
	binary.BigEndian.PutUint32(b[0x0:0x4], journalMagic)
	binary.BigEndian.PutUint32(b[0x4:0x8], uint32(journalBlockTypeSuperblockV2))
	binary.BigEndian.PutUint32(b[0x8:0xc], 0)

	// Basic fields
	binary.BigEndian.PutUint32(b[0xc:0x10], 4096)  // blockSize
	binary.BigEndian.PutUint32(b[0x10:0x14], 1000) // maxLen
	binary.BigEndian.PutUint32(b[0x14:0x18], 1)    // first
	binary.BigEndian.PutUint32(b[0x18:0x1c], 1)    // sequence
	binary.BigEndian.PutUint32(b[0x1c:0x20], 0)    // start
	binary.BigEndian.PutUint32(b[0x20:0x24], 0)    // errno

	// V2 fields
	binary.BigEndian.PutUint32(b[0x24:0x28], jbd2CompatFeatureChecksum) // compatFeatures
	binary.BigEndian.PutUint32(b[0x28:0x2c], jbd2IncompatFeature64Bit)  // incompatFeatures
	binary.BigEndian.PutUint32(b[0x2c:0x30], 0)                         // roCompatFeatures
	binary.BigEndian.PutUint32(b[0x40:0x44], 1)                         // nrUsers
	binary.BigEndian.PutUint32(b[0x48:0x4c], 32768)                     // maxTransaction
	binary.BigEndian.PutUint32(b[0x4c:0x50], 32768)                     // maxTransData
	b[0x50] = checksumTypeCRC32C

	// UUID
	testUUID, _ := uuid.NewRandom()
	copy(b[0x30:0x40], testUUID[:])

	js, err := JournalSuperblockFromBytes(b)

	if err != nil {
		t.Fatalf("JournalSuperblockFromBytes() error = %v", err)
	}

	if js.blockSize != 4096 {
		t.Errorf("blockSize = %d, want 4096", js.blockSize)
	}

	if js.maxLen != 1000 {
		t.Errorf("maxLen = %d, want 1000", js.maxLen)
	}

	if js.first != 1 {
		t.Errorf("first = %d, want 1", js.first)
	}

	if js.sequence != 1 {
		t.Errorf("sequence = %d, want 1", js.sequence)
	}

	if js.nrUsers != 1 {
		t.Errorf("nrUsers = %d, want 1", js.nrUsers)
	}

	if !js.Uses64BitBlockNumbers() {
		t.Error("Uses64BitBlockNumbers() = false, want true")
	}

	if js.uuid.String() != testUUID.String() {
		t.Errorf("uuid mismatch: %s != %s", js.uuid.String(), testUUID.String())
	}
}

// TestJournalSuperblockToBytes tests serializing a journal superblock
func TestJournalSuperblockToBytes(t *testing.T) {
	testUUID, _ := uuid.NewRandom()
	js := &JournalSuperblock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeSuperblockV2,
			sequence:  0,
		},
		blockSize:        4096,
		maxLen:           1000,
		first:            1,
		sequence:         1,
		start:            0,
		errno:            0,
		compatFeatures:   jbd2CompatFeatureChecksum,
		incompatFeatures: jbd2IncompatFeature64Bit,
		roCompatFeatures: 0,
		uuid:             &testUUID,
		nrUsers:          1,
		dynsuper:         0,
		maxTransaction:   32768,
		maxTransData:     32768,
		checksumType:     checksumTypeCRC32C,
		maxFCBlocks:      0,
		head:             0,
		checksum:         0,
	}

	b, err := js.ToBytes()

	if err != nil {
		t.Fatalf("ToBytes() error = %v", err)
	}

	if len(b) != JournalSuperblockSize {
		t.Errorf("ToBytes() returned %d bytes, want %d", len(b), JournalSuperblockSize)
	}

	// Verify magic
	if magic := binary.BigEndian.Uint32(b[0x0:0x4]); magic != journalMagic {
		t.Errorf("magic = %x, want %x", magic, journalMagic)
	}

	// Verify blockSize
	if blockSize := binary.BigEndian.Uint32(b[0xc:0x10]); blockSize != 4096 {
		t.Errorf("blockSize = %d, want 4096", blockSize)
	}

	// Verify maxLen
	if maxLen := binary.BigEndian.Uint32(b[0x10:0x14]); maxLen != 1000 {
		t.Errorf("maxLen = %d, want 1000", maxLen)
	}
}

// TestJournalSuperblockRoundTrip tests serialization and deserialization
func TestJournalSuperblockRoundTrip(t *testing.T) {
	testUUID, _ := uuid.NewRandom()
	original := &JournalSuperblock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeSuperblockV2,
			sequence:  0,
		},
		blockSize:        4096,
		maxLen:           1000,
		first:            1,
		sequence:         1,
		start:            0,
		errno:            0,
		compatFeatures:   jbd2CompatFeatureChecksum,
		incompatFeatures: jbd2IncompatFeature64Bit,
		roCompatFeatures: 0,
		uuid:             &testUUID,
		nrUsers:          1,
		dynsuper:         0,
		maxTransaction:   32768,
		maxTransData:     32768,
		checksumType:     checksumTypeCRC32C,
		maxFCBlocks:      0,
		head:             0,
		checksum:         0,
	}

	b, err := original.ToBytes()
	if err != nil {
		t.Fatalf("ToBytes() error = %v", err)
	}

	restored, err := JournalSuperblockFromBytes(b)
	if err != nil {
		t.Fatalf("JournalSuperblockFromBytes() error = %v", err)
	}

	if restored.blockSize != original.blockSize {
		t.Errorf("blockSize: %d != %d", restored.blockSize, original.blockSize)
	}
	if restored.maxLen != original.maxLen {
		t.Errorf("maxLen: %d != %d", restored.maxLen, original.maxLen)
	}
	if restored.first != original.first {
		t.Errorf("first: %d != %d", restored.first, original.first)
	}
	if restored.sequence != original.sequence {
		t.Errorf("sequence: %d != %d", restored.sequence, original.sequence)
	}
	if restored.uuid.String() != original.uuid.String() {
		t.Errorf("uuid: %s != %s", restored.uuid.String(), original.uuid.String())
	}
}

// TestJournalSuperblockFeatureFlags tests feature flag methods
func TestJournalSuperblockFeatureFlags(t *testing.T) {
	testUUID, _ := uuid.NewRandom()
	js := &JournalSuperblock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeSuperblockV2,
			sequence:  0,
		},
		blockSize:        4096,
		maxLen:           1000,
		first:            1,
		sequence:         1,
		start:            0,
		errno:            0,
		compatFeatures:   jbd2CompatFeatureChecksum,
		incompatFeatures: jbd2IncompatFeature64Bit | jbd2IncompatFeatureChecksumV3,
		roCompatFeatures: 0,
		uuid:             &testUUID,
		nrUsers:          1,
		checksumType:     checksumTypeCRC32C,
	}

	tests := []struct {
		name     string
		method   func() bool
		expected bool
	}{
		{
			name:     "HasChecksums",
			method:   js.HasChecksums,
			expected: true,
		},
		{
			name:     "Uses64BitBlockNumbers",
			method:   js.Uses64BitBlockNumbers,
			expected: true,
		},
		{
			name: "SupportsCompatFeature checksum",
			method: func() bool {
				return js.SupportsCompatFeature(jbd2CompatFeatureChecksum)
			},
			expected: true,
		},
		{
			name: "SupportsFeature 64bit",
			method: func() bool {
				return js.SupportsFeature(jbd2IncompatFeature64Bit)
			},
			expected: true,
		},
		{
			name: "SupportsFeature revoke (not set)",
			method: func() bool {
				return js.SupportsFeature(jbd2IncompatFeatureRevoke)
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.method()
			if result != tt.expected {
				t.Errorf("%s() = %v, want %v", tt.name, result, tt.expected)
			}
		})
	}
}

// TestNewJournalSuperblock tests creating a new journal superblock
func TestNewJournalSuperblock(t *testing.T) {
	js := NewJournalSuperblock(4096, 1000)

	if js.blockSize != 4096 {
		t.Errorf("blockSize = %d, want 4096", js.blockSize)
	}

	if js.maxLen != 1000 {
		t.Errorf("maxLen = %d, want 1000", js.maxLen)
	}

	if js.first != 1 {
		t.Errorf("first = %d, want 1", js.first)
	}

	if js.sequence != 1 {
		t.Errorf("sequence = %d, want 1", js.sequence)
	}

	if js.uuid == nil {
		t.Error("uuid is nil, want valid UUID")
	}

	// By default, journal should have no features enabled (matching mke2fs behavior)
	if js.HasChecksums() {
		t.Error("HasChecksums() = true, want false (default)")
	}

	if js.Uses64BitBlockNumbers() {
		t.Error("Uses64BitBlockNumbers() = true, want false (default)")
	}
}

// TestJournalCommitBlock tests commit block operations
func TestJournalCommitBlock(t *testing.T) {
	cb := newJournalCommitBlock(42)

	if cb.header.blockType != journalBlockTypeCommit {
		t.Errorf("blockType = %d, want %d", cb.header.blockType, journalBlockTypeCommit)
	}

	if cb.header.sequence != 42 {
		t.Errorf("sequence = %d, want 42", cb.header.sequence)
	}

	testTime := time.Unix(1609459200, 123456789) // 2021-01-01 00:00:00 UTC
	cb.SetCommitTime(testTime)

	if cb.commitSec != uint64(testTime.Unix()) {
		t.Errorf("commitSec = %d, want %d", cb.commitSec, uint64(testTime.Unix()))
	}

	if cb.commitNsec != uint32(testTime.Nanosecond()) {
		t.Errorf("commitNsec = %d, want %d", cb.commitNsec, uint32(testTime.Nanosecond()))
	}

	// Test serialization
	b, err := cb.ToBytes(4096)
	if err != nil {
		t.Fatalf("ToBytes() error = %v", err)
	}

	if len(b) != 4096 {
		t.Errorf("ToBytes() returned %d bytes, want 4096", len(b))
	}

	// Verify magic in serialized form
	if magic := binary.BigEndian.Uint32(b[0x0:0x4]); magic != journalMagic {
		t.Errorf("serialized magic = %x, want %x", magic, journalMagic)
	}
}

// TestJournalCommitBlockRoundTrip tests commit block serialization
func TestJournalCommitBlockRoundTrip(t *testing.T) {
	original := newJournalCommitBlock(123)
	testTime := time.Unix(1609459200, 987654321)
	original.SetCommitTime(testTime)

	b, err := original.ToBytes(4096)
	if err != nil {
		t.Fatalf("ToBytes() error = %v", err)
	}

	restored, err := journalCommitBlockFromBytes(b)
	if err != nil {
		t.Fatalf("journalCommitBlockFromBytes() error = %v", err)
	}

	if restored.commitSec != original.commitSec {
		t.Errorf("commitSec: %d != %d", restored.commitSec, original.commitSec)
	}

	if restored.commitNsec != original.commitNsec {
		t.Errorf("commitNsec: %d != %d", restored.commitNsec, original.commitNsec)
	}
}

// TestJournalRevokeBlock tests revocation block operations
func TestJournalRevokeBlock(t *testing.T) {
	rb := newJournalRevokeBlock(50)

	if rb.header.blockType != journalBlockTypeRevoke {
		t.Errorf("blockType = %d, want %d", rb.header.blockType, journalBlockTypeRevoke)
	}

	if rb.header.sequence != 50 {
		t.Errorf("sequence = %d, want 50", rb.header.sequence)
	}

	// Add some blocks
	rb.AddBlock(100)
	rb.AddBlock(200)
	rb.AddBlock(300)

	if len(rb.blocks) != 3 {
		t.Errorf("len(blocks) = %d, want 3", len(rb.blocks))
	}

	if rb.blocks[0] != 100 {
		t.Errorf("blocks[0] = %d, want 100", rb.blocks[0])
	}

	if rb.blocks[1] != 200 {
		t.Errorf("blocks[1] = %d, want 200", rb.blocks[1])
	}

	if rb.blocks[2] != 300 {
		t.Errorf("blocks[2] = %d, want 300", rb.blocks[2])
	}
}

// TestJournalRevokeBlockSerialization tests revocation block serialization
func TestJournalRevokeBlockSerialization(t *testing.T) {
	testUUID, _ := uuid.NewRandom()
	superblock := &JournalSuperblock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeSuperblockV2,
			sequence:  0,
		},
		blockSize:        4096,
		maxLen:           1000,
		first:            1,
		sequence:         1,
		incompatFeatures: jbd2IncompatFeature64Bit,
		uuid:             &testUUID,
	}

	rb := newJournalRevokeBlock(25)
	rb.AddBlock(100)
	rb.AddBlock(200)

	b, err := rb.ToBytes(superblock, 4096)
	if err != nil {
		t.Fatalf("ToBytes() error = %v", err)
	}

	if len(b) != 4096 {
		t.Errorf("ToBytes() returned %d bytes, want 4096", len(b))
	}

	// Verify header
	if magic := binary.BigEndian.Uint32(b[0x0:0x4]); magic != journalMagic {
		t.Errorf("magic = %x, want %x", magic, journalMagic)
	}

	if blockType := binary.BigEndian.Uint32(b[0x4:0x8]); blockType != uint32(journalBlockTypeRevoke) {
		t.Errorf("blockType = %d, want %d", blockType, journalBlockTypeRevoke)
	}

	// Verify count field
	count := binary.BigEndian.Uint32(b[0xc:0x10])
	if count < 16 {
		t.Errorf("count = %d, want >= 16", count)
	}
}

// TestJournalDescriptorBlock tests descriptor block operations
func TestJournalDescriptorBlock(t *testing.T) {
	db := newJournalDescriptorBlock(75)

	if db.header.blockType != journalBlockTypeDescriptor {
		t.Errorf("blockType = %d, want %d", db.header.blockType, journalBlockTypeDescriptor)
	}

	if db.header.sequence != 75 {
		t.Errorf("sequence = %d, want 75", db.header.sequence)
	}

	if len(db.tags) != 0 {
		t.Errorf("initial tags length = %d, want 0", len(db.tags))
	}
}

// TestJournalDescriptorBlockSerialization tests descriptor block serialization
func TestJournalDescriptorBlockSerialization(t *testing.T) {
	testUUID, _ := uuid.NewRandom()
	superblock := &JournalSuperblock{
		header: &journalHeader{
			magic:     journalMagic,
			blockType: journalBlockTypeSuperblockV2,
			sequence:  0,
		},
		blockSize:        4096,
		maxLen:           1000,
		first:            1,
		sequence:         1,
		incompatFeatures: jbd2IncompatFeature64Bit,
		uuid:             &testUUID,
	}

	db := newJournalDescriptorBlock(100)

	// Create some block tags
	tag1 := &journalBlockTag{
		blockNr:  1000,
		flags:    0,
		checksum: 0,
	}

	tag2 := &journalBlockTag{
		blockNr:  2000,
		flags:    uint32(tagFlagLast), // Last tag
		checksum: 0,
	}

	db.tags = append(db.tags, tag1, tag2)

	b, err := db.ToBytes(superblock, 4096)
	if err != nil {
		t.Fatalf("ToBytes() error = %v", err)
	}

	if len(b) != 4096 {
		t.Errorf("ToBytes() returned %d bytes, want 4096", len(b))
	}

	// Verify magic
	if magic := binary.BigEndian.Uint32(b[0x0:0x4]); magic != journalMagic {
		t.Errorf("magic = %x, want %x", magic, journalMagic)
	}

	// Verify block type
	if blockType := binary.BigEndian.Uint32(b[0x4:0x8]); blockType != uint32(journalBlockTypeDescriptor) {
		t.Errorf("blockType = %d, want %d", blockType, journalBlockTypeDescriptor)
	}
}

// TestBlockTagSerialization tests block tag serialization with 64-bit support
func TestBlockTagSerialization(t *testing.T) {
	testUUID, _ := uuid.NewRandom()
	superblock := &JournalSuperblock{
		incompatFeatures: jbd2IncompatFeature64Bit,
		uuid:             &testUUID,
	}

	tag := &journalBlockTag{
		blockNr:  0x123456789ABCDEF0,
		flags:    uint32(tagFlagSameUUID), // Same UUID, so no UUID field
		checksum: 0xDEADBEEF,
	}

	b := tag.toBytes(false, superblock)

	// Should be: 4 bytes lower blockNr + 4 bytes flags + 4 bytes upper blockNr + 4 bytes checksum = 16 bytes
	expectedSize := 16
	if len(b) != expectedSize {
		t.Errorf("toBytes() returned %d bytes, want %d", len(b), expectedSize)
	}

	// Verify lower block number
	lower := binary.BigEndian.Uint32(b[0x0:0x4])
	if uint64(lower) != tag.blockNr&0xffffffff {
		t.Errorf("lower blockNr = %x, want %x", lower, uint32(tag.blockNr&0xffffffff))
	}

	// Verify upper block number
	upper := binary.BigEndian.Uint32(b[0x8:0xc])
	if uint64(upper) != tag.blockNr>>32 {
		t.Errorf("upper blockNr = %x, want %x", upper, uint32(tag.blockNr>>32))
	}

	// Verify checksum
	checksum := binary.BigEndian.Uint32(b[0xc:0x10])
	if checksum != 0xDEADBEEF {
		t.Errorf("checksum = %x, want %x", checksum, 0xDEADBEEF)
	}
}

// TestBlockTagLastFlag tests last flag handling in block tags
func TestBlockTagLastFlag(t *testing.T) {
	testUUID, _ := uuid.NewRandom()
	superblock := &JournalSuperblock{
		incompatFeatures: jbd2IncompatFeature64Bit,
		uuid:             &testUUID,
	}

	tag := &journalBlockTag{
		blockNr:  1000,
		flags:    0,
		checksum: 0,
	}

	// Serialize with isLast=true
	b := tag.toBytes(true, superblock)

	// Verify flags field has last flag set
	flags := binary.BigEndian.Uint32(b[0x4:0x8])
	if flags&uint32(tagFlagLast) == 0 {
		t.Error("last flag not set in serialized tag")
	}
}

// TestGetBlockTagSize tests the block tag size calculation
func TestGetBlockTagSize(t *testing.T) {
	testUUID, _ := uuid.NewRandom()

	tests := []struct {
		name        string
		superblock  *JournalSuperblock
		tag         *journalBlockTag
		expectedMin int
	}{
		{
			name: "without 64-bit, with UUID",
			superblock: &JournalSuperblock{
				incompatFeatures: 0,
				uuid:             &testUUID,
			},
			tag: &journalBlockTag{
				flags: 0, // UUID included
				uuid:  make([]byte, 16),
			},
			expectedMin: 24, // 4 + 4 + 4 + 16
		},
		{
			name: "with 64-bit, with UUID",
			superblock: &JournalSuperblock{
				incompatFeatures: jbd2IncompatFeature64Bit,
				uuid:             &testUUID,
			},
			tag: &journalBlockTag{
				flags: 0, // UUID included
				uuid:  make([]byte, 16),
			},
			expectedMin: 28, // 4 + 4 + 4 + 4 + 16
		},
		{
			name: "with 64-bit, same UUID",
			superblock: &JournalSuperblock{
				incompatFeatures: jbd2IncompatFeature64Bit,
				uuid:             &testUUID,
			},
			tag: &journalBlockTag{
				flags: uint32(tagFlagSameUUID), // UUID not included
			},
			expectedMin: 12, // 4 + 4 + 4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := getBlockTagSize(tt.superblock, tt.tag)
			if size < tt.expectedMin {
				t.Errorf("getBlockTagSize() = %d, want >= %d", size, tt.expectedMin)
			}
		})
	}
}
