package ext4

import (
	"encoding/binary"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/google/uuid"
)

// TestCreateInodesPerGroupAlignment verifies Create rounds inodes_per_group
// to a multiple of both inodes_per_block and 8 (inode bitmap granularity).
// At 650 MiB with 4 KiB blocks, rounding to 8 alone yields 6936 (% 16 == 8)
// and Linux 6.1 ext4lazyinit rejects group 0; rounding to inodes_per_block
// alone breaks the multiple-of-8 bitmap invariant for 1 KiB blocks.
func TestCreateInodesPerGroupAlignment(t *testing.T) {
	tests := []struct {
		name            string
		size            int64
		sectorsPerBlock uint8
		blocksize       int64
	}{
		{name: "4KiB blocks", size: 650 * 1024 * 1024, sectorsPerBlock: 8, blocksize: 4096},
		{name: "1KiB blocks", size: 64 * 1024 * 1024, sectorsPerBlock: 2, blocksize: 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, f := testCreateEmptyFile(t, tt.size)
			defer f.Close()

			fsuuid := uuid.MustParse("6d696372-6f76-4d00-b007-66735f763100")
			fs, err := Create(file.New(f, false), tt.size, 0, 512, &Params{
				VolumeName:      "rootfs",
				UUID:            &fsuuid,
				SectorsPerBlock: tt.sectorsPerBlock,
				Features:        []FeatureOpt{WithFeatureReservedGDTBlocksForExpansion(false)},
			})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			if fs == nil {
				t.Fatal("expected non-nil filesystem")
			}

			// s_inodes_per_group at superblock offset 0x28; superblock at byte 1024.
			var sbBuf [4]byte
			if _, err := f.ReadAt(sbBuf[:], 1024+0x28); err != nil {
				t.Fatalf("read s_inodes_per_group: %v", err)
			}
			ipg := binary.LittleEndian.Uint32(sbBuf[:])

			inodesPerBlock := uint32(tt.blocksize / DefaultInodeSize)
			if ipg%inodesPerBlock != 0 {
				t.Errorf("inodes_per_group=%d not divisible by inodes_per_block=%d", ipg, inodesPerBlock)
			}
			if ipg%8 != 0 {
				t.Errorf("inodes_per_group=%d not a multiple of 8 (inode bitmap granularity)", ipg)
			}

			// Group descriptor 0 bg_flags at offset 0x12 within the descriptor.
			// GDT starts at block 1 for >1KiB blocks, block 2 for 1KiB blocks.
			gdtOffset := tt.blocksize
			if tt.blocksize == 1024 {
				gdtOffset = 2048
			}
			var gdBuf [2]byte
			if _, err := f.ReadAt(gdBuf[:], gdtOffset+0x12); err != nil {
				t.Fatalf("read bg_flags: %v", err)
			}
			flags := binary.LittleEndian.Uint16(gdBuf[:])
			if flags&uint16(blockGroupFlagInodeTableZeroed) == 0 {
				t.Errorf("group 0 bg_flags=%#x missing INODE_ZEROED (%#x)", flags, blockGroupFlagInodeTableZeroed)
			}
		})
	}
}
