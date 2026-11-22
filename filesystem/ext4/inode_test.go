package ext4

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"
)

func TestInodeFromBytesChecksum(t *testing.T) {
	tests := []struct {
		name              string
		metadataChecksums bool
		storedChecksum    bool
		modified          bool
		wantErr           bool
	}{
		{name: "disabled with zero checksum"},
		{name: "disabled with stale checksum", storedChecksum: true, modified: true},
		{name: "enabled with valid checksum", metadataChecksums: true, storedChecksum: true},
		{name: "enabled with zero checksum", metadataChecksums: true, wantErr: true},
		{name: "enabled with stale checksum", metadataChecksums: true, storedChecksum: true, modified: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := &superblock{
				inodeSize:    256,
				checksumSeed: 0x12345678,
				features: featureFlags{
					metadataChecksums: tt.metadataChecksums,
				},
			}
			data := make([]byte, sb.inodeSize)
			binary.LittleEndian.PutUint16(data[0x00:0x02], uint16(fileTypeRegularFile)|0o644)
			binary.LittleEndian.PutUint32(data[0x04:0x08], 42)
			binary.LittleEndian.PutUint32(data[0x64:0x68], 17)
			binary.LittleEndian.PutUint16(data[0x80:0x82], 32)

			if tt.storedChecksum {
				var prefix [8]byte
				binary.LittleEndian.PutUint32(prefix[:4], 12)
				binary.LittleEndian.PutUint32(prefix[4:], 17)
				// ext4 uses CRC32C without the standard library's initial/final complement.
				table := crc32.MakeTable(crc32.Castagnoli)
				checksum := crc32.Update(^sb.checksumSeed, table, prefix[:])
				checksum = ^crc32.Update(checksum, table, data)
				binary.LittleEndian.PutUint16(data[0x7c:0x7e], uint16(checksum))
				binary.LittleEndian.PutUint16(data[0x82:0x84], uint16(checksum>>16))
			}

			// Linux updates inode fields without maintaining checksums when metadata_csum is disabled.
			if tt.modified {
				binary.LittleEndian.PutUint32(data[0x04:0x08], 43)
			}

			in, err := inodeFromBytes(data, sb, 12)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
					t.Fatalf("expected checksum mismatch, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("inodeFromBytes: %v", err)
			}
			wantSize := uint64(42)
			if tt.modified {
				wantSize = 43
			}
			if in.size != wantSize {
				t.Errorf("size = %d, want %d", in.size, wantSize)
			}
		})
	}
}

func TestInodeDeviceNumber(t *testing.T) {
	tests := []struct {
		name          string
		fileType      fileType
		blockPointers [15]uint32
		wantMajor     uint32
		wantMinor     uint32
	}{
		{
			name:      "non-device returns zero",
			fileType:  fileTypeRegularFile,
			wantMajor: 0,
			wantMinor: 0,
		},
		{
			name:     "old-style char device",
			fileType: fileTypeCharacterDevice,
			// major=1, minor=3
			blockPointers: [15]uint32{(1 << 8) | 3},
			wantMajor:     1,
			wantMinor:     3,
		},
		{
			name:     "old-style block device",
			fileType: fileTypeBlockDevice,
			// major=8, minor=0
			blockPointers: [15]uint32{(8 << 8) | 0}, //nolint:staticcheck // (minor=0)
			wantMajor:     8,
			wantMinor:     0,
		},
		{
			name:     "new-style char device with large minor",
			fileType: fileTypeCharacterDevice,
			// major=136, minor=0
			blockPointers: [15]uint32{0, (0 & 0xff) | (136 << 8) | ((0 & ^uint32(0xff)) << 12)},
			wantMajor:     136,
			wantMinor:     0,
		},
		{
			name:     "new-style block device with large minor",
			fileType: fileTypeBlockDevice,
			// major=259, minor=1
			blockPointers: [15]uint32{0, (1 & 0xff) | (259 << 8) | ((1 & ^uint32(0xff)) << 12)},
			wantMajor:     259,
			wantMinor:     1,
		},
		{
			name:     "new-style with large minor number",
			fileType: fileTypeBlockDevice,
			// major=8, minor=256
			blockPointers: [15]uint32{0, (256 & 0xff) | (8 << 8) | ((256 & ^uint32(0xff)) << 12)},
			wantMajor:     8,
			wantMinor:     256,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := &inode{
				fileType:      tt.fileType,
				blockPointers: tt.blockPointers,
			}
			major, minor := in.deviceNumber()
			if major != tt.wantMajor || minor != tt.wantMinor {
				t.Errorf("deviceNumber() = (%d, %d), want (%d, %d)", major, minor, tt.wantMajor, tt.wantMinor)
			}
		})
	}
}
