package ext4

import "testing"

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
