package erofs

import (
	"encoding/binary"
	"testing"
)

func TestGetInodeErrors(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"reserved format", func(b []byte) { binary.LittleEndian.PutUint16(b[4128:], 1<<5) }},
		{"invalid mode", func(b []byte) { binary.LittleEndian.PutUint16(b[4132:], 0) }},
		{"inline compressed tail", func(b []byte) {
			binary.LittleEndian.PutUint16(b[4128:], uint16(dataLayoutCompressedCompact)<<1)
			binary.LittleEndian.PutUint16(b[4162:], 1)
			binary.LittleEndian.PutUint16(b[4164:], zAdviseInlineData)
		}},
		{"xattrs outside image", func(b []byte) { binary.LittleEndian.PutUint16(b[4130:], 10000) }},
		{"large inode number wraps", func(b []byte) {
			binary.LittleEndian.PutUint64(b[8192:], uint64(1)<<59|1)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			image := buildImage(false)
			tt.mutate(image)
			filesystem, err := Read(testBackend(t, image), int64(len(image)), 0, 0)
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			if _, err := filesystem.ReadFile("hello"); err == nil {
				t.Fatal("Read() error = nil, want error")
			}
		})
	}
}
