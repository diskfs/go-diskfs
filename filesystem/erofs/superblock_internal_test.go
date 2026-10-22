package erofs

import (
	"encoding/binary"
	"testing"
)

func validSuperblockBytes() []byte {
	b := make([]byte, 148)
	binary.LittleEndian.PutUint32(b[0:4], superMagic)
	b[12] = 12
	binary.LittleEndian.PutUint16(b[14:16], 0)
	binary.LittleEndian.PutUint32(b[36:40], 2)
	binary.LittleEndian.PutUint32(b[40:44], 1)
	copy(b[64:80], []byte("test volume"))
	return b
}

func TestParseSuperblock(t *testing.T) {
	b := validSuperblockBytes()
	s, err := parseSuperblock(b, 8192)
	if err != nil {
		t.Fatalf("parseSuperblock() error = %v", err)
	}
	if s.blockSize() != 4096 {
		t.Errorf("blockSize() = %d, want 4096", s.blockSize())
	}
	if s.rootNID != 0 || s.metaBlockAddress != 1 {
		t.Errorf("unexpected root location: nid=%d meta=%d", s.rootNID, s.metaBlockAddress)
	}
}

func TestParseSuperblockErrors(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"magic", func(b []byte) { binary.LittleEndian.PutUint32(b[0:4], 0) }},
		{"block size", func(b []byte) { b[12] = 8 }},
		{"compat feature", func(b []byte) { binary.LittleEndian.PutUint32(b[8:12], 1<<3) }},
		{"incompat feature", func(b []byte) { binary.LittleEndian.PutUint32(b[80:84], 1<<2) }},
		{"extra devices", func(b []byte) { binary.LittleEndian.PutUint16(b[86:88], 1) }},
		{"directory blocks", func(b []byte) { b[90] = 1 }},
		{"xattr prefixes", func(b []byte) { b[91] = 1 }},
		{"shared xattr prefixes", func(b []byte) { b[105] = 1 }},
		{"reserved", func(b []byte) { b[106] = 1 }},
		{"48-bit", func(b []byte) { binary.LittleEndian.PutUint64(b[112:120], 1) }},
		{"metabox", func(b []byte) {
			b[13] = 1
			binary.LittleEndian.PutUint64(b[128:136], 1)
		}},
		{"root outside image", func(b []byte) { binary.LittleEndian.PutUint16(b[14:16], 1000) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := validSuperblockBytes()
			tt.mutate(b)
			if _, err := parseSuperblock(b, 8192); err == nil {
				t.Fatal("parseSuperblock() error = nil, want error")
			}
		})
	}
}

func TestSuperblockChecksum(t *testing.T) {
	b := make([]byte, 3072)
	copy(b, validSuperblockBytes())
	binary.LittleEndian.PutUint32(b[8:12], featureCompatChecksum)
	binary.LittleEndian.PutUint32(b[4:8], superblockChecksum(b[:3072]))
	s, err := parseSuperblock(b, 8192)
	if err != nil {
		t.Fatalf("parseSuperblock() error = %v", err)
	}
	if err := verifySuperblockChecksum(s, b); err != nil {
		t.Fatalf("verifySuperblockChecksum() error = %v", err)
	}
	b[100] ^= 1
	if err := verifySuperblockChecksum(s, b); err == nil {
		t.Fatal("verifySuperblockChecksum() error = nil after corruption")
	}
}
