package erofs

import (
	"encoding/binary"
	"testing"
)

func TestInitCompressedFullFragmentHighOffset(t *testing.T) {
	image := make([]byte, 16384)
	binary.LittleEndian.PutUint32(image[4160:], 7)
	binary.LittleEndian.PutUint16(image[4164:], zAdviseFragment)

	indexes := []struct {
		offset        int
		physicalBlock uint32
	}{
		{offset: 4176, physicalBlock: 10},
		{offset: 4184, physicalBlock: 11},
		{offset: 4192, physicalBlock: 1},
	}
	for _, index := range indexes {
		binary.LittleEndian.PutUint16(image[index.offset:], uint16(zClusterTypeHead1))
		binary.LittleEndian.PutUint16(image[index.offset+2:], 0)
		binary.LittleEndian.PutUint32(image[index.offset+4:], index.physicalBlock)
	}

	filesystem := &FileSystem{
		backend:   testBackend(t, image),
		size:      int64(len(image)),
		blocksize: 4096,
		superblock: &superblock{
			blockSizeBits:    12,
			metaBlockAddress: 1,
		},
	}
	node := &inode{
		nid:        1,
		inodeSize:  inodeCompactSize,
		size:       12288,
		dataLayout: dataLayoutCompressedFull,
	}
	if err := filesystem.initCompressed(node); err != nil {
		t.Fatalf("initCompressed() error = %v", err)
	}
	want := int64(uint64(1)<<32 | 7)
	if node.compressed.fragmentOffset != want {
		t.Fatalf("fragmentOffset = %d, want %d", node.compressed.fragmentOffset, want)
	}
	if node.compressed.fragmentOffset < 0 {
		t.Fatalf("fragmentOffset = %d is negative", node.compressed.fragmentOffset)
	}
}
