package ext4

import (
	"encoding/binary"
	"testing"
)

// TestExtentNodeHeaderToBytes tests serialization of the extent node header
func TestExtentNodeHeaderToBytes(t *testing.T) {
	tests := []struct {
		name    string
		header  extentNodeHeader
		entries uint16
		max     uint16
		depth   uint16
	}{
		{"leaf root", extentNodeHeader{depth: 0, entries: 2, max: 4, blockSize: 4096}, 2, 4, 0},
		{"leaf non-root", extentNodeHeader{depth: 0, entries: 10, max: 340, blockSize: 4096}, 10, 340, 0},
		{"internal depth 1", extentNodeHeader{depth: 1, entries: 3, max: 4, blockSize: 4096}, 3, 4, 1},
		{"internal depth 5", extentNodeHeader{depth: 5, entries: 1, max: 340, blockSize: 4096}, 1, 340, 5},
		{"zero entries", extentNodeHeader{depth: 0, entries: 0, max: 4, blockSize: 4096}, 0, 4, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.header.toBytes()
			if len(b) != extentTreeHeaderLength {
				t.Fatalf("expected %d bytes, got %d", extentTreeHeaderLength, len(b))
			}
			// Check magic signature
			sig := binary.LittleEndian.Uint16(b[0:2])
			if sig != extentHeaderSignature {
				t.Errorf("expected magic 0x%04x, got 0x%04x", extentHeaderSignature, sig)
			}
			// Check entries
			entries := binary.LittleEndian.Uint16(b[2:4])
			if entries != tt.entries {
				t.Errorf("expected entries %d, got %d", tt.entries, entries)
			}
			// Check max
			max := binary.LittleEndian.Uint16(b[4:6])
			if max != tt.max {
				t.Errorf("expected max %d, got %d", tt.max, max)
			}
			// Check depth
			depth := binary.LittleEndian.Uint16(b[6:8])
			if depth != tt.depth {
				t.Errorf("expected depth %d, got %d", tt.depth, depth)
			}
		})
	}
}

// TestExtentLeafNodeToBytes tests serialization of leaf nodes
func TestExtentLeafNodeToBytes(t *testing.T) {
	leaf := extentLeafNode{
		extentNodeHeader: extentNodeHeader{
			depth:     0,
			entries:   2,
			max:       4,
			blockSize: 4096,
		},
		extents: extents{
			{fileBlock: 0, startingBlock: 100, count: 5},
			{fileBlock: 5, startingBlock: 200, count: 10},
		},
	}

	b := leaf.toBytes()
	expectedLen := 12 + 12*int(leaf.max)
	if len(b) != expectedLen {
		t.Fatalf("expected %d bytes, got %d", expectedLen, len(b))
	}

	// Verify header magic
	sig := binary.LittleEndian.Uint16(b[0:2])
	if sig != extentHeaderSignature {
		t.Errorf("expected magic 0x%04x, got 0x%04x", extentHeaderSignature, sig)
	}

	// Verify first extent entry at offset 12
	fileBlock0 := binary.LittleEndian.Uint32(b[12:16])
	if fileBlock0 != 0 {
		t.Errorf("expected first extent fileBlock 0, got %d", fileBlock0)
	}
	count0 := binary.LittleEndian.Uint16(b[16:18])
	if count0 != 5 {
		t.Errorf("expected first extent count 5, got %d", count0)
	}

	// Verify second extent entry at offset 24
	fileBlock1 := binary.LittleEndian.Uint32(b[24:28])
	if fileBlock1 != 5 {
		t.Errorf("expected second extent fileBlock 5, got %d", fileBlock1)
	}
	count1 := binary.LittleEndian.Uint16(b[28:30])
	if count1 != 10 {
		t.Errorf("expected second extent count 10, got %d", count1)
	}
}

// TestExtentLeafNodeToBytesRoundTrip tests that serialization followed by parsing
// yields the same extents back
func TestExtentLeafNodeToBytesRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		extents extents
		max     uint16
	}{
		{
			"single extent",
			extents{{fileBlock: 0, startingBlock: 100, count: 10}},
			4,
		},
		{
			"multiple extents",
			extents{
				{fileBlock: 0, startingBlock: 100, count: 5},
				{fileBlock: 5, startingBlock: 200, count: 10},
				{fileBlock: 15, startingBlock: 500, count: 1},
			},
			4,
		},
		{
			"high disk block",
			extents{{fileBlock: 0, startingBlock: 0x1FFFFFFFFFF, count: 3}},
			4,
		},
		{
			"max count",
			extents{{fileBlock: 0, startingBlock: 50, count: 0x7FFF}},
			4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leaf := extentLeafNode{
				extentNodeHeader: extentNodeHeader{
					depth:     0,
					entries:   uint16(len(tt.extents)),
					max:       tt.max,
					blockSize: 4096,
				},
				extents: tt.extents,
			}

			b := leaf.toBytes()

			// Parse it back
			parsed, err := parseExtents(b, 4096, 0, 10000)
			if err != nil {
				t.Fatalf("parseExtents failed: %v", err)
			}

			parsedLeaf, ok := parsed.(*extentLeafNode)
			if !ok {
				t.Fatalf("expected *extentLeafNode, got %T", parsed)
			}

			if len(parsedLeaf.extents) != len(tt.extents) {
				t.Fatalf("expected %d extents, got %d", len(tt.extents), len(parsedLeaf.extents))
			}

			for i, ext := range parsedLeaf.extents {
				if ext.fileBlock != tt.extents[i].fileBlock {
					t.Errorf("extent[%d] fileBlock: expected %d, got %d", i, tt.extents[i].fileBlock, ext.fileBlock)
				}
				if ext.startingBlock != tt.extents[i].startingBlock {
					t.Errorf("extent[%d] startingBlock: expected %d, got %d", i, tt.extents[i].startingBlock, ext.startingBlock)
				}
				if ext.count != tt.extents[i].count {
					t.Errorf("extent[%d] count: expected %d, got %d", i, tt.extents[i].count, ext.count)
				}
			}
		})
	}
}

// TestExtentInternalNodeToBytes tests serialization of internal nodes
func TestExtentInternalNodeToBytes(t *testing.T) {
	node := extentInternalNode{
		extentNodeHeader: extentNodeHeader{
			depth:     1,
			entries:   2,
			max:       4,
			blockSize: 4096,
		},
		children: []*extentChildPtr{
			{fileBlock: 0, count: 100, diskBlock: 50},
			{fileBlock: 100, count: 200, diskBlock: 51},
		},
	}

	b := node.toBytes()
	expectedLen := 12 + 12*int(node.max)
	if len(b) != expectedLen {
		t.Fatalf("expected %d bytes, got %d", expectedLen, len(b))
	}

	// Verify header
	sig := binary.LittleEndian.Uint16(b[0:2])
	if sig != extentHeaderSignature {
		t.Errorf("expected magic 0x%04x, got 0x%04x", extentHeaderSignature, sig)
	}
	depth := binary.LittleEndian.Uint16(b[6:8])
	if depth != 1 {
		t.Errorf("expected depth 1, got %d", depth)
	}

	// Verify first child pointer at offset 12
	fileBlock0 := binary.LittleEndian.Uint32(b[12:16])
	if fileBlock0 != 0 {
		t.Errorf("expected first child fileBlock 0, got %d", fileBlock0)
	}
}

// TestParseExtentsTooSmall tests that parseExtents rejects inputs that are too small
func TestParseExtentsTooSmall(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"just header", make([]byte, 11)},
		{"header no entry", make([]byte, 12)},
		{"just under minimum", make([]byte, 23)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseExtents(tt.data, 4096, 0, 100)
			if err == nil {
				t.Errorf("expected error for %d-byte input, got nil", len(tt.data))
			}
		})
	}
}

// TestParseExtentsInvalidMagic tests that parseExtents rejects data with wrong magic signature
func TestParseExtentsInvalidMagic(t *testing.T) {
	b := make([]byte, 24)
	// Set wrong magic
	binary.LittleEndian.PutUint16(b[0:2], 0xBEEF)
	binary.LittleEndian.PutUint16(b[2:4], 1) // entries
	binary.LittleEndian.PutUint16(b[4:6], 4) // max
	binary.LittleEndian.PutUint16(b[6:8], 0) // depth

	_, err := parseExtents(b, 4096, 0, 100)
	if err == nil {
		t.Errorf("expected error for invalid magic, got nil")
	}
}

// TestParseExtentsLeafNode tests parsing a well-formed leaf node
func TestParseExtentsLeafNode(t *testing.T) {
	// Build a valid leaf node with 2 extents
	b := make([]byte, 36) // 12 header + 12*2 entries
	binary.LittleEndian.PutUint16(b[0:2], extentHeaderSignature)
	binary.LittleEndian.PutUint16(b[2:4], 2) // entries
	binary.LittleEndian.PutUint16(b[4:6], 4) // max
	binary.LittleEndian.PutUint16(b[6:8], 0) // depth = 0 (leaf)

	// First extent: fileBlock=0, count=5, startingBlock=100
	binary.LittleEndian.PutUint32(b[12:16], 0)  // fileBlock
	binary.LittleEndian.PutUint16(b[16:18], 5)   // count
	binary.LittleEndian.PutUint16(b[18:20], 0)   // startingBlock high 16
	binary.LittleEndian.PutUint32(b[20:24], 100) // startingBlock low 32

	// Second extent: fileBlock=5, count=10, startingBlock=200
	binary.LittleEndian.PutUint32(b[24:28], 5)   // fileBlock
	binary.LittleEndian.PutUint16(b[28:30], 10)  // count
	binary.LittleEndian.PutUint16(b[30:32], 0)   // startingBlock high 16
	binary.LittleEndian.PutUint32(b[32:36], 200) // startingBlock low 32

	result, err := parseExtents(b, 4096, 0, 15)
	if err != nil {
		t.Fatalf("parseExtents failed: %v", err)
	}

	leaf, ok := result.(*extentLeafNode)
	if !ok {
		t.Fatalf("expected *extentLeafNode, got %T", result)
	}

	if len(leaf.extents) != 2 {
		t.Fatalf("expected 2 extents, got %d", len(leaf.extents))
	}

	if leaf.extents[0].fileBlock != 0 || leaf.extents[0].count != 5 || leaf.extents[0].startingBlock != 100 {
		t.Errorf("first extent mismatch: %+v", leaf.extents[0])
	}
	if leaf.extents[1].fileBlock != 5 || leaf.extents[1].count != 10 || leaf.extents[1].startingBlock != 200 {
		t.Errorf("second extent mismatch: %+v", leaf.extents[1])
	}
}

// TestParseExtentsInternalNode tests parsing a well-formed internal node
func TestParseExtentsInternalNode(t *testing.T) {
	// Build a valid internal node with 2 children
	b := make([]byte, 36) // 12 header + 12*2 entries
	binary.LittleEndian.PutUint16(b[0:2], extentHeaderSignature)
	binary.LittleEndian.PutUint16(b[2:4], 2) // entries
	binary.LittleEndian.PutUint16(b[4:6], 4) // max
	binary.LittleEndian.PutUint16(b[6:8], 1) // depth = 1 (internal)

	// First child: fileBlock=0, diskBlock=50
	binary.LittleEndian.PutUint32(b[12:16], 0)  // fileBlock
	binary.LittleEndian.PutUint32(b[16:20], 50) // diskBlock low 32
	binary.LittleEndian.PutUint16(b[20:22], 0)  // diskBlock high 16

	// Second child: fileBlock=100, diskBlock=60
	binary.LittleEndian.PutUint32(b[24:28], 100) // fileBlock
	binary.LittleEndian.PutUint32(b[28:32], 60)  // diskBlock low 32
	binary.LittleEndian.PutUint16(b[32:34], 0)   // diskBlock high 16

	result, err := parseExtents(b, 4096, 0, 200)
	if err != nil {
		t.Fatalf("parseExtents failed: %v", err)
	}

	internal, ok := result.(*extentInternalNode)
	if !ok {
		t.Fatalf("expected *extentInternalNode, got %T", result)
	}

	if len(internal.children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(internal.children))
	}

	if internal.children[0].fileBlock != 0 || internal.children[0].diskBlock != 50 {
		t.Errorf("first child mismatch: fileBlock=%d diskBlock=%d", internal.children[0].fileBlock, internal.children[0].diskBlock)
	}
	if internal.children[1].fileBlock != 100 || internal.children[1].diskBlock != 60 {
		t.Errorf("second child mismatch: fileBlock=%d diskBlock=%d", internal.children[1].fileBlock, internal.children[1].diskBlock)
	}

	// Verify the count of the first child was computed from the second child's fileBlock
	if internal.children[0].count != 100 {
		t.Errorf("first child count: expected 100, got %d", internal.children[0].count)
	}
}

// TestExtentLeafNodeFindBlocks tests the findBlocks method on leaf nodes
func TestExtentLeafNodeFindBlocks(t *testing.T) {
	leaf := &extentLeafNode{
		extentNodeHeader: extentNodeHeader{
			depth:     0,
			entries:   3,
			max:       4,
			blockSize: 4096,
		},
		extents: extents{
			{fileBlock: 0, startingBlock: 100, count: 5},    // file blocks 0-4 -> disk 100-104
			{fileBlock: 5, startingBlock: 200, count: 3},    // file blocks 5-7 -> disk 200-202
			{fileBlock: 10, startingBlock: 500, count: 10},  // file blocks 10-19 -> disk 500-509
		},
	}

	tests := []struct {
		name     string
		start    uint64
		count    uint64
		expected []uint64
	}{
		{"first extent all", 0, 5, []uint64{100, 101, 102, 103, 104}},
		{"first extent partial", 1, 3, []uint64{101, 102, 103}},
		{"second extent all", 5, 3, []uint64{200, 201, 202}},
		{"span first and second", 3, 5, []uint64{103, 104, 200, 201, 202}},
		{"third extent partial", 12, 3, []uint64{502, 503, 504}},
		{"single block", 0, 1, []uint64{100}},
		{"gap region", 8, 1, nil}, // blocks 8-9 are not covered by any extent
		{"span gap and third", 8, 5, []uint64{500, 501, 502}}, // 8,9 are gap, 10-12 are in third extent
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blocks, err := leaf.findBlocks(tt.start, tt.count, nil)
			if err != nil {
				t.Fatalf("findBlocks error: %v", err)
			}
			if len(blocks) != len(tt.expected) {
				t.Fatalf("expected %d blocks, got %d: %v", len(tt.expected), len(blocks), blocks)
			}
			for i, b := range blocks {
				if b != tt.expected[i] {
					t.Errorf("block[%d]: expected %d, got %d", i, tt.expected[i], b)
				}
			}
		})
	}
}

// TestExtentLeafNodeBlocks tests the blocks() method that returns all extents
func TestExtentLeafNodeBlocks(t *testing.T) {
	original := extents{
		{fileBlock: 0, startingBlock: 100, count: 5},
		{fileBlock: 5, startingBlock: 200, count: 3},
	}
	leaf := &extentLeafNode{
		extentNodeHeader: extentNodeHeader{depth: 0, entries: 2, max: 4, blockSize: 4096},
		extents:          original,
	}

	result, err := leaf.blocks(nil)
	if err != nil {
		t.Fatalf("blocks() error: %v", err)
	}

	if len(result) != len(original) {
		t.Fatalf("expected %d extents, got %d", len(original), len(result))
	}

	for i, ext := range result {
		if ext != original[i] {
			t.Errorf("extent[%d]: expected %+v, got %+v", i, original[i], ext)
		}
	}
}

// TestExtentEqual tests the extent.equal method
func TestExtentEqual(t *testing.T) {
	a := &extent{fileBlock: 0, startingBlock: 100, count: 5}
	b := &extent{fileBlock: 0, startingBlock: 100, count: 5}
	c := &extent{fileBlock: 1, startingBlock: 100, count: 5}

	if !a.equal(b) {
		t.Errorf("expected equal extents to be equal")
	}
	if a.equal(c) {
		t.Errorf("expected different extents to be not equal")
	}
	if a.equal(nil) {
		t.Errorf("expected non-nil != nil")
	}

	var nilExt *extent
	if !nilExt.equal(nil) {
		t.Errorf("expected nil == nil")
	}
	if nilExt.equal(a) {
		t.Errorf("expected nil != non-nil")
	}
}

// TestExtentsBlockCount tests the blockCount method
func TestExtentsBlockCount(t *testing.T) {
	tests := []struct {
		name     string
		exts     extents
		expected uint64
	}{
		{"empty", extents{}, 0},
		{"single", extents{{fileBlock: 0, startingBlock: 10, count: 5}}, 5},
		{"multiple", extents{
			{fileBlock: 0, startingBlock: 10, count: 5},
			{fileBlock: 5, startingBlock: 20, count: 3},
			{fileBlock: 8, startingBlock: 30, count: 10},
		}, 18},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.exts.blockCount()
			if result != tt.expected {
				t.Errorf("expected blockCount %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestExtentLeafNodeGetters tests the getter methods on leaf nodes
func TestExtentLeafNodeGetters(t *testing.T) {
	leaf := &extentLeafNode{
		extentNodeHeader: extentNodeHeader{depth: 0, entries: 2, max: 4, blockSize: 4096},
		extents: extents{
			{fileBlock: 10, startingBlock: 100, count: 5},
			{fileBlock: 15, startingBlock: 200, count: 3},
		},
	}

	if leaf.getDepth() != 0 {
		t.Errorf("expected depth 0, got %d", leaf.getDepth())
	}
	if leaf.getMax() != 4 {
		t.Errorf("expected max 4, got %d", leaf.getMax())
	}
	if leaf.getBlockSize() != 4096 {
		t.Errorf("expected blockSize 4096, got %d", leaf.getBlockSize())
	}
	if leaf.getFileBlock() != 10 {
		t.Errorf("expected fileBlock 10, got %d", leaf.getFileBlock())
	}
	if leaf.getCount() != 2 {
		t.Errorf("expected count 2, got %d", leaf.getCount())
	}
}

// TestExtentInternalNodeGetters tests the getter methods on internal nodes
func TestExtentInternalNodeGetters(t *testing.T) {
	node := &extentInternalNode{
		extentNodeHeader: extentNodeHeader{depth: 2, entries: 3, max: 340, blockSize: 4096},
		children: []*extentChildPtr{
			{fileBlock: 0, count: 100, diskBlock: 50},
			{fileBlock: 100, count: 200, diskBlock: 51},
			{fileBlock: 300, count: 100, diskBlock: 52},
		},
	}

	if node.getDepth() != 2 {
		t.Errorf("expected depth 2, got %d", node.getDepth())
	}
	if node.getMax() != 340 {
		t.Errorf("expected max 340, got %d", node.getMax())
	}
	if node.getBlockSize() != 4096 {
		t.Errorf("expected blockSize 4096, got %d", node.getBlockSize())
	}
	if node.getFileBlock() != 0 {
		t.Errorf("expected fileBlock 0, got %d", node.getFileBlock())
	}
	if node.getCount() != 3 {
		t.Errorf("expected count 3, got %d", node.getCount())
	}
}

// TestGetDiskBlockFromNode tests retrieving disk block from both node types
func TestGetDiskBlockFromNode(t *testing.T) {
	leaf := &extentLeafNode{
		extentNodeHeader: extentNodeHeader{depth: 0, entries: 1, max: 4, blockSize: 4096},
		extents:          extents{{fileBlock: 0, startingBlock: 100, count: 5}},
		diskBlock:        42,
	}
	if getDiskBlockFromNode(leaf) != 42 {
		t.Errorf("expected diskBlock 42 for leaf, got %d", getDiskBlockFromNode(leaf))
	}

	internal := &extentInternalNode{
		extentNodeHeader: extentNodeHeader{depth: 1, entries: 1, max: 4, blockSize: 4096},
		children:         []*extentChildPtr{{fileBlock: 0, count: 10, diskBlock: 50}},
		diskBlock:        99,
	}
	if getDiskBlockFromNode(internal) != 99 {
		t.Errorf("expected diskBlock 99 for internal, got %d", getDiskBlockFromNode(internal))
	}
}

// TestCreateRootExtentTree tests creation of a new root extent tree
func TestCreateRootExtentTree(t *testing.T) {
	// We can't test extendExtentTree directly without a full filesystem,
	// but we can test createRootExtentTree which doesn't need disk I/O
	// if the extents fit in 4 entries.

	t.Run("fits in root", func(t *testing.T) {
		exts := &extents{
			{fileBlock: 0, startingBlock: 100, count: 5},
			{fileBlock: 5, startingBlock: 200, count: 3},
		}
		fs := &FileSystem{
			superblock: &superblock{blockSize: 4096},
		}
		result, err := createRootExtentTree(exts, fs)
		if err != nil {
			t.Fatalf("createRootExtentTree failed: %v", err)
		}
		leaf, ok := result.(*extentLeafNode)
		if !ok {
			t.Fatalf("expected *extentLeafNode, got %T", result)
		}
		if len(leaf.extents) != 2 {
			t.Errorf("expected 2 extents, got %d", len(leaf.extents))
		}
		if leaf.max != 4 {
			t.Errorf("expected max 4 (root inode), got %d", leaf.max)
		}
		if leaf.depth != 0 {
			t.Errorf("expected depth 0, got %d", leaf.depth)
		}
	})

	t.Run("exactly 4 extents", func(t *testing.T) {
		exts := &extents{
			{fileBlock: 0, startingBlock: 10, count: 5},
			{fileBlock: 5, startingBlock: 20, count: 5},
			{fileBlock: 10, startingBlock: 30, count: 5},
			{fileBlock: 15, startingBlock: 40, count: 5},
		}
		fs := &FileSystem{
			superblock: &superblock{blockSize: 4096},
		}
		result, err := createRootExtentTree(exts, fs)
		if err != nil {
			t.Fatalf("createRootExtentTree failed: %v", err)
		}
		leaf, ok := result.(*extentLeafNode)
		if !ok {
			t.Fatalf("expected *extentLeafNode, got %T", result)
		}
		if len(leaf.extents) != 4 {
			t.Errorf("expected 4 extents, got %d", len(leaf.extents))
		}
	})

	t.Run("too many for root", func(t *testing.T) {
		exts := &extents{
			{fileBlock: 0, startingBlock: 10, count: 1},
			{fileBlock: 1, startingBlock: 20, count: 1},
			{fileBlock: 2, startingBlock: 30, count: 1},
			{fileBlock: 3, startingBlock: 40, count: 1},
			{fileBlock: 4, startingBlock: 50, count: 1},
		}
		fs := &FileSystem{
			superblock: &superblock{blockSize: 4096},
		}
		_, err := createRootExtentTree(exts, fs)
		if err == nil {
			t.Errorf("expected error when too many extents for root, got nil")
		}
	})
}

// TestExtentsBlockFinderFromExtents tests the convenience constructor
func TestExtentsBlockFinderFromExtents(t *testing.T) {
	exts := extents{
		{fileBlock: 0, startingBlock: 100, count: 5},
		{fileBlock: 5, startingBlock: 200, count: 3},
	}

	result := extentsBlockFinderFromExtents(exts, 4096)
	leaf, ok := result.(*extentLeafNode)
	if !ok {
		t.Fatalf("expected *extentLeafNode, got %T", result)
	}
	if len(leaf.extents) != 2 {
		t.Errorf("expected 2 extents, got %d", len(leaf.extents))
	}
	if leaf.max != 4 {
		t.Errorf("expected max 4, got %d", leaf.max)
	}
	if leaf.blockSize != 4096 {
		t.Errorf("expected blockSize 4096, got %d", leaf.blockSize)
	}
}

// TestInternalNodeToBytesRoundTrip tests that internal node serialization/parsing round-trips
func TestInternalNodeToBytesRoundTrip(t *testing.T) {
	node := &extentInternalNode{
		extentNodeHeader: extentNodeHeader{
			depth:     1,
			entries:   2,
			max:       4,
			blockSize: 4096,
		},
		children: []*extentChildPtr{
			{fileBlock: 0, count: 100, diskBlock: 50},
			{fileBlock: 100, count: 200, diskBlock: 60},
		},
	}

	b := node.toBytes()

	// Parse it back
	parsed, err := parseExtents(b, 4096, 0, 300)
	if err != nil {
		t.Fatalf("parseExtents failed: %v", err)
	}

	internal, ok := parsed.(*extentInternalNode)
	if !ok {
		t.Fatalf("expected *extentInternalNode, got %T", parsed)
	}

	if internal.depth != 1 {
		t.Errorf("expected depth 1, got %d", internal.depth)
	}
	if len(internal.children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(internal.children))
	}

	if internal.children[0].fileBlock != 0 {
		t.Errorf("first child fileBlock: expected 0, got %d", internal.children[0].fileBlock)
	}
	if internal.children[0].diskBlock != 50 {
		t.Errorf("first child diskBlock: expected 50, got %d", internal.children[0].diskBlock)
	}
	if internal.children[1].fileBlock != 100 {
		t.Errorf("second child fileBlock: expected 100, got %d", internal.children[1].fileBlock)
	}
	if internal.children[1].diskBlock != 60 {
		t.Errorf("second child diskBlock: expected 60, got %d", internal.children[1].diskBlock)
	}
}

// TestParseExtentsHighDiskBlock tests that high disk block numbers (48-bit) are correctly parsed
func TestParseExtentsHighDiskBlock(t *testing.T) {
	// Build a leaf node with a high disk block number that uses the upper 16 bits
	b := make([]byte, 24) // 12 header + 12 entry
	binary.LittleEndian.PutUint16(b[0:2], extentHeaderSignature)
	binary.LittleEndian.PutUint16(b[2:4], 1) // entries
	binary.LittleEndian.PutUint16(b[4:6], 4) // max
	binary.LittleEndian.PutUint16(b[6:8], 0) // depth = 0 (leaf)

	// Extent: fileBlock=0, count=1, startingBlock=0x0001_0000_0064 (high bits = 1, low = 100)
	binary.LittleEndian.PutUint32(b[12:16], 0)   // fileBlock
	binary.LittleEndian.PutUint16(b[16:18], 1)    // count
	binary.LittleEndian.PutUint16(b[18:20], 1)    // startingBlock high 16 bits
	binary.LittleEndian.PutUint32(b[20:24], 100)  // startingBlock low 32 bits

	result, err := parseExtents(b, 4096, 0, 1)
	if err != nil {
		t.Fatalf("parseExtents failed: %v", err)
	}

	leaf, ok := result.(*extentLeafNode)
	if !ok {
		t.Fatalf("expected *extentLeafNode, got %T", result)
	}

	expected := uint64(0x100000064) // 1<<32 + 100
	if leaf.extents[0].startingBlock != expected {
		t.Errorf("expected startingBlock 0x%x, got 0x%x", expected, leaf.extents[0].startingBlock)
	}
}

// TestChildPtrMatchesNode tests the helper for matching child pointers to nodes
func TestChildPtrMatchesNode(t *testing.T) {
	leaf := &extentLeafNode{
		extentNodeHeader: extentNodeHeader{depth: 0, entries: 1, max: 4, blockSize: 4096},
		extents:          extents{{fileBlock: 10, startingBlock: 100, count: 5}},
	}

	matchingPtr := &extentChildPtr{fileBlock: 10, count: 5, diskBlock: 50}
	nonMatchingPtr := &extentChildPtr{fileBlock: 20, count: 5, diskBlock: 60}

	if !childPtrMatchesNode(matchingPtr, leaf) {
		t.Errorf("expected matching ptr to match leaf node")
	}
	if childPtrMatchesNode(nonMatchingPtr, leaf) {
		t.Errorf("expected non-matching ptr to not match leaf node")
	}
}
