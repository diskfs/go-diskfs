package ext4

import (
	"encoding/binary"
	"fmt"
)

const (
	extentTreeHeaderLength int    = 12
	extentTreeEntryLength  int    = 12
	extentHeaderSignature  uint16 = 0xf30a
	extentTreeMaxDepth     int    = 5
)

// extens a structure holding multiple extents
type extents []extent

// extent a structure with information about a single contiguous run of blocks containing file data
type extent struct {
	// fileBlock block number relative to the file. E.g. if the file is composed of 5 blocks, this could be 0-4
	fileBlock uint32
	// startingBlock the first block on disk that contains the data in this extent. E.g. if the file is made up of data from blocks 100-104 on the disk, this would be 100
	startingBlock uint64
	// count how many contiguous blocks are covered by this extent
	count uint16
}

// equal if 2 extents are equal
//
//nolint:unused // useful function for future
func (e *extent) equal(a *extent) bool {
	if (e == nil && a != nil) || (a == nil && e != nil) {
		return false
	}
	if e == nil && a == nil {
		return true
	}
	return *e == *a
}

// blocks how many blocks are covered in the extents
//
//nolint:unused // useful function for future
func (e extents) blocks() uint64 {
	var count uint64
	for _, ext := range e {
		count += uint64(ext.count)
	}
	return count
}

// extentBlockFinder provides a way of finding the blocks on disk that represent the block range of a given file.
// Arguments are the starting and ending blocks in the file. Returns a slice of blocks to read on disk.
// These blocks are in order. For example, if you ask to read file blocks starting at 20 for a count of 25, then you might
// get a single fileToBlocks{block: 100, count: 25} if the file is contiguous on disk. Or you might get
// fileToBlocks{block: 100, count: 10}, fileToBlocks{block: 200, count: 15} if the file is fragmented on disk.
// The slice should be read in order.
type extentBlockFinder interface {
	// findBlocks find the actual blocks for a range in the file, given the start block in the file and how many blocks
	findBlocks(start, count uint64, fs *FileSystem) ([]uint64, error)
	// blocks get all of the blocks for a file, in sequential order, essentially unravels the tree into a slice of extents
	blocks(fs *FileSystem) (extents, error)
	// toBytes convert this extentBlockFinder to bytes to be stored in a block or inode
	toBytes() []byte
}

var (
	_ extentBlockFinder = &extentInternalNode{}
	_ extentBlockFinder = &extentLeafNode{}
)

// extentNodeHeader represents the header of an extent node
type extentNodeHeader struct {
	depth     uint16 // the depth of tree below here; for leaf nodes, will be 0
	entries   uint16 // number of entries
	max       uint16 // maximum number of entries allowed at this level
	blockSize uint32 // block size for this tree
}

func (e extentNodeHeader) toBytes() []byte {
	b := make([]byte, 12)
	binary.LittleEndian.PutUint16(b[0:2], extentHeaderSignature)
	binary.LittleEndian.PutUint16(b[2:4], e.entries)
	binary.LittleEndian.PutUint16(b[4:6], e.max)
	binary.LittleEndian.PutUint16(b[6:8], e.depth)
	return b
}

// extentChildPtr represents a child pointer in an internal node of extents
// the child could be a leaf node or another internal node. We only would know
// after parsing diskBlock to see its header.
type extentChildPtr struct {
	fileBlock uint32 // extents or children of this cover from file block fileBlock onwards
	count     uint32 // how many blocks are covered by this extent
	diskBlock uint64 // block number where the children live
}

// extentLeafNode represents a leaf node of extents
// it includes the information in the header and the extents (leaf nodes).
// By definition, this is a leaf node, so depth=0
type extentLeafNode struct {
	extentNodeHeader
	extents extents // the actual extents
}

// findBlocks find the actual blocks for a range in the file. leaf nodes already have all of the data inside,
// so the FileSystem reference is unused.
func (e extentLeafNode) findBlocks(start, count uint64, _ *FileSystem) ([]uint64, error) {
	var ret []uint64

	// before anything, figure out which file block is the start and end of the desired range
	end := start + count - 1

	// we are at the bottom of the tree, so we can just return the extents
	for _, ext := range e.extents {
		extentStart := uint64(ext.fileBlock)
		extentEnd := uint64(ext.fileBlock + uint32(ext.count) - 1)

		// Check if the extent does not overlap with the given block range
		if extentEnd < start || extentStart > end {
			continue
		}

		// Calculate the overlapping range
		overlapStart := max(start, extentStart)
		overlapEnd := min(end, extentEnd)

		// Calculate the starting disk block for the overlap
		diskBlockStart := ext.startingBlock + (overlapStart - extentStart)

		// Append the corresponding disk blocks to the result
		for i := uint64(0); i <= overlapEnd-overlapStart; i++ {
			ret = append(ret, diskBlockStart+i)
		}
	}
	return ret, nil
}

// blocks find the actual blocks for a range in the file. leaf nodes already have all of the data inside,
// so the FileSystem reference is unused.
func (e extentLeafNode) blocks(_ *FileSystem) (extents, error) {
	return e.extents[:], nil
}

// toBytes convert the node to raw bytes to be stored, either in a block or in an inode
func (e extentLeafNode) toBytes() []byte {
	// 12 byte header, 12 bytes per child
	b := make([]byte, 12+12*e.max)
	copy(b[0:12], e.extentNodeHeader.toBytes())

	for i, ext := range e.extents {
		base := (i + 1) * 12
		binary.LittleEndian.PutUint32(b[base:base+4], ext.fileBlock)
		binary.LittleEndian.PutUint16(b[base+4:base+6], ext.count)
		diskBlock := make([]byte, 8)
		binary.LittleEndian.PutUint64(diskBlock, ext.startingBlock)
		copy(b[base+6:base+8], diskBlock[4:6])
		copy(b[base+8:base+12], diskBlock[0:4])
	}
	return b
}

// extentInternalNode represents an internal node in a tree of extents
// it includes the information in the header and the internal nodes
// By definition, this is an internal node, so depth>0
type extentInternalNode struct {
	extentNodeHeader
	children []*extentChildPtr // the children
}

// findBlocks find the actual blocks for a range in the file. internal nodes need to read the filesystem to
// get the child nodes, so the FileSystem reference is used.
func (e extentInternalNode) findBlocks(start, count uint64, fs *FileSystem) ([]uint64, error) {
	var ret []uint64

	// before anything, figure out which file block is the start and end of the desired range
	end := start + count - 1

	// we are not depth 0, so we have children extent tree nodes. Figure out which ranges we are in.
	// the hard part here is that each child has start but not end or count. You only know it from reading the next one.
	// So if the one we are looking at is in the range, we get it from the children, and keep going
	for _, child := range e.children {
		extentStart := uint64(child.fileBlock)
		extentEnd := uint64(child.fileBlock + child.count - 1)

		// Check if the extent does not overlap with the given block range
		if extentEnd < start || extentStart > end {
			continue
		}

		// read the extent block from the disk
		b, err := fs.readBlock(child.diskBlock)
		if err != nil {
			return nil, err
		}
		ebf, err := parseExtents(b, e.blockSize, uint32(extentStart), uint32(extentEnd))
		if err != nil {
			return nil, err
		}
		blocks, err := ebf.findBlocks(extentStart, uint64(child.count), fs)
		if err != nil {
			return nil, err
		}
		if len(blocks) > 0 {
			ret = append(ret, blocks...)
		}
	}
	return ret, nil
}

// blocks find the actual blocks for a range in the file. leaf nodes already have all of the data inside,
// so the FileSystem reference is unused.
func (e extentInternalNode) blocks(fs *FileSystem) (extents, error) {
	var ret extents

	// we are not depth 0, so we have children extent tree nodes. Walk the tree below us and find all of the blocks
	for _, child := range e.children {
		// read the extent block from the disk
		b, err := fs.readBlock(child.diskBlock)
		if err != nil {
			return nil, err
		}
		ebf, err := parseExtents(b, e.blockSize, child.fileBlock, child.fileBlock+child.count-1)
		if err != nil {
			return nil, err
		}
		blocks, err := ebf.blocks(fs)
		if err != nil {
			return nil, err
		}
		if len(blocks) > 0 {
			ret = append(ret, blocks...)
		}
	}
	return ret, nil
}

// toBytes convert the node to raw bytes to be stored, either in a block or in an inode
func (e extentInternalNode) toBytes() []byte {
	// 12 byte header, 12 bytes per child
	b := make([]byte, 12+12*e.max)
	copy(b[0:12], e.extentNodeHeader.toBytes())

	for i, child := range e.children {
		base := (i + 1) * 12
		binary.LittleEndian.PutUint32(b[base:base+4], child.fileBlock)
		diskBlock := make([]byte, 8)
		binary.LittleEndian.PutUint64(diskBlock, child.diskBlock)
		copy(b[base+4:base+8], diskBlock[0:4])
		copy(b[base+8:base+10], diskBlock[4:6])
	}
	return b
}

// parseExtents takes bytes, parses them to find the actual extents or the next blocks down.
// It does not recurse down the tree, as we do not want to do that until we actually are ready
// to read those blocks. This is similar to how ext4 driver in the Linux kernel does it.
// totalBlocks is the total number of blocks covered in this given section of the extent tree.
func parseExtents(b []byte, blocksize, start, count uint32) (extentBlockFinder, error) {
	var ret extentBlockFinder
	// must have at least header and one entry
	minLength := extentTreeHeaderLength + extentTreeEntryLength
	if len(b) < minLength {
		return nil, fmt.Errorf("cannot parse extent tree from %d bytes, minimum required %d", len(b), minLength)
	}
	// check magic signature
	if binary.LittleEndian.Uint16(b[0:2]) != extentHeaderSignature {
		return nil, fmt.Errorf("invalid extent tree signature: %x", b[0x0:0x2])
	}
	e := extentNodeHeader{
		entries:   binary.LittleEndian.Uint16(b[0x2:0x4]),
		max:       binary.LittleEndian.Uint16(b[0x4:0x6]),
		depth:     binary.LittleEndian.Uint16(b[0x6:0x8]),
		blockSize: blocksize,
	}
	// b[0x8:0xc] is used for the generation by Lustre but not standard ext4, so we ignore

	// we have parsed the header, now read either the leaf entries or the intermediate nodes
	switch e.depth {
	case 0:
		var leafNode extentLeafNode
		// read the leaves
		for i := 0; i < int(e.entries); i++ {
			start := i*extentTreeEntryLength + extentTreeHeaderLength
			diskBlock := make([]byte, 8)
			copy(diskBlock[0:4], b[start+8:start+12])
			copy(diskBlock[4:6], b[start+6:start+8])
			leafNode.extents = append(leafNode.extents, extent{
				fileBlock:     binary.LittleEndian.Uint32(b[start : start+4]),
				count:         binary.LittleEndian.Uint16(b[start+4 : start+6]),
				startingBlock: binary.LittleEndian.Uint64(diskBlock),
			})
		}
		ret = leafNode
	default:
		var (
			internalNode extentInternalNode
		)
		for i := 0; i < int(e.entries); i++ {
			start := i*extentTreeEntryLength + extentTreeHeaderLength
			diskBlock := make([]byte, 8)
			copy(diskBlock[0:4], b[start+4:start+8])
			copy(diskBlock[4:6], b[start+8:start+10])
			ptr := &extentChildPtr{
				diskBlock: binary.LittleEndian.Uint64(diskBlock),
				fileBlock: binary.LittleEndian.Uint32(b[start : start+4]),
			}
			internalNode.children = append(internalNode.children, ptr)
			if i > 0 {
				internalNode.children[i-1].count = ptr.fileBlock - internalNode.children[i-1].fileBlock
			}
		}
		if len(internalNode.children) > 0 {
			internalNode.children[len(internalNode.children)-1].count = start + count - internalNode.children[len(internalNode.children)-1].fileBlock
		}
		ret = internalNode
	}

	return ret, nil
}
