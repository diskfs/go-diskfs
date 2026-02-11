package ext4

import (
	"encoding/binary"
	"fmt"
	"sort"
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

// blockCount how many filesystem blocks are covered in the extents.
// Remember that these are filesystem blocks, which can vary, not the fixed 512-byte sectors on disk,
// often used in superblock or inode in various places.
//
//nolint:unused // useful function for future
func (e extents) blockCount() uint64 {
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
	getDepth() uint16
	getMax() uint16
	getBlockSize() uint32
	getFileBlock() uint32
	getCount() uint32
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
	extents   extents // the actual extents
	diskBlock uint64  // block number where this node is stored on disk (0 if root/in inode)
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
	return e.extents, nil
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

func (e *extentLeafNode) getDepth() uint16 {
	return e.depth
}

func (e *extentLeafNode) getMax() uint16 {
	return e.max
}

func (e *extentLeafNode) getBlockSize() uint32 {
	return e.blockSize
}

func (e *extentLeafNode) getFileBlock() uint32 {
	return e.extents[0].fileBlock
}

func (e *extentLeafNode) getCount() uint32 {
	return uint32(len(e.extents))
}

// extentInternalNode represents an internal node in a tree of extents
// it includes the information in the header and the internal nodes
// By definition, this is an internal node, so depth>0
type extentInternalNode struct {
	extentNodeHeader
	children  []*extentChildPtr // the children
	diskBlock uint64            // block number where this node is stored on disk (0 if root/in inode)
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
func (e *extentInternalNode) getDepth() uint16 {
	return e.depth
}

func (e *extentInternalNode) getMax() uint16 {
	return e.max
}

func (e *extentInternalNode) getBlockSize() uint32 {
	return e.blockSize
}

func (e *extentInternalNode) getFileBlock() uint32 {
	return e.children[0].fileBlock
}

func (e *extentInternalNode) getCount() uint32 {
	return uint32(len(e.children))
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
		leafNode := extentLeafNode{
			extentNodeHeader: e,
		}
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
		ret = &leafNode
	default:
		internalNode := extentInternalNode{
			extentNodeHeader: e,
		}
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
		ret = &internalNode
	}

	return ret, nil
}

// extendExtentTree extends extent tree with a slice of new extents
// if the existing tree is nil, create a new one.
// For example, if the input is an extent tree - like the kind found in an inode - and you want to add more extents to it,
// you add the provided extents, and it expands the tree, including creating new internal nodes and writing them to disk, as needed.
// Returns the updated tree, the number of metadata blocks allocated for extent tree nodes, and any error.

func extendExtentTree(existing extentBlockFinder, added *extents, fs *FileSystem, parent *extentInternalNode) (extentBlockFinder, uint64, error) {
	// Check if existing is a leaf or internal node
	switch node := existing.(type) {
	case *extentLeafNode:
		return extendLeafNode(node, added, fs, parent)
	case *extentInternalNode:
		return extendInternalNode(node, added, fs, parent)
	case nil:
		// brand new extent tree. The root is in the inode, which has a max of 4 extents.
		result, err := createRootExtentTree(added, fs)
		return result, 0, err
	default:
		return nil, 0, fmt.Errorf("unsupported extentBlockFinder type")
	}
}

func createRootExtentTree(added *extents, fs *FileSystem) (extentBlockFinder, error) {
	// the root always is in the inode, which has a maximum of 4 extents. If it fits within that, we can just create a leaf node.
	if len(*added) <= 4 {
		return &extentLeafNode{
			extentNodeHeader: extentNodeHeader{
				depth:     0,
				entries:   uint16(len(*added)),
				max:       4,
				blockSize: fs.superblock.blockSize,
			},
			extents: *added,
		}, nil
	}
	// in theory, we never should be creating a root internal node. We always should be starting with an extent or two,
	// and later expanding the file.
	// It might be theoretically possible, though, so we will handle it in the future.
	return nil, fmt.Errorf("cannot create root internal node")
}

func extendLeafNode(node *extentLeafNode, added *extents, fs *FileSystem, parent *extentInternalNode) (extentBlockFinder, uint64, error) {
	// Check if the leaf node has enough space for the added extents
	if len(node.extents)+len(*added) <= int(node.max) {
		// Simply append the extents if there's enough space
		node.extents = append(node.extents, *added...)
		node.entries = uint16(len(node.extents))

		// Write the updated node back to the disk
		err := writeNodeToDisk(node, fs, parent)
		if err != nil {
			return nil, 0, err
		}

		return node, 0, nil
	}

	// Check if the original node was the root (parent == nil)
	if parent == nil {
		// Calculate max entries for a non-root leaf node
		maxEntriesNonRoot := (node.blockSize - 12) / 12
		totalExtents := len(node.extents) + len(*added)

		// If all extents fit in a single non-root leaf node, create one leaf + internal root
		// This avoids unnecessarily splitting into two nodes
		if uint32(totalExtents) <= maxEntriesNonRoot {
			newLeaf, metaBlocks, err := promoteLeafToChild(node, added, fs)
			if err != nil {
				return nil, 0, err
			}
			newRoot := createInternalNode([]extentBlockFinder{newLeaf}, nil, fs)
			return newRoot, metaBlocks, nil
		}

		// Otherwise split the node
		newNodes, metaBlocks, err := splitLeafNode(node, added, fs, parent)
		if err != nil {
			return nil, 0, err
		}

		// Create a new internal node to reference the split leaf nodes
		var newNodesAsBlockFinder []extentBlockFinder
		for _, n := range newNodes {
			newNodesAsBlockFinder = append(newNodesAsBlockFinder, n)
		}
		newRoot := createInternalNode(newNodesAsBlockFinder, nil, fs)
		return newRoot, metaBlocks, nil
	}

	// If not enough space in a non-root node, split it
	newNodes, splitMetaBlocks, err := splitLeafNode(node, added, fs, parent)
	if err != nil {
		return nil, 0, err
	}

	// If the original node was not the root, handle the parent internal node
	parentNode, err := getParentNode(node, fs)
	if err != nil {
		return nil, 0, err
	}

	_ = newNodes // nodes are already written to disk in splitLeafNode
	result, parentMetaBlocks, err := extendInternalNode(parentNode, added, fs, parent)
	return result, splitMetaBlocks + parentMetaBlocks, err
}

func splitLeafNode(node *extentLeafNode, added *extents, fs *FileSystem, parent *extentInternalNode) ([]*extentLeafNode, uint64, error) {
	// Combine existing and new extents
	allExtents := node.extents
	allExtents = append(allExtents, *added...)
	// Sort extents by fileBlock to maintain order
	sort.Slice(allExtents, func(i, j int) bool {
		return allExtents[i].fileBlock < allExtents[j].fileBlock
	})

	// Calculate the midpoint to split the extents
	mid := len(allExtents) / 2

	// Calculate max entries for non-root nodes (based on block size)
	// Each entry is 12 bytes, header is 12 bytes
	maxEntries := (node.blockSize - 12) / 12

	// Create the first new leaf node
	firstLeaf := &extentLeafNode{
		extentNodeHeader: extentNodeHeader{
			depth:     0,
			entries:   uint16(mid),
			max:       uint16(maxEntries),
			blockSize: node.blockSize,
		},
		extents: allExtents[:mid],
	}

	// Create the second new leaf node
	secondLeaf := &extentLeafNode{
		extentNodeHeader: extentNodeHeader{
			depth:     0,
			entries:   uint16(len(allExtents) - mid),
			max:       uint16(maxEntries),
			blockSize: node.blockSize,
		},
		extents: allExtents[mid:],
	}

	var metaBlocks uint64

	// When splitting the root (parent == nil), we need to allocate new disk blocks
	// for the child nodes since they will no longer live in the inode
	if parent == nil {
		// Allocate blocks for both new leaf nodes
		blockAlloc, err := fs.allocateExtents(uint64(fs.superblock.blockSize)*2, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("could not allocate blocks for split leaf nodes: %w", err)
		}
		// Get the starting block from the allocated extent
		allocatedExtents := *blockAlloc
		if len(allocatedExtents) == 0 || allocatedExtents[0].count < 2 {
			return nil, 0, fmt.Errorf("could not allocate enough blocks for split leaf nodes")
		}
		firstLeaf.diskBlock = allocatedExtents[0].startingBlock
		secondLeaf.diskBlock = allocatedExtents[0].startingBlock + 1
		metaBlocks = 2

		// Write the leaf nodes to their allocated blocks
		if err := writeNodeToBlock(firstLeaf, fs, firstLeaf.diskBlock); err != nil {
			return nil, 0, err
		}
		if err := writeNodeToBlock(secondLeaf, fs, secondLeaf.diskBlock); err != nil {
			return nil, 0, err
		}
	} else {
		// Write new leaf nodes to the disk using parent reference
		err := writeNodeToDisk(firstLeaf, fs, parent)
		if err != nil {
			return nil, 0, err
		}
		err = writeNodeToDisk(secondLeaf, fs, parent)
		if err != nil {
			return nil, 0, err
		}
	}

	return []*extentLeafNode{firstLeaf, secondLeaf}, metaBlocks, nil
}

// promoteLeafToChild takes a root leaf node and its new extents, combines them into a single
// non-root leaf node that will live on disk. This is used when all extents fit in one non-root leaf.
func promoteLeafToChild(node *extentLeafNode, added *extents, fs *FileSystem) (*extentLeafNode, uint64, error) {
	// Combine existing and new extents
	allExtents := node.extents
	allExtents = append(allExtents, *added...)
	// Sort extents by fileBlock to maintain order
	sort.Slice(allExtents, func(i, j int) bool {
		return allExtents[i].fileBlock < allExtents[j].fileBlock
	})

	// Calculate max entries for non-root nodes (based on block size)
	maxEntries := (node.blockSize - 12) / 12

	// Create the new leaf node
	newLeaf := &extentLeafNode{
		extentNodeHeader: extentNodeHeader{
			depth:     0,
			entries:   uint16(len(allExtents)),
			max:       uint16(maxEntries),
			blockSize: node.blockSize,
		},
		extents: allExtents,
	}

	// Allocate a block for the new leaf node
	blockAlloc, err := fs.allocateExtents(uint64(fs.superblock.blockSize), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("could not allocate block for leaf node: %w", err)
	}
	allocatedExtents := *blockAlloc
	if len(allocatedExtents) == 0 || allocatedExtents[0].count < 1 {
		return nil, 0, fmt.Errorf("could not allocate block for leaf node")
	}
	newLeaf.diskBlock = allocatedExtents[0].startingBlock

	// Write the leaf node to its allocated block
	if err := writeNodeToBlock(newLeaf, fs, newLeaf.diskBlock); err != nil {
		return nil, 0, err
	}

	return newLeaf, 1, nil
}

func createInternalNode(nodes []extentBlockFinder, parent *extentInternalNode, fs *FileSystem) *extentInternalNode {
	// Calculate max entries for internal nodes (based on block size)
	// Each entry is 12 bytes, header is 12 bytes
	// For root node in inode, max is 4
	maxEntries := uint16(4)
	if parent != nil {
		maxEntries = uint16((nodes[0].getBlockSize() - 12) / 12)
	}

	internalNode := &extentInternalNode{
		extentNodeHeader: extentNodeHeader{
			depth:     nodes[0].getDepth() + 1, // Depth is 1 more than the children
			entries:   uint16(len(nodes)),
			max:       maxEntries,
			blockSize: nodes[0].getBlockSize(),
		},
		children: make([]*extentChildPtr, len(nodes)),
	}

	for i, node := range nodes {
		var diskBlock uint64
		if parent == nil {
			// When creating a new root, get disk block from the node itself
			diskBlock = getDiskBlockFromNode(node)
		} else {
			diskBlock = getBlockNumberFromNode(node, parent)
		}
		internalNode.children[i] = &extentChildPtr{
			fileBlock: node.getFileBlock(),
			count:     node.getCount(),
			diskBlock: diskBlock,
		}
	}

	// Write the new internal node to the disk (root nodes live in inode, so parent==nil means no write)
	err := writeNodeToDisk(internalNode, fs, parent)
	if err != nil {
		return nil
	}

	return internalNode
}

func getBlockNumberFromNode(node extentBlockFinder, parent *extentInternalNode) uint64 {
	if parent == nil {
		return 0
	}
	for _, childPtr := range parent.children {
		if childPtrMatchesNode(childPtr, node) {
			return childPtr.diskBlock
		}
	}
	return 0 // Return 0 or an appropriate error value if the block number is not found
}

// getDiskBlockFromNode retrieves the disk block number stored in the node itself
func getDiskBlockFromNode(node extentBlockFinder) uint64 {
	switch n := node.(type) {
	case *extentLeafNode:
		return n.diskBlock
	case *extentInternalNode:
		return n.diskBlock
	default:
		return 0
	}
}

// writeNodeToBlock writes an extent node to a specific disk block
func writeNodeToBlock(node extentBlockFinder, fs *FileSystem, blockNumber uint64) error {
	writableFile, err := fs.backend.Writable()
	if err != nil {
		return err
	}

	data := node.toBytes()
	_, err = writableFile.WriteAt(data, int64(blockNumber)*int64(fs.superblock.blockSize))
	return err
}

// Helper function to match a child pointer to a node
func childPtrMatchesNode(childPtr *extentChildPtr, node extentBlockFinder) bool {
	switch n := node.(type) {
	case *extentLeafNode:
		return childPtr.fileBlock == n.extents[0].fileBlock
	case *extentInternalNode:
		// Logic to determine if the childPtr matches the internal node
		// Placeholder: Implement based on your specific matching criteria
		return true
	default:
		return false
	}
}

func extendInternalNode(node *extentInternalNode, added *extents, fs *FileSystem, parent *extentInternalNode) (extentBlockFinder, uint64, error) {
	// Find the appropriate child node to extend
	childIndex := findChildNode(node, added)
	childPtr := node.children[childIndex]

	// Load the actual child node from the disk
	childNode, err := loadChildNode(childPtr, fs)
	if err != nil {
		return nil, 0, err
	}

	// Recursively extend the child node
	updatedChild, metaBlocks, err := extendExtentTree(childNode, added, fs, node)
	if err != nil {
		return nil, 0, err
	}

	// Update the current internal node to reference the updated child
	switch updatedChild := updatedChild.(type) {
	case *extentLeafNode:
		node.children[childIndex] = &extentChildPtr{
			fileBlock: updatedChild.extents[0].fileBlock,
			count:     uint32(len(updatedChild.extents)),
			diskBlock: getBlockNumberFromNode(updatedChild, node),
		}
	case *extentInternalNode:
		node.children[childIndex] = &extentChildPtr{
			fileBlock: updatedChild.children[0].fileBlock,
			count:     uint32(len(updatedChild.children)),
			diskBlock: getBlockNumberFromNode(updatedChild, node),
		}
	default:
		return nil, 0, fmt.Errorf("unsupported updatedChild type")
	}

	// Check if the internal node is at capacity
	if len(node.children) > int(node.max) {
		// Split the internal node if it's at capacity
		newInternalNodes, err := splitInternalNode(node, node.children[childIndex], fs, parent)
		if err != nil {
			return nil, 0, err
		}

		// Check if the original node was the root
		if parent == nil {
			// Create a new internal node as the new root
			var newNodesAsBlockFinder []extentBlockFinder
			for _, n := range newInternalNodes {
				newNodesAsBlockFinder = append(newNodesAsBlockFinder, n)
			}
			newRoot := createInternalNode(newNodesAsBlockFinder, nil, fs)
			return newRoot, metaBlocks, nil
		}

		// If the original node was not the root, handle the parent internal node
		return extendInternalNode(parent, added, fs, parent)
	}

	// Write the updated node back to the disk
	err = writeNodeToDisk(node, fs, parent)
	if err != nil {
		return nil, 0, err
	}

	return node, metaBlocks, nil
}

// Helper function to get the parent node of a given internal node
//
//nolint:revive // this parameter will be used eventually
func getParentNode(node extentBlockFinder, fs *FileSystem) (*extentInternalNode, error) {
	// Logic to find and return the parent node of the given node
	// This is a placeholder and needs to be implemented based on your specific tree structure
	return nil, fmt.Errorf("getParentNode not implemented")
}

func splitInternalNode(node *extentInternalNode, newChild *extentChildPtr, fs *FileSystem, parent *extentInternalNode) ([]*extentInternalNode, error) {
	// Combine existing children with the new child
	allChildren := node.children
	allChildren = append(allChildren, newChild)
	// Sort children by fileBlock to maintain order
	sort.Slice(allChildren, func(i, j int) bool {
		return allChildren[i].fileBlock < allChildren[j].fileBlock
	})

	// Calculate the midpoint to split the children
	mid := len(allChildren) / 2

	// Create the first new internal node
	firstInternal := &extentInternalNode{
		extentNodeHeader: extentNodeHeader{
			depth:     node.depth,
			entries:   uint16(mid),
			max:       node.max,
			blockSize: node.blockSize,
		},
		children: allChildren[:mid],
	}

	// Create the second new internal node
	secondInternal := &extentInternalNode{
		extentNodeHeader: extentNodeHeader{
			depth:     node.depth,
			entries:   uint16(len(allChildren) - mid),
			max:       node.max,
			blockSize: node.blockSize,
		},
		children: allChildren[mid:],
	}

	// Write new internal nodes to the disk
	err := writeNodeToDisk(firstInternal, fs, parent)
	if err != nil {
		return nil, err
	}
	err = writeNodeToDisk(secondInternal, fs, parent)
	if err != nil {
		return nil, err
	}

	return []*extentInternalNode{firstInternal, secondInternal}, nil
}

func writeNodeToDisk(node extentBlockFinder, fs *FileSystem, parent *extentInternalNode) error {
	// Root nodes live in the inode; only write when there's a parent block.
	if parent == nil {
		return nil
	}
	blockNumber := getBlockNumberFromNode(node, parent)

	if blockNumber == 0 {
		return fmt.Errorf("block number not found for node")
	}

	writableFile, err := fs.backend.Writable()
	if err != nil {
		return err
	}

	data := node.toBytes()
	_, err = writableFile.WriteAt(data, int64(blockNumber)*int64(fs.superblock.blockSize))
	return err
}

// Helper function to get a new block number when there is no parent
//
//nolint:revive // this parameter will be used eventually
func getNewBlockNumber(fs *FileSystem) uint64 {
	// Logic to allocate a new block
	// This is a placeholder and needs to be implemented based on your specific filesystem structure
	return 0 // Placeholder: Replace with actual implementation
}

// Helper function to find the block number of a child node from its parent
func findChildBlockNumber(parent *extentInternalNode, child extentBlockFinder) uint64 {
	for _, childPtr := range parent.children {
		if childPtrMatchesNode(childPtr, child) {
			return childPtr.diskBlock
		}
	}
	return 0
}

func findChildNode(node *extentInternalNode, added *extents) int {
	// Assuming added extents are sorted, find the correct child node to extend
	addedSlice := *added
	for i, child := range node.children {
		if addedSlice[0].fileBlock < child.fileBlock {
			return i - 1
		}
	}
	return len(node.children) - 1
}

// loadChildNode load up a child node from the disk
//
//nolint:unparam // this parameter will be used eventually
func loadChildNode(childPtr *extentChildPtr, fs *FileSystem) (extentBlockFinder, error) {
	data := make([]byte, fs.superblock.blockSize)
	_, err := fs.backend.ReadAt(data, int64(childPtr.diskBlock)*int64(fs.superblock.blockSize))
	if err != nil {
		return nil, err
	}

	// Logic to decode data into an extentBlockFinder (extentLeafNode or extentInternalNode)
	// This is a placeholder and needs to be implemented based on your specific encoding scheme
	var node extentBlockFinder
	// Implement the logic to decode the node from the data
	return node, nil
}

func extentsBlockFinderFromExtents(exts extents, blocksize uint32) extentBlockFinder {
	return &extentLeafNode{
		extentNodeHeader: extentNodeHeader{
			depth:     0,
			entries:   uint16(len(exts)),
			max:       4, // assuming max 4 for leaf nodes in inode
			blockSize: blocksize,
		},
		extents: exts,
	}
}
