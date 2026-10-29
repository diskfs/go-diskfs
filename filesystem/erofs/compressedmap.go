package erofs

import (
	"encoding/binary"
	"fmt"
	"math"
)

type compressedInfo struct {
	advise            uint16
	lclusterBits      uint
	algorithmType     [2]uint8
	fragmentOffset    int64
	inlineDataSize    int
	tailExtentHeadLCN uint64
	initialized       bool
}

type compressedExtent struct {
	logicalStart   int64
	logicalLength  int64
	physicalStart  int64
	physicalLength int64
	physicalBlock  uint32
	algorithm      uint8
	fragment       bool
}

type mapRecorder struct {
	lcn            uint64
	clusterType    uint8
	headType       uint8
	clusterOffset  int
	delta          [2]uint16
	physicalBlock  uint32
	compressedBlks uint32
}

func roundUp64(value, alignment int64) int64 {
	return (value + alignment - 1) &^ (alignment - 1)
}

func (f *FileSystem) initCompressed(node *inode) error {
	if node.compressed != nil && node.compressed.initialized {
		return nil
	}
	inodeOffset, err := f.inodeOffset(node.nid)
	if err != nil {
		return err
	}
	headerOffset := roundUp64(inodeOffset+int64(node.inodeSize+node.xattrSize), 8)
	header := make([]byte, 8)
	if err := f.readAt(header, headerOffset); err != nil {
		return fmt.Errorf("unable to read compression header for inode %d: %w", node.nid, err)
	}
	info := &compressedInfo{}
	if header[7]&(1<<7) != 0 {
		info.advise = zAdviseFragment
		info.fragmentOffset = int64(binary.LittleEndian.Uint64(header) - (1 << 63))
		info.initialized = true
		node.compressed = info
		return nil
	}
	info.advise = binary.LittleEndian.Uint16(header[4:6])
	info.lclusterBits = uint(f.superblock.blockSizeBits) + uint(header[7]&15)
	info.algorithmType[0] = header[6] & 15
	info.algorithmType[1] = header[6] >> 4
	if info.advise&zAdviseFragment != 0 {
		info.fragmentOffset = int64(binary.LittleEndian.Uint32(header[0:4]))
	} else if info.advise&zAdviseInlineData != 0 {
		info.inlineDataSize = int(binary.LittleEndian.Uint16(header[2:4]))
	}
	if node.dataLayout == dataLayoutCompressedCompact &&
		((info.advise&zAdviseBigPcluster1 == 0) != (info.advise&zAdviseBigPcluster2 == 0)) {
		return fmt.Errorf("inconsistent big pcluster advice for inode %d", node.nid)
	}
	if info.lclusterBits > 27 {
		return fmt.Errorf("invalid logical cluster bits %d for inode %d", info.lclusterBits, node.nid)
	}
	if info.inlineDataSize != 0 {
		return fmt.Errorf("inline compressed tails are not supported for inode %d", node.nid)
	}
	node.compressed = info
	if info.advise&zAdviseFragment != 0 {
		extent, err := f.mapCompressed(node, node.size-1, true)
		if err != nil {
			return err
		}
		info.tailExtentHeadLCN = uint64(extent.logicalStart >> info.lclusterBits)
		if node.dataLayout == dataLayoutCompressedFull {
			fragmentOffset := uint64(info.fragmentOffset) | uint64(extent.physicalBlock)<<32
			if fragmentOffset > math.MaxInt64 {
				return fmt.Errorf("fragment offset for inode %d is too large", node.nid)
			}
			info.fragmentOffset = int64(fragmentOffset)
		}
	}
	info.initialized = true
	node.compressed = info
	return nil
}

func (f *FileSystem) mapCompressed(node *inode, offset int64, findTail bool) (*compressedExtent, error) {
	if offset < 0 || offset >= node.size {
		return nil, fmt.Errorf("compressed offset %d is outside inode %d size %d", offset, node.nid, node.size)
	}
	info := node.compressed
	if info == nil {
		return nil, fmt.Errorf("compression metadata is not initialized for inode %d", node.nid)
	}
	if info.advise&zAdviseFragment != 0 && !findTail && info.tailExtentHeadLCN == 0 {
		return &compressedExtent{logicalStart: 0, logicalLength: node.size, fragment: true}, nil
	}
	if node.dataLayout == dataLayoutCompressedFull && info.advise&1 != 0 {
		return nil, fmt.Errorf("compressed extent metadata is not supported for inode %d", node.nid)
	}

	initialLCN := uint64(offset >> info.lclusterBits)
	endOffset := int(offset & ((1 << info.lclusterBits) - 1))
	recorder := mapRecorder{}
	var err error
	if node.dataLayout == dataLayoutCompressedCompact {
		err = f.loadCompactLCluster(node, &recorder, initialLCN, false)
	} else {
		err = f.loadFullLCluster(node, &recorder, initialLCN)
	}
	if err != nil {
		return nil, err
	}
	extent := &compressedExtent{}
	end := int64((recorder.lcn + 1) << info.lclusterBits)
	switch recorder.clusterType {
	case zClusterTypePlain, zClusterTypeHead1, zClusterTypeHead2:
		if endOffset >= recorder.clusterOffset {
			recorder.headType = recorder.clusterType
			extent.logicalStart = int64(recorder.lcn)<<info.lclusterBits | int64(recorder.clusterOffset)
			if end > node.size {
				end = node.size
			}
		} else {
			if recorder.lcn == 0 {
				return nil, fmt.Errorf("invalid logical cluster zero for inode %d", node.nid)
			}
			end = int64(recorder.lcn)<<info.lclusterBits | int64(recorder.clusterOffset)
			recorder.delta[0] = 1
			if err := f.lookBack(node, &recorder, recorder.delta[0]); err != nil {
				return nil, err
			}
		}
	case zClusterTypeNonHead:
		if err := f.lookBack(node, &recorder, recorder.delta[0]); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("invalid logical cluster type %d for inode %d", recorder.clusterType, node.nid)
	}
	extent.logicalStart = int64(recorder.lcn)<<info.lclusterBits | int64(recorder.clusterOffset)
	end, err = f.compressedExtentEnd(node, recorder.lcn, recorder.clusterOffset)
	if err != nil {
		return nil, err
	}
	extent.logicalLength = end - extent.logicalStart
	if findTail {
		info.tailExtentHeadLCN = recorder.lcn
		extent.physicalBlock = recorder.physicalBlock
		return extent, nil
	}
	if info.advise&zAdviseFragment != 0 && recorder.lcn == info.tailExtentHeadLCN {
		extent.fragment = true
		return extent, nil
	}
	extent.physicalStart = int64(recorder.physicalBlock) * f.blockSize()
	if err := f.compressedLength(node, &recorder); err != nil {
		return nil, err
	}
	extent.physicalLength = int64(recorder.compressedBlks) * f.blockSize()
	if recorder.headType == zClusterTypePlain {
		if info.advise&zAdviseInterlaced != 0 {
			extent.algorithm = compressionInterlaced
		} else {
			extent.algorithm = compressionShifted
		}
	} else if recorder.headType == zClusterTypeHead2 {
		extent.algorithm = info.algorithmType[1]
	} else {
		extent.algorithm = info.algorithmType[0]
	}
	if extent.algorithm >= compressionInterlaced+1 ||
		(extent.algorithm < compressionShifted && f.superblock.availableComprAlgs&(1<<extent.algorithm) == 0) {
		return nil, fmt.Errorf("unsupported compression algorithm %d for inode %d", extent.algorithm, node.nid)
	}
	if extent.physicalLength == 0 || extent.physicalLength > 1024*1024 || extent.logicalLength > 12*1024*1024 {
		return nil, fmt.Errorf("invalid compressed extent sizes for inode %d", node.nid)
	}
	if extent.physicalStart < 0 || extent.physicalStart+extent.physicalLength > f.size {
		return nil, fmt.Errorf("compressed extent for inode %d is outside the image", node.nid)
	}
	return extent, nil
}

func (f *FileSystem) compressedExtentEnd(node *inode, headLCN uint64, clusterOffset int) (int64, error) {
	info := node.compressed
	lcn := headLCN
	for {
		if int64(lcn)<<info.lclusterBits >= node.size {
			return node.size, nil
		}
		recorder := mapRecorder{}
		var err error
		if node.dataLayout == dataLayoutCompressedCompact {
			err = f.loadCompactLCluster(node, &recorder, lcn, true)
		} else {
			err = f.loadFullLCluster(node, &recorder, lcn)
		}
		if err != nil {
			return 0, err
		}
		delta := recorder.delta[1]
		if recorder.clusterType == zClusterTypeNonHead {
			if delta == 0 {
				delta = 1
			}
		} else {
			if lcn != headLCN {
				return int64(lcn)<<info.lclusterBits | int64(recorder.clusterOffset), nil
			}
			delta = 1
		}
		lcn += uint64(delta)
	}
}

func (f *FileSystem) loadFullLCluster(node *inode, recorder *mapRecorder, lcn uint64) error {
	inodeOffset, err := f.inodeOffset(node.nid)
	if err != nil {
		return err
	}
	end := roundUp64(inodeOffset+int64(node.inodeSize+node.xattrSize), 8)
	pos := roundUp64(end, 8) + 16 + int64(lcn)*8
	b := make([]byte, 8)
	if err := f.readAt(b, pos); err != nil {
		return fmt.Errorf("unable to read full compression index for inode %d: %w", node.nid, err)
	}
	recorder.lcn = lcn
	recorder.clusterType = uint8(binary.LittleEndian.Uint16(b[0:2]) & 3)
	if recorder.clusterType == zClusterTypeNonHead {
		recorder.clusterOffset = 1 << node.compressed.lclusterBits
		recorder.delta[0] = binary.LittleEndian.Uint16(b[4:6])
		if recorder.delta[0]&zD0ClusterCount != 0 {
			if node.compressed.advise&(zAdviseBigPcluster1|zAdviseBigPcluster2) == 0 {
				return fmt.Errorf("invalid compressed block count for inode %d", node.nid)
			}
			recorder.compressedBlks = uint32(recorder.delta[0] &^ zD0ClusterCount)
			recorder.delta[0] = 1
		}
		recorder.delta[1] = binary.LittleEndian.Uint16(b[6:8])
	} else {
		recorder.clusterOffset = int(binary.LittleEndian.Uint16(b[2:4]))
		recorder.physicalBlock = binary.LittleEndian.Uint32(b[4:8])
		if binary.LittleEndian.Uint16(b[0:2])&zPartialRef != 0 {
			return fmt.Errorf("partial compressed references are not supported for inode %d", node.nid)
		}
	}
	return f.validateLCluster(node, recorder)
}

func decodeCompactBits(bits uint, b []byte, bitPosition int) (uint16, uint8) {
	value := binary.LittleEndian.Uint32(b[bitPosition/8:bitPosition/8+4]) >> (bitPosition & 7)
	return uint16(value & ((1 << bits) - 1)), uint8(value >> bits & 3)
}

func (f *FileSystem) loadCompactLCluster(node *inode, recorder *mapRecorder, requestedLCN uint64, lookahead bool) error {
	info := node.compressed
	inodeOffset, err := f.inodeOffset(node.nid)
	if err != nil {
		return err
	}
	ebase := 8 + roundUp64(inodeOffset+int64(node.inodeSize+node.xattrSize), 8)
	totalIndex := uint64((node.size + f.blockSize() - 1) / f.blockSize())
	if requestedLCN >= totalIndex || info.lclusterBits > 14 {
		return fmt.Errorf("invalid logical cluster %d for inode %d", requestedLCN, node.nid)
	}
	lcn := requestedLCN
	compacted4Initial := uint64(((32 - ebase%32) / 4) & 7)
	compacted2B := uint64(0)
	if info.advise&zAdviseCompacted2B != 0 && compacted4Initial < totalIndex {
		compacted2B = (totalIndex - compacted4Initial) / 16 * 16
	}
	pos := ebase
	shift := uint(2)
	if lcn >= compacted4Initial {
		pos += int64(compacted4Initial) * 4
		lcn -= compacted4Initial
		if lcn < compacted2B {
			shift = 1
		} else {
			pos += int64(compacted2B) * 2
			lcn -= compacted2B
		}
	}
	pos += int64(lcn) << shift
	var vcnt uint
	if shift == 2 && info.lclusterBits <= 14 {
		vcnt = 2
	} else if shift == 1 && info.lclusterBits <= 12 {
		vcnt = 16
	} else {
		return fmt.Errorf("unsupported compact index packing for inode %d", node.nid)
	}
	packSize := int64(vcnt) << shift
	packStart := pos &^ (packSize - 1)
	pack := make([]byte, packSize)
	if err := f.readAt(pack, packStart); err != nil {
		return fmt.Errorf("unable to read compact compression index for inode %d: %w", node.nid, err)
	}
	lobits := info.lclusterBits
	if lobits < 12 {
		lobits = 12
	}
	encodeBits := int(((packSize - 4) * 8) / int64(vcnt))
	index := int((pos - packStart) >> shift)
	lo, clusterType := decodeCompactBits(lobits, pack, encodeBits*index)
	recorder.lcn = requestedLCN
	recorder.clusterType = clusterType
	if clusterType == zClusterTypeNonHead {
		recorder.clusterOffset = 1 << info.lclusterBits
		if lookahead {
			recorder.delta[1] = compactLookahead(pack, index, int(vcnt), encodeBits, int(lobits))
		}
		if lo&zD0ClusterCount != 0 {
			if info.advise&zAdviseBigPcluster1 == 0 {
				return fmt.Errorf("invalid big pcluster count for inode %d", node.nid)
			}
			recorder.compressedBlks = uint32(lo &^ zD0ClusterCount)
			recorder.delta[0] = 1
			return f.validateLCluster(node, recorder)
		}
		if index+1 != int(vcnt) {
			recorder.delta[0] = lo
			return f.validateLCluster(node, recorder)
		}
		previous, previousType := decodeCompactBits(lobits, pack, encodeBits*(index-1))
		if previousType != zClusterTypeNonHead {
			previous = 0
		} else if previous&zD0ClusterCount != 0 {
			previous = 1
		}
		recorder.delta[0] = previous + 1
		return f.validateLCluster(node, recorder)
	}
	recorder.clusterOffset = int(lo)
	recorder.delta[0] = 0
	bigPcluster := info.advise&zAdviseBigPcluster1 != 0
	nblk := uint32(1)
	if bigPcluster {
		nblk = 0
	}
	for index > 0 {
		index--
		lo, clusterType = decodeCompactBits(lobits, pack, encodeBits*index)
		if clusterType == zClusterTypeNonHead {
			if !bigPcluster {
				index -= int(lo)
			} else if lo&zD0ClusterCount != 0 {
				index--
				nblk += uint32(lo &^ zD0ClusterCount)
				continue
			} else if lo <= 1 {
				return fmt.Errorf("invalid big pcluster delta for inode %d", node.nid)
			} else {
				index -= int(lo) - 2
				continue
			}
		}
		if index >= 0 {
			nblk++
		}
	}
	recorder.physicalBlock = binary.LittleEndian.Uint32(pack[packSize-4:]) + nblk
	return f.validateLCluster(node, recorder)
}

func compactLookahead(pack []byte, index, vcnt, encodeBits, lobits int) uint16 {
	var distance uint16
	var last uint16
	for i := index; i < vcnt; i++ {
		lo, clusterType := decodeCompactBits(uint(lobits), pack, encodeBits*i)
		if clusterType != zClusterTypeNonHead {
			return distance
		}
		distance++
		last = lo
	}
	if last&zD0ClusterCount == 0 {
		distance += last - 1
	}
	return distance
}

func (f *FileSystem) validateLCluster(node *inode, recorder *mapRecorder) error {
	if recorder.clusterType > zClusterTypeHead2 {
		return fmt.Errorf("invalid compression index type %d for inode %d", recorder.clusterType, node.nid)
	}
	if recorder.clusterType != zClusterTypeNonHead && recorder.clusterOffset >= 1<<node.compressed.lclusterBits {
		return fmt.Errorf("invalid compression cluster offset for inode %d", node.nid)
	}
	return nil
}

func (f *FileSystem) lookBack(node *inode, recorder *mapRecorder, distance uint16) error {
	for recorder.lcn >= uint64(distance) {
		lcn := recorder.lcn - uint64(distance)
		var err error
		if node.dataLayout == dataLayoutCompressedCompact {
			err = f.loadCompactLCluster(node, recorder, lcn, false)
		} else {
			err = f.loadFullLCluster(node, recorder, lcn)
		}
		if err != nil {
			return err
		}
		if recorder.clusterType == zClusterTypeNonHead {
			distance = recorder.delta[0]
			if distance == 0 {
				break
			}
			continue
		}
		recorder.headType = recorder.clusterType
		return nil
	}
	return fmt.Errorf("invalid compression lookback distance for inode %d", node.nid)
}

func (f *FileSystem) compressedLength(node *inode, recorder *mapRecorder) error {
	info := node.compressed
	if (recorder.headType == zClusterTypeHead1 && info.advise&zAdviseBigPcluster1 == 0) ||
		((recorder.headType == zClusterTypePlain || recorder.headType == zClusterTypeHead2) && info.advise&zAdviseBigPcluster2 == 0) ||
		(recorder.lcn+1)<<info.lclusterBits >= uint64(node.size) {
		recorder.compressedBlks = 1
	}
	if recorder.compressedBlks != 0 {
		return nil
	}
	lcn := recorder.lcn + 1
	var err error
	if node.dataLayout == dataLayoutCompressedCompact {
		err = f.loadCompactLCluster(node, recorder, lcn, false)
	} else {
		err = f.loadFullLCluster(node, recorder, lcn)
	}
	if err != nil {
		return err
	}
	if recorder.clusterType != zClusterTypeNonHead {
		recorder.compressedBlks = 1
		return nil
	}
	if recorder.clusterType != zClusterTypeNonHead || recorder.delta[0] != 1 || recorder.compressedBlks == 0 {
		return fmt.Errorf("unable to determine compressed length for inode %d", node.nid)
	}
	return nil
}
