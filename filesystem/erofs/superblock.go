package erofs

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type superblock struct {
	checksum            uint32
	featureCompat       uint32
	blockSizeBits       uint8
	superblockExtSlots  uint8
	rootNID             uint16
	inos                uint64
	epoch               uint64
	fixedNsec           uint32
	blocks              uint32
	metaBlockAddress    uint32
	xattrBlockAddress   uint32
	uuid                [16]byte
	volumeName          [16]byte
	featureIncompat     uint32
	availableComprAlgs  uint16
	extraDevices        uint16
	deviceSlotOffset    uint16
	dirBlockBits        uint8
	xattrPrefixCount    uint8
	xattrPrefixStart    uint32
	packedNID           uint64
	xattrFilterReserved uint8
	ishareXattrPrefixID uint8
	buildTime           uint32
	rootNID8            uint64
	metaboxNID          uint64
}

func (s *superblock) blockSize() int64 {
	return int64(1) << s.blockSizeBits
}

func (s *superblock) superblockSize() int {
	return superblockBaseSize + int(s.superblockExtSlots)*superblockExtSlotSize
}

func parseSuperblock(b []byte, imageSize int64) (*superblock, error) {
	if len(b) < superblockBaseSize {
		return nil, fmt.Errorf("superblock is %d bytes, expected at least %d", len(b), superblockBaseSize)
	}
	if imageSize < superOffset+int64(superblockBaseSize) {
		return nil, fmt.Errorf("image is %d bytes, too small for an EROFS superblock", imageSize)
	}
	if binary.LittleEndian.Uint32(b[0:4]) != superMagic {
		return nil, fmt.Errorf("invalid EROFS magic 0x%08x", binary.LittleEndian.Uint32(b[0:4]))
	}

	s := &superblock{
		checksum:            binary.LittleEndian.Uint32(b[4:8]),
		featureCompat:       binary.LittleEndian.Uint32(b[8:12]),
		blockSizeBits:       b[12],
		superblockExtSlots:  b[13],
		rootNID:             binary.LittleEndian.Uint16(b[14:16]),
		inos:                binary.LittleEndian.Uint64(b[16:24]),
		epoch:               binary.LittleEndian.Uint64(b[24:32]),
		fixedNsec:           binary.LittleEndian.Uint32(b[32:36]),
		blocks:              binary.LittleEndian.Uint32(b[36:40]),
		metaBlockAddress:    binary.LittleEndian.Uint32(b[40:44]),
		xattrBlockAddress:   binary.LittleEndian.Uint32(b[44:48]),
		featureIncompat:     binary.LittleEndian.Uint32(b[80:84]),
		availableComprAlgs:  binary.LittleEndian.Uint16(b[84:86]),
		extraDevices:        binary.LittleEndian.Uint16(b[86:88]),
		deviceSlotOffset:    binary.LittleEndian.Uint16(b[88:90]),
		dirBlockBits:        b[90],
		xattrPrefixCount:    b[91],
		xattrPrefixStart:    binary.LittleEndian.Uint32(b[92:96]),
		packedNID:           binary.LittleEndian.Uint64(b[96:104]),
		xattrFilterReserved: b[104],
		ishareXattrPrefixID: b[105],
		buildTime:           binary.LittleEndian.Uint32(b[108:112]),
		rootNID8:            binary.LittleEndian.Uint64(b[112:120]),
	}
	if s.superblockSize() > 128 && len(b) >= 136 {
		s.metaboxNID = binary.LittleEndian.Uint64(b[128:136])
	}
	copy(s.uuid[:], b[48:64])
	copy(s.volumeName[:], b[64:80])

	if s.blockSizeBits < 9 || s.blockSizeBits > 16 {
		return nil, fmt.Errorf("invalid block size 2^%d", s.blockSizeBits)
	}
	if s.superblockSize() > len(b) || int64(superOffset)+int64(s.superblockSize()) > imageSize {
		return nil, fmt.Errorf("invalid superblock extension count %d", s.superblockExtSlots)
	}
	allowedCompat := featureCompatChecksum | featureCompatMtime | featureCompatXattr
	if unknown := s.featureCompat &^ allowedCompat; unknown != 0 {
		return nil, fmt.Errorf("unsupported compatible feature bits 0x%08x", unknown)
	}
	allowedIncompat := featureIncompatLZ4Padding | featureIncompatComprCfgs | featureIncompatFragments
	if unknown := s.featureIncompat &^ allowedIncompat; unknown != 0 {
		return nil, fmt.Errorf("unsupported incompatible feature bits 0x%08x", unknown)
	}
	if s.extraDevices != 0 {
		return nil, fmt.Errorf("multi-device images are not supported")
	}
	if s.dirBlockBits != 0 {
		return nil, fmt.Errorf("directory block size 2^%d is not supported", s.dirBlockBits)
	}
	if s.xattrPrefixCount != 0 || s.xattrPrefixStart != 0 {
		return nil, fmt.Errorf("long xattr prefixes are not supported")
	}
	if s.xattrFilterReserved != 0 {
		return nil, fmt.Errorf("reserved superblock byte 112 is nonzero")
	}
	if s.ishareXattrPrefixID != 0 {
		return nil, fmt.Errorf("shared xattr prefixes are not supported")
	}
	if b[106] != 0 || b[107] != 0 {
		return nil, fmt.Errorf("reserved superblock bytes 106-107 are nonzero")
	}
	reserved3 := uint64(0)
	if s.superblockSize() > 136 && len(b) >= 144 {
		reserved3 = binary.LittleEndian.Uint64(b[136:144])
	}
	if s.rootNID8 != 0 || s.metaboxNID != 0 || binary.LittleEndian.Uint64(b[120:128]) != 0 || reserved3 != 0 {
		return nil, fmt.Errorf("48-bit and metabox layouts are not supported")
	}
	if s.blocks == 0 || s.metaBlockAddress >= s.blocks {
		return nil, fmt.Errorf("invalid metadata block address %d with %d blocks", s.metaBlockAddress, s.blocks)
	}
	if s.packedNID != 0 && s.packedNID>>63 != 0 {
		return nil, fmt.Errorf("invalid packed inode number 0x%016x", s.packedNID)
	}
	rootOffset := int64(s.metaBlockAddress)*s.blockSize() + int64(s.rootNID)*inodeSlotSize
	if rootOffset < 0 || rootOffset+inodeCompactSize > imageSize {
		return nil, fmt.Errorf("root inode offset %d is outside the image", rootOffset)
	}
	if s.blocks > uint32(imageSize/s.blockSize()) {
		return nil, fmt.Errorf("filesystem has %d blocks but image only has %d", s.blocks, imageSize/s.blockSize())
	}
	if s.featureCompat&featureCompatChecksum != 0 {
		checksumLength := int(s.blockSize())
		if checksumLength > superOffset {
			checksumLength -= superOffset
		}
		if len(b) < checksumLength {
			return nil, fmt.Errorf("superblock is %d bytes, expected %d for checksum validation", len(b), checksumLength)
		}
		if actual := superblockChecksum(b[:checksumLength]); actual != s.checksum {
			return nil, fmt.Errorf("invalid superblock checksum 0x%08x, want 0x%08x", s.checksum, actual)
		}
	}
	return s, nil
}

func superblockChecksum(b []byte) uint32 {
	if len(b) < superblockBaseSize {
		return 0
	}
	checksummed := append([]byte(nil), b...)
	binary.LittleEndian.PutUint32(checksummed[4:8], 0)
	return ^crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), checksummed)
}
