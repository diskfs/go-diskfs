package erofs

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

const (
	linuxModeSocket  uint16 = 0xC000
	linuxModeSymlink uint16 = 0xA000
	linuxModeRegular uint16 = 0x8000
	linuxModeBlock   uint16 = 0x6000
	linuxModeDir     uint16 = 0x4000
	linuxModeChar    uint16 = 0x2000
	linuxModeFIFO    uint16 = 0x1000
)

type inode struct {
	nid        uint64
	mode       uint16
	size       int64
	uid        uint32
	gid        uint32
	nlink      uint32
	mtime      time.Time
	inodeSize  int
	xattrSize  int
	dataLayout uint8
	// startBlock is a filesystem block number for flat data (the on-disk union
	// instead stores compressed block counts or a device number for other types).
	startBlock uint64
	rdev       uint32
	compressed *compressedInfo
}

func (i *inode) isDir() bool {
	return i.mode&0xF000 == linuxModeDir
}

func (i *inode) isSymlink() bool {
	return i.mode&0xF000 == linuxModeSymlink
}

func (f *FileSystem) inodeOffset(nid uint64) (int64, error) {
	if nid>>63 != 0 {
		return 0, fmt.Errorf("invalid inode number 0x%016x", nid)
	}
	if f.size < inodeCompactSize {
		return 0, fmt.Errorf("image is too small for inode %d", nid)
	}
	// NIDs count 32-byte inode slots from the metadata area's block address.
	metadataBase := int64(f.superblock.metaBlockAddress) * f.blockSize()
	available := f.size - inodeCompactSize - metadataBase
	if available < 0 || nid > uint64(available)/inodeSlotSize {
		return 0, fmt.Errorf("inode %d offset is outside the image", nid)
	}
	return metadataBase + int64(nid)*inodeSlotSize, nil
}

func (f *FileSystem) getInode(nid uint64) (*inode, error) {
	offset, err := f.inodeOffset(nid)
	if err != nil {
		return nil, err
	}

	readLength := inodeExtendedSize
	if offset+inodeExtendedSize > f.size {
		readLength = inodeCompactSize
	}
	b := make([]byte, readLength)
	if err := f.readAt(b, offset); err != nil {
		return nil, fmt.Errorf("unable to read inode %d: %w", nid, err)
	}
	format := binary.LittleEndian.Uint16(b[0:2])
	if format&inodeFormatReservedMask != 0 {
		return nil, fmt.Errorf("inode %d has unsupported format 0x%04x", nid, format)
	}
	version := format & 1
	layout := uint8((format >> 1) & 0x7)
	if layout > dataLayoutChunked {
		return nil, fmt.Errorf("inode %d has unsupported data layout %d", nid, layout)
	}

	i := &inode{
		nid:        nid,
		dataLayout: layout,
	}
	xattrCount := binary.LittleEndian.Uint16(b[2:4])
	i.mode = binary.LittleEndian.Uint16(b[4:6])
	i.xattrSize = 0
	if xattrCount != 0 {
		i.xattrSize = 12 + 4*(int(xattrCount)-1)
	}

	if version == 0 {
		i.inodeSize = inodeCompactSize
		i.size = int64(binary.LittleEndian.Uint32(b[8:12]))
		i.mtime = time.Unix(int64(f.superblock.epoch)+int64(binary.LittleEndian.Uint32(b[12:16])), int64(f.superblock.fixedNsec)).UTC()
		i.uid = uint32(binary.LittleEndian.Uint16(b[24:26]))
		i.gid = uint32(binary.LittleEndian.Uint16(b[26:28]))
		nb := binary.LittleEndian.Uint16(b[6:8])
		if !i.isDir() && format&(1<<4) != 0 {
			i.nlink = 1
		} else {
			i.nlink = uint32(nb)
			nb = 0
		}
		i.startBlock = uint64(binary.LittleEndian.Uint32(b[16:20])) | uint64(nb)<<32
	} else {
		if readLength < inodeExtendedSize {
			return nil, fmt.Errorf("inode %d extended inode extends beyond the image", nid)
		}
		i.inodeSize = inodeExtendedSize
		i.size = int64(binary.LittleEndian.Uint64(b[8:16]))
		i.startBlock = uint64(binary.LittleEndian.Uint32(b[16:20])) |
			uint64(binary.LittleEndian.Uint16(b[6:8]))<<32
		i.uid = binary.LittleEndian.Uint32(b[24:28])
		i.gid = binary.LittleEndian.Uint32(b[28:32])
		i.mtime = time.Unix(int64(binary.LittleEndian.Uint64(b[32:40])), int64(binary.LittleEndian.Uint32(b[40:44]))).UTC()
		i.nlink = binary.LittleEndian.Uint32(b[44:48])
	}

	switch i.mode & 0xF000 {
	case linuxModeDir, linuxModeRegular, linuxModeSymlink:
		if i.startBlock>>48 != 0 {
			return nil, fmt.Errorf("inode %d uses unsupported 48-bit block addresses", nid)
		}
	case linuxModeChar, linuxModeBlock:
		i.rdev = binary.LittleEndian.Uint32(b[16:20])
	case linuxModeFIFO, linuxModeSocket:
		i.rdev = 0
	default:
		return nil, fmt.Errorf("inode %d has invalid mode 0o%o", nid, i.mode)
	}
	if i.size < 0 {
		return nil, fmt.Errorf("inode %d has negative size %d", nid, i.size)
	}
	if offset+int64(i.inodeSize+i.xattrSize) > f.size {
		return nil, fmt.Errorf("inode %d inline metadata extends beyond the image", nid)
	}
	if layout == dataLayoutChunked {
		return nil, fmt.Errorf("inode %d uses unsupported data layout %d", nid, layout)
	}
	if layout == dataLayoutCompressedFull || layout == dataLayoutCompressedCompact {
		if err := f.initCompressed(i); err != nil {
			return nil, err
		}
	}
	return i, nil
}

func goFileMode(mode uint16) os.FileMode {
	var result os.FileMode
	switch mode & 0xF000 {
	case linuxModeDir:
		result |= os.ModeDir
	case linuxModeSymlink:
		result |= os.ModeSymlink
	case linuxModeChar:
		result |= os.ModeDevice | os.ModeCharDevice
	case linuxModeBlock:
		result |= os.ModeDevice
	case linuxModeFIFO:
		result |= os.ModeNamedPipe
	case linuxModeSocket:
		result |= os.ModeSocket
	}
	if mode&0o4000 != 0 {
		result |= os.ModeSetuid
	}
	if mode&0o2000 != 0 {
		result |= os.ModeSetgid
	}
	if mode&0o1000 != 0 {
		result |= os.ModeSticky
	}
	return result | os.FileMode(mode&0777)
}
