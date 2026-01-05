package ext4

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/diskfs/go-diskfs/filesystem/ext4/crc"
)

type inodeFlag uint32
type fileType uint16

func (i inodeFlag) included(a uint32) bool {
	return a&uint32(i) == uint32(i)
}

const (
	ext2InodeSize uint16 = 128
	// minInodeSize is ext2 + the extra min 32 bytes in ext4
	minInodeExtraSize                uint16    = 32
	wantInodeExtraSize               uint16    = 128
	minInodeSize                     uint16    = ext2InodeSize + minInodeExtraSize
	extentInodeMaxEntries            int       = 4
	inodeFlagSecureDeletion          inodeFlag = 0x1
	inodeFlagPreserveForUndeletion   inodeFlag = 0x2
	inodeFlagCompressed              inodeFlag = 0x4
	inodeFlagSynchronous             inodeFlag = 0x8
	inodeFlagImmutable               inodeFlag = 0x10
	inodeFlagAppendOnly              inodeFlag = 0x20
	inodeFlagNoDump                  inodeFlag = 0x40
	inodeFlagNoAccessTimeUpdate      inodeFlag = 0x80
	inodeFlagDirtyCompressed         inodeFlag = 0x100
	inodeFlagCompressedClusters      inodeFlag = 0x200
	inodeFlagNoCompress              inodeFlag = 0x400
	inodeFlagEncryptedInode          inodeFlag = 0x800
	inodeFlagHashedDirectoryIndexes  inodeFlag = 0x1000
	inodeFlagAFSMagicDirectory       inodeFlag = 0x2000
	inodeFlagAlwaysJournal           inodeFlag = 0x4000
	inodeFlagNoMergeTail             inodeFlag = 0x8000
	inodeFlagSyncDirectoryData       inodeFlag = 0x10000
	inodeFlagTopDirectory            inodeFlag = 0x20000
	inodeFlagHugeFile                inodeFlag = 0x40000
	inodeFlagUsesExtents             inodeFlag = 0x80000
	inodeFlagExtendedAttributes      inodeFlag = 0x200000
	inodeFlagBlocksPastEOF           inodeFlag = 0x400000
	inodeFlagSnapshot                inodeFlag = 0x1000000
	inodeFlagDeletingSnapshot        inodeFlag = 0x4000000
	inodeFlagCompletedSnapshotShrink inodeFlag = 0x8000000
	inodeFlagInlineData              inodeFlag = 0x10000000
	inodeFlagInheritProject          inodeFlag = 0x20000000

	fileTypeFifo            fileType = 0x1000
	fileTypeCharacterDevice fileType = 0x2000
	fileTypeDirectory       fileType = 0x4000
	fileTypeBlockDevice     fileType = 0x6000
	fileTypeRegularFile     fileType = 0x8000
	fileTypeSymbolicLink    fileType = 0xA000
	fileTypeSocket          fileType = 0xC000

	filePermissionsOwnerExecute uint16 = 0x40
	filePermissionsOwnerWrite   uint16 = 0x80
	filePermissionsOwnerRead    uint16 = 0x100
	filePermissionsGroupExecute uint16 = 0x8
	filePermissionsGroupWrite   uint16 = 0x10
	filePermissionsGroupRead    uint16 = 0x20
	filePermissionsOtherExecute uint16 = 0x1
	filePermissionsOtherWrite   uint16 = 0x2
	filePermissionsOtherRead    uint16 = 0x4
	filePermissionsSticky       uint16 = 0x200
	filePermissionsGroupSetgid  uint16 = 0x400
	filePermissionsOwnerSetuid  uint16 = 0x800
)

// mountOptions is a structure holding flags for an inode
type inodeFlags struct {
	secureDeletion          bool
	preserveForUndeletion   bool
	compressed              bool
	synchronous             bool
	immutable               bool
	appendOnly              bool
	noDump                  bool
	noAccessTimeUpdate      bool
	dirtyCompressed         bool
	compressedClusters      bool
	noCompress              bool
	encryptedInode          bool
	hashedDirectoryIndexes  bool
	AFSMagicDirectory       bool
	alwaysJournal           bool
	noMergeTail             bool
	syncDirectoryData       bool
	topDirectory            bool
	hugeFile                bool
	usesExtents             bool
	extendedAttributes      bool
	blocksPastEOF           bool
	snapshot                bool
	deletingSnapshot        bool
	completedSnapshotShrink bool
	inlineData              bool
	inheritProject          bool
}

type filePermissions struct {
	read    bool
	write   bool
	execute bool
	special bool
}

// inode is a structure holding the data about an inode
type inode struct {
	number                 uint32
	permissionsOther       filePermissions
	permissionsGroup       filePermissions
	permissionsOwner       filePermissions
	fileType               fileType
	owner                  uint32
	group                  uint32
	size                   uint64
	accessTime             time.Time
	changeTime             time.Time
	modifyTime             time.Time
	createTime             time.Time
	deletionTime           uint32
	hardLinks              uint16
	blocks                 uint64
	filesystemBlocks       bool
	flags                  *inodeFlags
	version                uint64
	nfsFileVersion         uint32
	extendedAttributeBlock uint64
	inodeSize              uint16
	project                uint32
	extents                extentBlockFinder
	linkTarget             string
}

//nolint:unused // will be used in the future, not yet
func (i *inode) equal(a *inode) bool {
	if (i == nil && a != nil) || (a == nil && i != nil) {
		return false
	}
	if i == nil && a == nil {
		return true
	}
	return *i == *a
}

// inodeFromBytes create an inode struct from bytes
func inodeFromBytes(b []byte, sb *superblock, number uint32) (*inode, error) {
	// safely make sure it is the min size
	if len(b) < int(minInodeSize) {
		return nil, fmt.Errorf("inode data too short: %d bytes, must be min %d bytes", len(b), minInodeSize)
	}

	// checksum before using the data
	checksumBytes := make([]byte, 4)

	// checksum before using the data
	copy(checksumBytes[0:2], b[0x7c:0x7e])
	copy(checksumBytes[2:4], b[0x82:0x84])
	// zero out checksum fields before calculating the checksum
	b[0x7c] = 0
	b[0x7d] = 0
	b[0x82] = 0
	b[0x83] = 0

	// block count, reserved block count and free blocks depends on whether the fs is 64-bit or not
	owner := make([]byte, 4)
	fileSize := make([]byte, 8)
	group := make([]byte, 4)
	version := make([]byte, 8)
	extendedAttributeBlock := make([]byte, 8)

	mode := binary.LittleEndian.Uint16(b[0x0:0x2])

	copy(owner[0:2], b[0x2:0x4])
	copy(owner[2:4], b[0x78:0x7a])
	copy(group[0:2], b[0x18:0x20])
	copy(group[2:4], b[0x7a:0x7c])
	copy(fileSize[0:4], b[0x4:0x8])
	copy(fileSize[4:8], b[0x6c:0x70])
	copy(version[0:4], b[0x24:0x28])
	copy(version[4:8], b[0x98:0x9c])
	copy(extendedAttributeBlock[0:4], b[0x88:0x8c])
	copy(extendedAttributeBlock[4:6], b[0x76:0x78])

	// get the times
	// the structure normally is 0:4 (32 bits) is seconds since the epoch
	// The docs say:
	// If the inode structure size sb->s_inode_size is larger than 128 bytes and the i_inode_extra field is large
	// enough to encompass the respective i_[cma]time_extra field, the ctime, atime, and mtime inode fields are widened
	// to 64 bits. Within this "extra" 32-bit field, the lower two bits are used to extend the 32-bit seconds field to
	// be 34 bit wide; the upper 30 bits are used to provide nanosecond timestamp accuracy.
	//
	// Thus, the full 64-bit timestamp value is constructed as follows:
	//  original 32 bits (0:4) are seconds. Add (to the left) 2 more bits from the 32
	//  the remaining 30 bites are nanoseconds
	accessTimeSeconds := int32(binary.LittleEndian.Uint32(b[0x8:0xc]))
	changeTimeSeconds := int32(binary.LittleEndian.Uint32(b[0xc:0x10]))
	modifyTimeSeconds := int32(binary.LittleEndian.Uint32(b[0x10:0x14]))
	createTimeSeconds := int32(binary.LittleEndian.Uint32(b[0x90:0x94]))

	accessTimeExtra := binary.LittleEndian.Uint32(b[0x8c:0x90])
	changeTimeExtra := binary.LittleEndian.Uint32(b[0x84:0x88])
	modifyTimeExtra := binary.LittleEndian.Uint32(b[0x88:0x8c])
	createTimeExtra := binary.LittleEndian.Uint32(b[0x94:0x98])

	decodeTimestamp := func(seconds int32, extra uint32) (int64, int64) {
		// The formula derived from the kernel documentation table is:
		// Decoded = int64(int32(lower)) + (int64(extra_bits) << 32)
		sec := int64(seconds) + (int64(extra&0x3) << 32)
		// Nanoseconds are in the upper 30 bits
		nano := int64(extra >> 2)
		return sec, nano
	}

	atimeSec, atimeNano := decodeTimestamp(accessTimeSeconds, accessTimeExtra)
	ctimeSec, ctimeNano := decodeTimestamp(changeTimeSeconds, changeTimeExtra)
	mtimeSec, mtimeNano := decodeTimestamp(modifyTimeSeconds, modifyTimeExtra)
	crtimeSec, crtimeNano := decodeTimestamp(createTimeSeconds, createTimeExtra)

	flagsNum := binary.LittleEndian.Uint32(b[0x20:0x24])

	flags := parseInodeFlags(flagsNum)

	blocksLow := binary.LittleEndian.Uint32(b[0x1c:0x20])
	blocksHigh := binary.LittleEndian.Uint16(b[0x74:0x76])
	var (
		blocks           uint64
		filesystemBlocks bool
	)

	hugeFile := sb.features.hugeFile
	switch {
	case !hugeFile:
		// just 512-byte blocks
		blocks = uint64(blocksLow)
		filesystemBlocks = false
	case hugeFile && !flags.hugeFile:
		// larger number of 512-byte blocks
		blocks = uint64(blocksHigh)<<32 + uint64(blocksLow)
		filesystemBlocks = false
	default:
		// larger number of filesystem blocks
		blocks = uint64(blocksHigh)<<32 + uint64(blocksLow)
		filesystemBlocks = true
	}
	fileType := parseFileType(mode)
	fileSizeNum := binary.LittleEndian.Uint64(fileSize)

	extentInfo := make([]byte, 60)
	copy(extentInfo, b[0x28:0x64])
	// symlinks might store link target in extentInfo, or might store them elsewhere
	var (
		linkTarget string
		allExtents extentBlockFinder
		err        error
	)
	if fileType == fileTypeSymbolicLink && fileSizeNum < 60 {
		linkTarget = string(extentInfo[:fileSizeNum])
	} else {
		// parse the extent information in the inode to get the root of the extents tree
		// we do not walk the entire tree, to get a slice of blocks for the file.
		// If we want to do that, we call the extentBlockFinder.blocks() method
		allExtents, err = parseExtents(extentInfo, sb.blockSize, 0, uint32(blocks))
		if err != nil {
			return nil, fmt.Errorf("error parsing extent tree: %v", err)
		}
	}

	i := inode{
		number:                 number,
		permissionsGroup:       parseGroupPermissions(mode),
		permissionsOwner:       parseOwnerPermissions(mode),
		permissionsOther:       parseOtherPermissions(mode),
		fileType:               fileType,
		owner:                  binary.LittleEndian.Uint32(owner),
		group:                  binary.LittleEndian.Uint32(group),
		size:                   fileSizeNum,
		hardLinks:              binary.LittleEndian.Uint16(b[0x1a:0x1c]),
		blocks:                 blocks,
		filesystemBlocks:       filesystemBlocks,
		flags:                  &flags,
		nfsFileVersion:         binary.LittleEndian.Uint32(b[0x64:0x68]),
		version:                binary.LittleEndian.Uint64(version),
		inodeSize:              binary.LittleEndian.Uint16(b[0x80:0x82]) + minInodeSize,
		deletionTime:           binary.LittleEndian.Uint32(b[0x14:0x18]),
		accessTime:             time.Unix(atimeSec, atimeNano),
		changeTime:             time.Unix(ctimeSec, ctimeNano),
		modifyTime:             time.Unix(mtimeSec, mtimeNano),
		createTime:             time.Unix(crtimeSec, crtimeNano),
		extendedAttributeBlock: binary.LittleEndian.Uint64(extendedAttributeBlock),
		project:                binary.LittleEndian.Uint32(b[0x9c:0x100]),
		extents:                allExtents,
		linkTarget:             linkTarget,
	}
	checksum := binary.LittleEndian.Uint32(checksumBytes)
	actualChecksum := inodeChecksum(b, sb.checksumSeed, number, i.nfsFileVersion)

	if actualChecksum != checksum {
		return nil, fmt.Errorf("checksum mismatch, on-disk %x vs calculated %x", checksum, actualChecksum)
	}

	return &i, nil
}

// toBytes returns an inode ready to be written to disk
//
//nolint:unused // will be used in the future, not yet
func (i *inode) toBytes(sb *superblock) []byte {
	iSize := sb.inodeSize

	b := make([]byte, iSize)

	mode := make([]byte, 2)
	owner := make([]byte, 4)
	fileSize := make([]byte, 8)
	group := make([]byte, 4)
	accessTime := make([]byte, 8)
	changeTime := make([]byte, 8)
	modifyTime := make([]byte, 8)
	createTime := make([]byte, 8)
	version := make([]byte, 8)
	extendedAttributeBlock := make([]byte, 8)

	binary.LittleEndian.PutUint16(mode, i.permissionsGroup.toGroupInt()|i.permissionsOther.toOtherInt()|i.permissionsOwner.toOwnerInt()|uint16(i.fileType))
	binary.LittleEndian.PutUint32(owner, i.owner)
	binary.LittleEndian.PutUint32(group, i.group)
	binary.LittleEndian.PutUint64(fileSize, i.size)
	binary.LittleEndian.PutUint64(version, i.version)
	binary.LittleEndian.PutUint64(extendedAttributeBlock, i.extendedAttributeBlock)

	// ext4 timestamps are 32 bits of seconds, plus an extra 32-bit field
	// containing 30 bits of nanoseconds and 2 bits of extended seconds.
	// See https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout#Inode_Timestamps
	encodeAndWriteTimestamp := func(t time.Time, target []byte) {
		seconds := t.Unix()
		nanos := uint32(t.Nanosecond())
		high := uint32((seconds-int64(int32(seconds)))>>32) & 0x3
		extra := (nanos << 2) | high

		binary.LittleEndian.PutUint32(target[0:4], uint32(seconds))
		binary.LittleEndian.PutUint32(target[4:8], extra)
	}

	encodeAndWriteTimestamp(i.accessTime, accessTime)
	encodeAndWriteTimestamp(i.createTime, createTime)
	encodeAndWriteTimestamp(i.changeTime, changeTime)
	encodeAndWriteTimestamp(i.modifyTime, modifyTime)

	blocks := make([]byte, 8)
	binary.LittleEndian.PutUint64(blocks, i.blocks)

	copy(b[0x0:0x2], mode)
	copy(b[0x2:0x4], owner[0:2])
	copy(b[0x4:0x8], fileSize[0:4])
	copy(b[0x8:0xc], accessTime[0:4])
	copy(b[0xc:0x10], changeTime[0:4])
	copy(b[0x10:0x14], modifyTime[0:4])

	binary.LittleEndian.PutUint32(b[0x14:0x18], i.deletionTime)
	copy(b[0x18:0x1a], group[0:2])
	binary.LittleEndian.PutUint16(b[0x1a:0x1c], i.hardLinks)
	copy(b[0x1c:0x20], blocks[0:4])
	binary.LittleEndian.PutUint32(b[0x20:0x24], i.flags.toInt())
	copy(b[0x24:0x28], version[0:4])
	copy(b[0x28:0x64], i.extents.toBytes())
	binary.LittleEndian.PutUint32(b[0x64:0x68], i.nfsFileVersion)
	copy(b[0x68:0x6c], extendedAttributeBlock[0:4])
	copy(b[0x6c:0x70], fileSize[4:8])
	// b[0x70:0x74] is obsolete
	copy(b[0x74:0x76], blocks[4:8])
	copy(b[0x76:0x78], extendedAttributeBlock[4:6])
	copy(b[0x78:0x7a], owner[2:4])
	copy(b[0x7a:0x7c], group[2:4])
	// b[0x7c:0x7e] is for checkeum
	// b[0x7e:0x80] is unused
	binary.LittleEndian.PutUint16(b[0x80:0x82], i.inodeSize-minInodeSize)
	// b[0x82:0x84] is for checkeum
	copy(b[0x84:0x88], changeTime[4:8])
	copy(b[0x88:0x8c], modifyTime[4:8])
	copy(b[0x8c:0x90], accessTime[4:8])
	copy(b[0x90:0x94], createTime[0:4])
	copy(b[0x94:0x98], createTime[4:8])

	actualChecksum := inodeChecksum(b, sb.checksumSeed, i.number, i.nfsFileVersion)
	checksum := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksum, actualChecksum)
	copy(b[0x7c:0x7e], checksum[0:2])
	copy(b[0x82:0x84], checksum[2:4])

	return b
}

func (i *inode) permissionsToMode() os.FileMode {
	var mode os.FileMode

	// Map filetype to filemode
	switch i.fileType {
	case fileTypeRegularFile:
		// no extra bits for regular files
	case fileTypeDirectory:
		mode |= os.ModeDir
	case fileTypeSymbolicLink:
		mode |= os.ModeSymlink
	case fileTypeCharacterDevice:
		mode |= os.ModeDevice | os.ModeCharDevice
	case fileTypeBlockDevice:
		mode |= os.ModeDevice
	case fileTypeFifo:
		mode |= os.ModeNamedPipe
	case fileTypeSocket:
		mode |= os.ModeSocket
	}

	// Map permissions
	if i.permissionsOwner.read {
		mode |= 0o400
	}
	if i.permissionsOwner.write {
		mode |= 0o200
	}
	if i.permissionsOwner.execute {
		mode |= 0o100
	}
	if i.permissionsOwner.special {
		mode |= os.ModeSetuid
	}
	if i.permissionsGroup.read {
		mode |= 0o040
	}
	if i.permissionsGroup.write {
		mode |= 0o020
	}
	if i.permissionsGroup.execute {
		mode |= 0o010
	}
	if i.permissionsGroup.special {
		mode |= os.ModeSetgid
	}
	if i.permissionsOther.read {
		mode |= 0o004
	}
	if i.permissionsOther.write {
		mode |= 0o002
	}
	if i.permissionsOther.execute {
		mode |= 0o001
	}
	if i.permissionsOther.special {
		mode |= os.ModeSticky
	}

	return mode
}

func parseOwnerPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsOwnerExecute == filePermissionsOwnerExecute,
		write:   mode&filePermissionsOwnerWrite == filePermissionsOwnerWrite,
		read:    mode&filePermissionsOwnerRead == filePermissionsOwnerRead,
		special: mode&filePermissionsOwnerSetuid == filePermissionsOwnerSetuid,
	}
}
func parseGroupPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsGroupExecute == filePermissionsGroupExecute,
		write:   mode&filePermissionsGroupWrite == filePermissionsGroupWrite,
		read:    mode&filePermissionsGroupRead == filePermissionsGroupRead,
		special: mode&filePermissionsGroupSetgid == filePermissionsGroupSetgid,
	}
}
func parseOtherPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsOtherExecute == filePermissionsOtherExecute,
		write:   mode&filePermissionsOtherWrite == filePermissionsOtherWrite,
		read:    mode&filePermissionsOtherRead == filePermissionsOtherRead,
		special: mode&filePermissionsSticky == filePermissionsSticky,
	}
}

//nolint:unused // will be used in the future, not yet
func (fp *filePermissions) toOwnerInt() uint16 {
	var mode uint16
	if fp.execute {
		mode |= filePermissionsOwnerExecute
	}
	if fp.write {
		mode |= filePermissionsOwnerWrite
	}
	if fp.read {
		mode |= filePermissionsOwnerRead
	}
	if fp.special {
		mode |= filePermissionsOwnerSetuid
	}
	return mode
}

//nolint:unused // will be used in the future, not yet
func (fp *filePermissions) toOtherInt() uint16 {
	var mode uint16
	if fp.execute {
		mode |= filePermissionsOtherExecute
	}
	if fp.write {
		mode |= filePermissionsOtherWrite
	}
	if fp.read {
		mode |= filePermissionsOtherRead
	}
	if fp.special {
		mode |= filePermissionsSticky
	}
	return mode
}

//nolint:unused // will be used in the future, not yet
func (fp *filePermissions) toGroupInt() uint16 {
	var mode uint16
	if fp.execute {
		mode |= filePermissionsGroupExecute
	}
	if fp.write {
		mode |= filePermissionsGroupWrite
	}
	if fp.read {
		mode |= filePermissionsGroupRead
	}
	if fp.special {
		mode |= filePermissionsGroupSetgid
	}
	return mode
}

// parseFileType from the uint16 mode. The mode is built of bottom 12 bits
// being "any of" several permissions, and thus resolved via AND,
// while the top 4 bits are "only one of" several types, and thus resolved via just equal.
func parseFileType(mode uint16) fileType {
	return fileType(mode & 0xF000)
}

func parseInodeFlags(flags uint32) inodeFlags {
	return inodeFlags{
		secureDeletion:          inodeFlagSecureDeletion.included(flags),
		preserveForUndeletion:   inodeFlagPreserveForUndeletion.included(flags),
		compressed:              inodeFlagCompressed.included(flags),
		synchronous:             inodeFlagSynchronous.included(flags),
		immutable:               inodeFlagImmutable.included(flags),
		appendOnly:              inodeFlagAppendOnly.included(flags),
		noDump:                  inodeFlagNoDump.included(flags),
		noAccessTimeUpdate:      inodeFlagNoAccessTimeUpdate.included(flags),
		dirtyCompressed:         inodeFlagDirtyCompressed.included(flags),
		compressedClusters:      inodeFlagCompressedClusters.included(flags),
		noCompress:              inodeFlagNoCompress.included(flags),
		encryptedInode:          inodeFlagEncryptedInode.included(flags),
		hashedDirectoryIndexes:  inodeFlagHashedDirectoryIndexes.included(flags),
		AFSMagicDirectory:       inodeFlagAFSMagicDirectory.included(flags),
		alwaysJournal:           inodeFlagAlwaysJournal.included(flags),
		noMergeTail:             inodeFlagNoMergeTail.included(flags),
		syncDirectoryData:       inodeFlagSyncDirectoryData.included(flags),
		topDirectory:            inodeFlagTopDirectory.included(flags),
		hugeFile:                inodeFlagHugeFile.included(flags),
		usesExtents:             inodeFlagUsesExtents.included(flags),
		extendedAttributes:      inodeFlagExtendedAttributes.included(flags),
		blocksPastEOF:           inodeFlagBlocksPastEOF.included(flags),
		snapshot:                inodeFlagSnapshot.included(flags),
		deletingSnapshot:        inodeFlagDeletingSnapshot.included(flags),
		completedSnapshotShrink: inodeFlagCompletedSnapshotShrink.included(flags),
		inlineData:              inodeFlagInlineData.included(flags),
		inheritProject:          inodeFlagInheritProject.included(flags),
	}
}

//nolint:unused // will be used in the future, not yet
func (i *inodeFlags) toInt() uint32 {
	var flags uint32

	if i.secureDeletion {
		flags |= uint32(inodeFlagSecureDeletion)
	}
	if i.preserveForUndeletion {
		flags |= uint32(inodeFlagPreserveForUndeletion)
	}
	if i.compressed {
		flags |= uint32(inodeFlagCompressed)
	}
	if i.synchronous {
		flags |= uint32(inodeFlagSynchronous)
	}
	if i.immutable {
		flags |= uint32(inodeFlagImmutable)
	}
	if i.appendOnly {
		flags |= uint32(inodeFlagAppendOnly)
	}
	if i.noDump {
		flags |= uint32(inodeFlagNoDump)
	}
	if i.noAccessTimeUpdate {
		flags |= uint32(inodeFlagNoAccessTimeUpdate)
	}
	if i.dirtyCompressed {
		flags |= uint32(inodeFlagDirtyCompressed)
	}
	if i.compressedClusters {
		flags |= uint32(inodeFlagCompressedClusters)
	}
	if i.noCompress {
		flags |= uint32(inodeFlagNoCompress)
	}
	if i.encryptedInode {
		flags |= uint32(inodeFlagEncryptedInode)
	}
	if i.hashedDirectoryIndexes {
		flags |= uint32(inodeFlagHashedDirectoryIndexes)
	}
	if i.AFSMagicDirectory {
		flags |= uint32(inodeFlagAFSMagicDirectory)
	}
	if i.alwaysJournal {
		flags |= uint32(inodeFlagAlwaysJournal)
	}
	if i.noMergeTail {
		flags |= uint32(inodeFlagNoMergeTail)
	}
	if i.syncDirectoryData {
		flags |= uint32(inodeFlagSyncDirectoryData)
	}
	if i.topDirectory {
		flags |= uint32(inodeFlagTopDirectory)
	}
	if i.hugeFile {
		flags |= uint32(inodeFlagHugeFile)
	}
	if i.usesExtents {
		flags |= uint32(inodeFlagUsesExtents)
	}
	if i.extendedAttributes {
		flags |= uint32(inodeFlagExtendedAttributes)
	}
	if i.blocksPastEOF {
		flags |= uint32(inodeFlagBlocksPastEOF)
	}
	if i.snapshot {
		flags |= uint32(inodeFlagSnapshot)
	}
	if i.deletingSnapshot {
		flags |= uint32(inodeFlagDeletingSnapshot)
	}
	if i.completedSnapshotShrink {
		flags |= uint32(inodeFlagCompletedSnapshotShrink)
	}
	if i.inlineData {
		flags |= uint32(inodeFlagInlineData)
	}
	if i.inheritProject {
		flags |= uint32(inodeFlagInheritProject)
	}

	return flags
}

// inodeChecksum calculate the checksum for an inode
func inodeChecksum(b []byte, checksumSeed, inodeNumber, inodeGeneration uint32) uint32 {
	numberBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(numberBytes, inodeNumber)
	crcResult := crc.CRC32c(checksumSeed, numberBytes)
	genBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(genBytes, inodeGeneration)
	crcResult = crc.CRC32c(crcResult, genBytes)
	checksum := crc.CRC32c(crcResult, b)
	return checksum
}
