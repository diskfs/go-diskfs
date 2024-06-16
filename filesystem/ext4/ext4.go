package ext4

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/ext4/crc"
	"github.com/diskfs/go-diskfs/util"
	uuid "github.com/satori/go.uuid"
)

// SectorSize indicates what the sector size in bytes is
type SectorSize uint16

// BlockSize indicates how many sectors are in a block
type BlockSize uint8

// BlockGroupSize indicates how many blocks are in a group, standardly 8*block_size_in_bytes

const (
	// SectorSize512 is a sector size of 512 bytes, used as the logical size for all ext4 filesystems
	SectorSize512                SectorSize = 512
	minBlocksPerGroup            uint32     = 256
	BootSectorSize               SectorSize = 2 * SectorSize512
	SuperblockSize               SectorSize = 2 * SectorSize512
	BlockGroupFactor             int        = 8
	DefaultInodeRatio            int64      = 8192
	DefaultInodeSize             int64      = 256
	DefaultReservedBlocksPercent uint8      = 5
	DefaultVolumeName                       = "diskfs_ext4"
	minClusterSize               int        = 128
	maxClusterSize               int        = 65529
	bytesPerSlot                 int        = 32
	maxCharsLongFilename         int        = 13
	maxBlocksPerExtent           int        = 32768
	million                      int        = 1000000
	billion                      int        = 1000 * million
	firstNonReservedInode        uint32     = 11 // traditional

	minBlockLogSize int = 10 /* 1024 */
	maxBlockLogSize int = 16 /* 65536 */
	minBlockSize    int = (1 << minBlockLogSize)
	maxBlockSize    int = (1 << maxBlockLogSize)

	max32Num uint64 = math.MaxUint32
	max64Num uint64 = math.MaxUint64

	maxFilesystemSize32Bit uint64 = 16*2 ^ 40
	maxFilesystemSize64Bit uint64 = 1*2 ^ 60

	checksumType uint8 = 1

	// default for log groups per flex group
	defaultLogGroupsPerFlex int = 3

	// fixed inodes
	rootInode       uint32 = 2
	userQuotaInode  uint32 = 3
	groupQuotaInode uint32 = 4
	journalInode    uint32 = 8
	lostFoundInode         = 11 // traditional
)

type Params struct {
	UUID                  *uuid.UUID
	SectorsPerBlock       uint8
	BlocksPerGroup        uint32
	InodeRatio            int64
	InodeCount            uint32
	SparseSuperVersion    uint8
	Checksum              bool
	ClusterSize           int64
	ReservedBlocksPercent uint8
	VolumeName            string
	// JournalDevice external journal device, only checked if WithFeatureSeparateJournalDevice(true) is set
	JournalDevice      string
	LogFlexBlockGroups int
	Features           []FeatureOpt
	DefaultMountOpts   []MountOpt
}

// FileSystem implememnts the FileSystem interface
type FileSystem struct {
	bootSector       []byte
	superblock       *superblock
	groupDescriptors *groupDescriptors
	dataBlockBitmap  bitmap
	inodeBitmap      bitmap
	blockGroups      int64
	size             int64
	start            int64
	file             util.File
}

// Equal compare if two filesystems are equal
func (fs *FileSystem) Equal(a *FileSystem) bool {
	localMatch := fs.file == a.file
	sbMatch := fs.superblock.equal(a.superblock)
	gdMatch := fs.groupDescriptors.equal(a.groupDescriptors)
	return localMatch && sbMatch && gdMatch
}

// Create creates an ext4 filesystem in a given file or device
//
// requires the util.File where to create the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the util.File to create the filesystem,
// and blocksize is is the logical blocksize to use for creating the filesystem
//
// note that you are *not* required to create the filesystem on the entire disk. You could have a disk of size
// 20GB, and create a small filesystem of size 50MB that begins 2GB into the disk.
// This is extremely useful for creating filesystems on disk partitions.
//
// Note, however, that it is much easier to do this using the higher-level APIs at github.com/diskfs/go-diskfs
// which allow you to work directly with partitions, rather than having to calculate (and hopefully not make any errors)
// where a partition starts and ends.
//
// If the provided blocksize is 0, it will use the default of 512 bytes. If it is any number other than 0
// or 512, it will return an error.
//
//nolint:gocyclo // yes, this has high cyclomatic complexity, but we can accept it
func Create(f util.File, size, start, sectorsize int64, p *Params) (*FileSystem, error) {
	// be safe about the params pointer
	if p == nil {
		p = &Params{}
	}

	// sectorsize must be <=0 or exactly SectorSize512 or error
	// because of this, we know we can scale it down to a uint32, since it only can be 512 bytes
	if sectorsize != int64(SectorSize512) && sectorsize > 0 {
		return nil, fmt.Errorf("sectorsize for ext4 must be either 512 bytes or 0, not %d", sectorsize)
	}
	var sectorsize32 = uint32(sectorsize)
	// there almost are no limits on an ext4 fs - theoretically up to 1 YB
	// but we do have to check the max and min size per the requested parameters
	//   if size < minSizeGivenParameters {
	// 	    return nil, fmt.Errorf("requested size is smaller than minimum allowed ext4 size %d for given parameters", minSizeGivenParameters*4)
	//   }
	//   if size > maxSizeGivenParameters {
	//	     return nil, fmt.Errorf("requested size is bigger than maximum ext4 size %d for given parameters", maxSizeGivenParameters*4)
	//   }

	// uuid
	fsuuid := p.UUID
	if fsuuid == nil {
		fsuuid2 := uuid.NewV4()
		fsuuid = &fsuuid2
	}

	// blocksize
	sectorsPerBlock := p.SectorsPerBlock
	userProvidedBlocksize := false
	switch {
	case sectorsPerBlock > 128 || sectorsPerBlock < 2:
		return nil, fmt.Errorf("invalid sectors per block %d, must be between %d and %d sectors", sectorsPerBlock, 2, 128)
	case sectorsPerBlock < 1:
		sectorsPerBlock = 2
	default:
		userProvidedBlocksize = true
	}
	blocksize := uint32(sectorsPerBlock) * sectorsize32

	// how many whole blocks is that?
	numblocks := size / int64(blocksize)

	// recalculate if it was not user provided
	if !userProvidedBlocksize {
		sectorsPerBlockR, blocksizeR, numblocksR := recalculateBlocksize(numblocks, size)
		_, blocksize, numblocks = uint8(sectorsPerBlockR), blocksizeR, numblocksR
	}

	// how many blocks in each block group (and therefore how many block groups)
	// if not provided, by default it is 8*blocksize (in bytes)
	blocksPerGroup := p.BlocksPerGroup
	switch {
	case blocksPerGroup <= 0:
		blocksPerGroup = blocksize * 8
	case blocksPerGroup < minBlocksPerGroup:
		return nil, fmt.Errorf("invalid number of blocks per group %d, must be at least %d", blocksPerGroup, minBlocksPerGroup)
	case blocksPerGroup > 8*blocksize:
		return nil, fmt.Errorf("invalid number of blocks per group %d, must be no larger than 8*blocksize of %d", blocksPerGroup, blocksize)
	case blocksPerGroup%8 != 0:
		return nil, fmt.Errorf("invalid number of blocks per group %d, must be divisible by 8", blocksPerGroup)
	}

	// how many block groups do we have?
	blockGroups := numblocks / int64(blocksPerGroup)

	// track how many free blocks we have
	freeBlocks := numblocks

	clusterSize := p.ClusterSize

	// use our inode ratio to determine how many inodes we should have
	inodeRatio := p.InodeRatio
	if inodeRatio <= 0 {
		inodeRatio = DefaultInodeRatio
	}
	if inodeRatio < int64(blocksize) {
		inodeRatio = int64(blocksize)
	}
	if inodeRatio < clusterSize {
		inodeRatio = clusterSize
	}

	inodeCount := p.InodeCount
	switch {
	case inodeCount <= 0:
		// calculate how many inodes are needed
		inodeCount64 := (numblocks * int64(blocksize)) / inodeRatio
		if uint64(inodeCount64) > max32Num {
			return nil, fmt.Errorf("requested %d inodes, greater than max %d", inodeCount64, max32Num)
		}
		inodeCount = uint32(inodeCount64)
	case uint64(inodeCount) > max32Num:
		return nil, fmt.Errorf("requested %d inodes, greater than max %d", inodeCount, max32Num)
	}

	inodesPerGroup := int64(inodeCount) / blockGroups

	// track how many free inodes we have
	freeInodes := inodeCount

	// which blocks have superblock and GDT?
	var (
		backupSuperblocks            []int64
		backupSuperblockGroupsSparse [2]uint32
	)
	//  0 - primary
	//  ?? - backups
	switch p.SparseSuperVersion {
	case 2:
		// backups in first and last block group
		backupSuperblockGroupsSparse = [2]uint32{0, uint32(blockGroups) - 1}
		backupSuperblocks = []int64{0, 1, blockGroups - 1}
	default:
		backupSuperblockGroups := calculateBackupSuperblockGroups(blockGroups)
		backupSuperblocks = []int64{0}
		for _, bg := range backupSuperblockGroups {
			backupSuperblocks = append(backupSuperblocks, bg*int64(blocksPerGroup))
		}
	}

	freeBlocks -= int64(len(backupSuperblocks))

	var firstDataBlock uint32
	if blocksize == 1024 {
		firstDataBlock = 1
	}

	/*
		size calculations
		we have the total size of the disk from `size uint64`
		we have the sectorsize fixed at SectorSize512

		what do we need to determine or calculate?
		- block size
		- number of blocks
		- number of block groups
		- block groups for superblock and gdt backups
		- in each block group:
				- number of blocks in gdt
				- number of reserved blocks in gdt
				- number of blocks in inode table
				- number of data blocks

		config info:

		[defaults]
			base_features = sparse_super,large_file,filetype,resize_inode,dir_index,ext_attr
			default_mntopts = acl,user_xattr
			enable_periodic_fsck = 0
			blocksize = 4096
			inode_size = 256
			inode_ratio = 16384

		[fs_types]
			ext3 = {
				features = has_journal
			}
			ext4 = {
				features = has_journal,extent,huge_file,flex_bg,uninit_bg,64bit,dir_nlink,extra_isize
				inode_size = 256
			}
			ext4dev = {
				features = has_journal,extent,huge_file,flex_bg,uninit_bg,inline_data,64bit,dir_nlink,extra_isize
				inode_size = 256
				options = test_fs=1
			}
			small = {
				blocksize = 1024
				inode_size = 128
				inode_ratio = 4096
			}
			floppy = {
				blocksize = 1024
				inode_size = 128
				inode_ratio = 8192
			}
			big = {
				inode_ratio = 32768
			}
			huge = {
				inode_ratio = 65536
			}
			news = {
				inode_ratio = 4096
			}
			largefile = {
				inode_ratio = 1048576
				blocksize = -1
			}
			largefile4 = {
				inode_ratio = 4194304
				blocksize = -1
			}
			hurd = {
			     blocksize = 4096
			     inode_size = 128
			}
	*/

	// allocate root directory, single inode
	freeInodes--

	// how many reserved blocks?
	reservedBlocksPercent := p.ReservedBlocksPercent
	if reservedBlocksPercent <= 0 {
		reservedBlocksPercent = DefaultReservedBlocksPercent
	}

	// are checksums enabled?
	gdtChecksumType := gdtChecksumNone
	if p.Checksum {
		gdtChecksumType = gdtChecksumMetadata
	}

	// we do not yet support bigalloc
	var clustersPerGroup = blocksPerGroup

	// inodesPerGroup: once we know how many inodes per group, and how many groups
	//   we will have the total inode count

	volumeName := p.VolumeName
	if volumeName == "" {
		volumeName = DefaultVolumeName
	}

	fflags := defaultFeatureFlags
	for _, flagopt := range p.Features {
		flagopt(&fflags)
	}

	mflags := defaultMiscFlags

	// generate hash seed
	hashSeed := uuid.NewV4()
	hashSeedBytes := hashSeed.Bytes()
	htreeSeed := make([]uint32, 0, 4)
	htreeSeed = append(htreeSeed,
		binary.LittleEndian.Uint32(hashSeedBytes[:4]),
		binary.LittleEndian.Uint32(hashSeedBytes[4:8]),
		binary.LittleEndian.Uint32(hashSeedBytes[8:12]),
		binary.LittleEndian.Uint32(hashSeedBytes[12:16]),
	)

	// create a UUID for the journal
	journalSuperblockUUID := uuid.NewV4()

	// group descriptor size could be 32 or 64, depending on option
	var gdSize uint16
	if fflags.fs64Bit {
		gdSize = groupDescriptorSize64Bit
	}

	var firstMetaBG uint32
	if fflags.metaBlockGroups {
		return nil, fmt.Errorf("meta block groups not yet supported")
	}

	// calculate the maximum number of block groups
	// maxBlockGroups = (maxFSSize) / (blocksPerGroup * blocksize)
	var (
		maxBlockGroups uint64
	)
	if fflags.fs64Bit {
		maxBlockGroups = maxFilesystemSize64Bit / (uint64(blocksPerGroup) * uint64(blocksize))
	} else {
		maxBlockGroups = maxFilesystemSize32Bit / (uint64(blocksPerGroup) * uint64(blocksize))
	}
	reservedGDTBlocks := maxBlockGroups * 32 / maxBlockGroups
	if reservedGDTBlocks > math.MaxUint16 {
		return nil, fmt.Errorf("too many reserved blocks calculated for group descriptor table")
	}

	var (
		journalDeviceNumber uint32
		err                 error
	)
	if fflags.separateJournalDevice && p.JournalDevice != "" {
		journalDeviceNumber, err = journalDevice(p.JournalDevice)
		if err != nil {
			return nil, fmt.Errorf("unable to get journal device: %w", err)
		}
	}

	// get default mount options
	mountOptions := defaultMountOptionsFromOpts(p.DefaultMountOpts)

	// initial KB written. This must be adjusted over time to include:
	// - superblock itself (1KB bytes)
	// - GDT
	// - block bitmap (1KB per block group)
	// - inode bitmap (1KB per block group)
	// - inode tables (inodes per block group * bytes per inode)
	// - root directory

	// for now, we just make it 1024 = 1 KB
	initialKB := 1024

	// only set a project quota inode if the feature was enabled
	var projectQuotaInode uint32
	if fflags.projectQuotas {
		projectQuotaInode = lostFoundInode + 1
		freeInodes--
	}

	// how many log groups per flex group? Depends on if we have flex groups
	logGroupsPerFlex := 0
	if fflags.flexBlockGroups {
		logGroupsPerFlex = defaultLogGroupsPerFlex
		if p.LogFlexBlockGroups > 0 {
			logGroupsPerFlex = p.LogFlexBlockGroups
		}
	}

	// create the superblock - MUST ADD IN OPTIONS
	now, epoch := time.Now(), time.Unix(0, 0)
	sb := superblock{
		inodeCount:                   inodeCount,
		blockCount:                   uint64(numblocks),
		reservedBlocks:               uint64(reservedBlocksPercent) / 100 * uint64(numblocks),
		freeBlocks:                   uint64(freeBlocks),
		freeInodes:                   freeInodes,
		firstDataBlock:               firstDataBlock,
		blockSize:                    blocksize,
		clusterSize:                  uint64(clusterSize),
		blocksPerGroup:               blocksPerGroup,
		clustersPerGroup:             clustersPerGroup,
		inodesPerGroup:               uint32(inodesPerGroup),
		mountTime:                    now,
		writeTime:                    now,
		mountCount:                   0,
		mountsToFsck:                 0,
		filesystemState:              fsStateCleanlyUnmounted,
		errorBehaviour:               errorsContinue,
		minorRevision:                0,
		lastCheck:                    now,
		checkInterval:                0,
		creatorOS:                    osLinux,
		revisionLevel:                1,
		reservedBlocksDefaultUID:     0,
		reservedBlocksDefaultGID:     0,
		firstNonReservedInode:        firstNonReservedInode,
		inodeSize:                    uint16(DefaultInodeSize),
		blockGroup:                   0,
		features:                     fflags,
		uuid:                         fsuuid,
		volumeLabel:                  volumeName,
		lastMountedDirectory:         "/",
		algorithmUsageBitmap:         0, // not used in Linux e2fsprogs
		preallocationBlocks:          0, // not used in Linux e2fsprogs
		preallocationDirectoryBlocks: 0, // not used in Linux e2fsprogs
		reservedGDTBlocks:            uint16(reservedGDTBlocks),
		journalSuperblockUUID:        &journalSuperblockUUID,
		journalInode:                 journalInode,
		journalDeviceNumber:          journalDeviceNumber,
		orphanedInodesStart:          0,
		hashTreeSeed:                 htreeSeed,
		hashVersion:                  hashHalfMD4,
		groupDescriptorSize:          gdSize,
		defaultMountOptions:          *mountOptions,
		firstMetablockGroup:          firstMetaBG,
		mkfsTime:                     now,
		journalBackup:                nil,
		// 64-bit mode features
		inodeMinBytes:                minInodeExtraSize,
		inodeReserveBytes:            wantInodeExtraSize,
		miscFlags:                    mflags,
		raidStride:                   0,
		multiMountPreventionInterval: 0,
		multiMountProtectionBlock:    0,
		raidStripeWidth:              0,
		checksumType:                 checksumType,
		totalKBWritten:               uint64(initialKB),
		errorCount:                   0,
		errorFirstTime:               epoch,
		errorFirstInode:              0,
		errorFirstBlock:              0,
		errorFirstFunction:           "",
		errorFirstLine:               0,
		errorLastTime:                epoch,
		errorLastInode:               0,
		errorLastLine:                0,
		errorLastBlock:               0,
		errorLastFunction:            "",
		mountOptions:                 "", // no mount options until it is mounted
		backupSuperblockBlockGroups:  backupSuperblockGroupsSparse,
		lostFoundInode:               lostFoundInode,
		overheadBlocks:               0,
		checksumSeed:                 crc.CRC32c(0, fsuuid.Bytes()), // according to docs, this should be crc32c(~0, $orig_fs_uuid)
		snapshotInodeNumber:          0,
		snapshotID:                   0,
		snapshotReservedBlocks:       0,
		snapshotStartInode:           0,
		userQuotaInode:               userQuotaInode,
		groupQuotaInode:              groupQuotaInode,
		projectQuotaInode:            projectQuotaInode,
		logGroupsPerFlex:             uint64(logGroupsPerFlex),
	}
	gdt := groupDescriptors{}

	b, err := sb.toBytes()
	if err != nil {
		return nil, fmt.Errorf("error converting Superblock to bytes: %v", err)
	}

	g := gdt.toBytes(gdtChecksumType, sb.checksumSeed)
	// how big should the GDT be?
	gdSize = groupDescriptorSize
	if sb.features.fs64Bit {
		gdSize = groupDescriptorSize64Bit
	}
	gdtSize := int64(gdSize) * numblocks
	// write the superblock and GDT to the various locations on disk
	for _, bg := range backupSuperblocks {
		block := bg * int64(blocksPerGroup)
		blockStart := block * int64(blocksize)
		// allow that the first one requires an offset
		incr := int64(0)
		if block == 0 {
			incr = int64(SectorSize512) * 2
		}

		// write the superblock
		count, err := f.WriteAt(b, incr+blockStart+start)
		if err != nil {
			return nil, fmt.Errorf("error writing Superblock for block %d to disk: %v", block, err)
		}
		if count != int(SuperblockSize) {
			return nil, fmt.Errorf("wrote %d bytes of Superblock for block %d to disk instead of expected %d", count, block, SuperblockSize)
		}

		// write the GDT
		count, err = f.WriteAt(g, incr+blockStart+int64(SuperblockSize)+start)
		if err != nil {
			return nil, fmt.Errorf("error writing GDT for block %d to disk: %v", block, err)
		}
		if count != int(gdtSize) {
			return nil, fmt.Errorf("wrote %d bytes of GDT for block %d to disk instead of expected %d", count, block, gdtSize)
		}
	}

	// create root directory
	// there is nothing in there
	return &FileSystem{
		bootSector:       []byte{},
		superblock:       &sb,
		groupDescriptors: &gdt,
		blockGroups:      blockGroups,
		size:             size,
		start:            start,
		file:             f,
	}, nil
}

// Read reads a filesystem from a given disk.
//
// requires the util.File where to read the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the util.File the filesystem is expected to begin,
// and blocksize is is the logical blocksize to use for creating the filesystem
//
// note that you are *not* required to read a filesystem on the entire disk. You could have a disk of size
// 20GB, and a small filesystem of size 50MB that begins 2GB into the disk.
// This is extremely useful for working with filesystems on disk partitions.
//
// Note, however, that it is much easier to do this using the higher-level APIs at github.com/diskfs/go-diskfs
// which allow you to work directly with partitions, rather than having to calculate (and hopefully not make any errors)
// where a partition starts and ends.
//
// If the provided blocksize is 0, it will use the default of 512 bytes. If it is any number other than 0
// or 512, it will return an error.
func Read(file util.File, size, start, sectorsize int64) (*FileSystem, error) {
	// blocksize must be <=0 or exactly SectorSize512 or error
	if sectorsize != int64(SectorSize512) && sectorsize > 0 {
		return nil, fmt.Errorf("sectorsize for ext4 must be either 512 bytes or 0, not %d", sectorsize)
	}
	// we do not check for ext4 max size because it is theoreticallt 1YB, which is bigger than an int64! Even 1ZB is!
	if size < Ext4MinSize {
		return nil, fmt.Errorf("requested size is smaller than minimum allowed ext4 size %d", Ext4MinSize)
	}

	// load the information from the disk
	// read boot sector code
	bs := make([]byte, BootSectorSize)
	n, err := file.ReadAt(bs, start)
	if err != nil {
		return nil, fmt.Errorf("could not read boot sector bytes from file: %v", err)
	}
	if uint16(n) < uint16(BootSectorSize) {
		return nil, fmt.Errorf("only could read %d boot sector bytes from file", n)
	}

	// read the superblock
	// the superblock is one minimal block, i.e. 2 sectors
	superblockBytes := make([]byte, SuperblockSize)
	n, err = file.ReadAt(superblockBytes, start+int64(BootSectorSize))
	if err != nil {
		return nil, fmt.Errorf("could not read superblock bytes from file: %v", err)
	}
	if uint16(n) < uint16(SuperblockSize) {
		return nil, fmt.Errorf("only could read %d superblock bytes from file", n)
	}

	// convert the bytes into a superblock structure
	sb, err := superblockFromBytes(superblockBytes)
	if err != nil {
		return nil, fmt.Errorf("could not interpret superblock data: %v", err)
	}

	// now read the GDT
	// how big should the GDT be?
	gdtSize := uint64(sb.groupDescriptorSize) * sb.blockGroupCount()

	gdtBytes := make([]byte, gdtSize)
	n, err = file.ReadAt(gdtBytes, start+int64(BootSectorSize)+int64(SuperblockSize))
	if err != nil {
		return nil, fmt.Errorf("could not read Group Descriptor Table bytes from file: %v", err)
	}
	if uint64(n) < gdtSize {
		return nil, fmt.Errorf("only could read %d Group Descriptor Table bytes from file instead of %d", n, gdtSize)
	}
	gdt, err := groupDescriptorsFromBytes(gdtBytes, sb.groupDescriptorSize, sb.checksumSeed, sb.gdtChecksumType())
	if err != nil {
		return nil, fmt.Errorf("could not interpret Group Descriptor Table data: %v", err)
	}

	return &FileSystem{
		bootSector:       bs,
		superblock:       sb,
		groupDescriptors: gdt,
		blockGroups:      int64(sb.blockGroupCount()),
		size:             size,
		start:            start,
		file:             file,
	}, nil
}

// Type returns the type code for the filesystem. Always returns filesystem.TypeExt4
func (fs *FileSystem) Type() filesystem.Type {
	return filesystem.TypeExt4
}

// Mkdir make a directory at the given path. It is equivalent to `mkdir -p`, i.e. idempotent, in that:
//
// * It will make the entire tree path if it does not exist
// * It will not return an error if the path already exists
func (fs *FileSystem) Mkdir(p string) error {
	_, _, err := fs.readDirWithMkdir(p, true)
	// we are not interesting in returning the entries
	return err
}

// ReadDir return the contents of a given directory in a given filesystem.
//
// Returns a slice of os.FileInfo with all of the entries in the directory.
//
// Will return an error if the directory does not exist or is a regular file and not a directory
func (fs *FileSystem) ReadDir(p string) ([]os.FileInfo, error) {
	_, entries, err := fs.readDirWithMkdir(p, false)
	if err != nil {
		return nil, fmt.Errorf("error reading directory %s: %v", p, err)
	}
	// once we have made it here, looping is done. We have found the final entry
	// we need to return all of the file info
	count := len(entries)
	ret := make([]os.FileInfo, count)
	for i, e := range entries {
		in, err := fs.readInode(e.inode)
		if err != nil {
			return nil, fmt.Errorf("could not read inode %d at position %d in directory: %v", e.inode, i, err)
		}
		ret[i] = FileInfo{
			modTime: in.modifyTime,
			name:    e.filename,
			size:    int64(in.size),
			isDir:   e.fileType == dirFileTypeDirectory,
		}
	}

	return ret, nil
}

// OpenFile returns an io.ReadWriter from which you can read the contents of a file
// or write contents to the file
//
// accepts normal os.OpenFile flags
//
// returns an error if the file does not exist
func (fs *FileSystem) OpenFile(p string, flag int) (filesystem.File, error) {
	// get the path
	dir := path.Dir(p)
	filename := path.Base(p)
	// if the dir == filename, then it is just /
	if dir == filename {
		return nil, fmt.Errorf("cannot open directory %s as file", p)
	}
	// get the directory entries
	parentDir, entries, err := fs.readDirWithMkdir(dir, false)
	if err != nil {
		return nil, fmt.Errorf("could not read directory entries for %s", dir)
	}
	// we now know that the directory exists, see if the file exists
	var targetEntry *directoryEntry
	for _, e := range entries {
		if e.filename != filename {
			continue
		}
		// cannot do anything with directories
		if e.fileType == dirFileTypeDirectory {
			return nil, fmt.Errorf("cannot open directory %s as file", p)
		}
		// if we got this far, we have found the file
		targetEntry = e
		break
	}

	// see if the file exists
	// if the file does not exist, and is not opened for os.O_CREATE, return an error
	if targetEntry == nil {
		if flag&os.O_CREATE == 0 {
			return nil, fmt.Errorf("target file %s does not exist and was not asked to create", p)
		}
		// else create it
		targetEntry, err = fs.mkFile(parentDir, filename)
		if err != nil {
			return nil, fmt.Errorf("failed to create file %s: %v", p, err)
		}
	}
	// get the inode
	inodeNumber := targetEntry.inode
	inode, err := fs.readInode(inodeNumber)
	if err != nil {
		return nil, fmt.Errorf("could not read inode number %d: %v", inodeNumber, err)
	}

	// if a symlink, read the target, rather than the inode itself, which does not point to anything
	if inode.fileType == fileTypeSymbolicLink {
		// is the symlink relative or absolute?
		linkTarget := inode.linkTarget
		if !path.IsAbs(linkTarget) {
			// convert it into an absolute path
			// and start the process again
			linkTarget = path.Join(dir, linkTarget)
			// we probably could make this more efficient by checking if the final linkTarget
			// is in the same directory as we already are parsing, rather than walking the whole thing again
			// leave that for the future.
			linkTarget = path.Clean(linkTarget)
		}
		return fs.OpenFile(linkTarget, flag)
	}
	offset := int64(0)
	if flag&os.O_APPEND == os.O_APPEND {
		offset = int64(inode.size)
	}
	// when we open a file, we load the inode but also all of the extents
	extents, err := inode.extents.blocks(fs)
	if err != nil {
		return nil, fmt.Errorf("could not read extent tree for inode %d: %v", inodeNumber, err)
	}
	return &File{
		directoryEntry: targetEntry,
		inode:          inode,
		isReadWrite:    flag&os.O_RDWR != 0,
		isAppend:       flag&os.O_APPEND != 0,
		offset:         offset,
		filesystem:     fs,
		extents:        extents,
	}, nil
}

// Label read the volume label
func (fs *FileSystem) Label() string {
	if fs.superblock == nil {
		return ""
	}
	return fs.superblock.volumeLabel
}

// SetLabel changes the label on the writable filesystem. Different file system may hav different
// length constraints.
//
//nolint:revive // will use params when read-write
func (fs *FileSystem) SetLabel(label string) error {
	return errors.New("cannot set label, filesystem currently read-only")
}

// readInode read a single inode from disk
func (fs *FileSystem) readInode(inodeNumber uint32) (*inode, error) {
	if inodeNumber == 0 {
		return nil, fmt.Errorf("cannot read inode 0")
	}
	sb := fs.superblock
	inodeSize := sb.inodeSize
	inodesPerGroup := sb.inodesPerGroup
	// figure out which block group the inode is on
	bg := (inodeNumber - 1) / inodesPerGroup
	// read the group descriptor to find out the location of the inode table
	gd := fs.groupDescriptors.descriptors[bg]
	inodeTableBlock := gd.inodeTableLocation
	inodeBytes := make([]byte, inodeSize)
	// bytesStart is beginning byte for the inodeTableBlock
	byteStart := inodeTableBlock * uint64(sb.blockSize)
	// offsetInode is how many inodes in our inode is
	offsetInode := (inodeNumber - 1) % inodesPerGroup
	// offset is how many bytes in our inode is
	offset := offsetInode * uint32(inodeSize)
	read, err := fs.file.ReadAt(inodeBytes, int64(byteStart)+int64(offset))
	if err != nil {
		return nil, fmt.Errorf("failed to read inode %d from offset %d of block %d from block group %d: %v", inodeNumber, offset, inodeTableBlock, bg, err)
	}
	if read != int(inodeSize) {
		return nil, fmt.Errorf("read %d bytes for inode %d instead of inode size of %d", read, inodeNumber, inodeSize)
	}
	inode, err := inodeFromBytes(inodeBytes, sb, inodeNumber)
	if err != nil {
		return nil, fmt.Errorf("could not interpret inode data: %v", err)
	}
	// fill in symlink target if needed
	if inode.fileType == fileTypeSymbolicLink && inode.linkTarget == "" {
		// read the symlink target
		extents, err := inode.extents.blocks(fs)
		if err != nil {
			return nil, fmt.Errorf("could not read extent tree for symlink inode %d: %v", inodeNumber, err)
		}
		b, err := fs.readFileBytes(extents)
		if err != nil {
			return nil, fmt.Errorf("could not read symlink target for inode %d: %v", inodeNumber, err)
		}
		inode.linkTarget = string(b)
	}
	return inode, nil
}

// writeInode write a single inode to disk
func (fs *FileSystem) writeInode(i *inode) error {
	sb := fs.superblock
	inodeSize := sb.inodeSize
	inodesPerGroup := sb.inodesPerGroup
	// figure out which block group the inode is on
	bg := (i.number - 1) / inodesPerGroup
	// read the group descriptor to find out the location of the inode table
	gd := fs.groupDescriptors.descriptors[bg]
	inodeTableBlock := gd.inodeTableLocation
	// bytesStart is beginning byte for the inodeTableBlock
	//   byteStart := inodeTableBlock * sb.blockSize
	// offsetInode is how many inodes in our inode is
	offsetInode := (i.number - 1) % inodesPerGroup
	// offset is how many bytes in our inode is
	offset := int64(offsetInode) * int64(inodeSize)
	inodeBytes := i.toBytes(sb)
	wrote, err := fs.file.WriteAt(inodeBytes, offset)
	if err != nil {
		return fmt.Errorf("failed to write inode %d at offset %d of block %d from block group %d: %v", i.number, offset, inodeTableBlock, bg, err)
	}
	if wrote != int(inodeSize) {
		return fmt.Errorf("wrote %d bytes for inode %d instead of inode size of %d", wrote, i.number, inodeSize)
	}
	return nil
}

// read directory entries for a given directory
func (fs *FileSystem) readDirectory(inodeNumber uint32) ([]*directoryEntry, error) {
	// read the inode for the directory
	in, err := fs.readInode(inodeNumber)
	if err != nil {
		return nil, fmt.Errorf("could not read inode %d for directory: %v", inodeNumber, err)
	}
	// convert the extent tree into a sorted list of extents
	extents, err := in.extents.blocks(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to get blocks for inode %d: %w", in.number, err)
	}
	// read the contents of the file across all blocks
	b, err := fs.readFileBytes(extents)
	if err != nil {
		return nil, fmt.Errorf("error reading file bytes for inode %d: %v", inodeNumber, err)
	}

	var dirEntries []*directoryEntry
	// TODO: none of this works for hashed dir entries, indicated by in.flags.hashedDirectoryIndexes == true
	if in.flags.hashedDirectoryIndexes {
		treeRoot, err := parseDirectoryTreeRoot(b[:fs.superblock.blockSize], fs.superblock.features.largeDirectory)
		if err != nil {
			return nil, fmt.Errorf("failed to parse directory tree root: %v", err)
		}
		subDirEntries, err := parseDirEntriesHashed(b, treeRoot.depth, treeRoot, fs.superblock.blockSize, fs.superblock.features.metadataChecksums, in.number, in.nfsFileVersion, fs.superblock.checksumSeed)
		if err != nil {
			return nil, fmt.Errorf("failed to parse hashed directory entries: %v", err)
		}
		// include the dot and dotdot entries from treeRoot; they do not show up in the hashed entries
		dirEntries = []*directoryEntry{treeRoot.dotEntry, treeRoot.dotDotEntry}
		dirEntries = append(dirEntries, subDirEntries...)
	} else {
		// convert into directory entries
		dirEntries, err = parseDirEntriesLinear(b, fs.superblock.features.metadataChecksums, fs.superblock.blockSize, in.number, in.nfsFileVersion, fs.superblock.checksumSeed)
	}

	return dirEntries, err
}

// readFileBytes read all of the bytes for an individual file pointed at by a given inode
// normally not very useful, but helpful when reading an entire directory.
func (fs *FileSystem) readFileBytes(extents extents) ([]byte, error) {
	// walk through each one, gobbling up the bytes
	b := make([]byte, 0, fs.superblock.blockSize)
	for i, e := range extents {
		start := e.startingBlock * uint64(fs.superblock.blockSize)
		count := uint64(e.count) * uint64(fs.superblock.blockSize)
		b2 := make([]byte, count)
		read, err := fs.file.ReadAt(b2, int64(start))
		if err != nil {
			return nil, fmt.Errorf("failed to read bytes for extent %d: %v", i, err)
		}
		if read != int(count) {
			return nil, fmt.Errorf("read %d bytes instead of %d for extent %d", read, count, i)
		}
		b = append(b, b2...)
	}
	return b, nil
}

//nolint:revive // params are unused because this still is read-only, but it will be read-write at some point
func (fs *FileSystem) writeDirectoryEntries(dir *Directory) error {
	return errors.New("unsupported write directory entries, currently read-only")
}

// make a file
//
//nolint:revive // params are unused because this still is read-only, but it will be read-write at some point
func (fs *FileSystem) mkFile(parent *Directory, name string) (*directoryEntry, error) {
	return nil, errors.New("unsupported to create a file, currently read-only")
}

// readDirWithMkdir - walks down a directory tree to the last entry
// if it does not exist, it may or may not make it
func (fs *FileSystem) readDirWithMkdir(p string, doMake bool) (*Directory, []*directoryEntry, error) {
	paths := splitPath(p)

	// walk down the directory tree until all paths have been walked or we cannot find something
	// start with the root directory
	var entries []*directoryEntry
	currentDir := &Directory{
		directoryEntry: directoryEntry{
			inode:    rootInode,
			filename: "",
			fileType: dirFileTypeDirectory,
		},
	}
	entries, err := fs.readDirectory(rootInode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read directory %s", "/")
	}
	for i, subp := range paths {
		// do we have an entry whose name is the same as this name?
		found := false
		for _, e := range entries {
			if e.filename != subp {
				continue
			}
			if e.fileType != dirFileTypeDirectory {
				return nil, nil, fmt.Errorf("cannot create directory at %s since it is a file", "/"+strings.Join(paths[0:i+1], "/"))
			}
			// the filename matches, and it is a subdirectory, so we can break after saving the directory entry, which contains the inode
			found = true
			currentDir = &Directory{
				directoryEntry: *e,
			}
			break
		}

		// if not, either make it, retrieve its cluster and entries, and loop;
		//  or error out
		if !found {
			if doMake {
				var subdirEntry *directoryEntry
				subdirEntry, err = fs.mkSubdir(currentDir, subp)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to create subdirectory %s", "/"+strings.Join(paths[0:i+1], "/"))
				}
				// write the directory entries to disk
				err = fs.writeDirectoryEntries(currentDir)
				if err != nil {
					return nil, nil, fmt.Errorf("error writing directory entries to disk: %v", err)
				}
				// save where we are to search next
				currentDir = &Directory{
					directoryEntry: *subdirEntry,
				}
			} else {
				return nil, nil, fmt.Errorf("path %s not found", "/"+strings.Join(paths[0:i+1], "/"))
			}
		}
		// get all of the entries in this directory
		entries, err = fs.readDirectory(currentDir.inode)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read directory %s", "/"+strings.Join(paths[0:i+1], "/"))
		}
	}
	// once we have made it here, looping is done; we have found the final entry
	return currentDir, entries, nil
}

// readBlock read a single block from disk
func (fs *FileSystem) readBlock(blockNumber uint64) ([]byte, error) {
	sb := fs.superblock
	// bytesStart is beginning byte for the inodeTableBlock
	byteStart := blockNumber * uint64(sb.blockSize)
	blockBytes := make([]byte, sb.blockSize)
	read, err := fs.file.ReadAt(blockBytes, int64(byteStart))
	if err != nil {
		return nil, fmt.Errorf("failed to read block %d: %v", blockNumber, err)
	}
	if read != int(sb.blockSize) {
		return nil, fmt.Errorf("read %d bytes for block %d instead of size of %d", read, blockNumber, sb.blockSize)
	}
	return blockBytes, nil
}

// recalculate blocksize based on the existing number of blocks
// -      0 <= blocks <   3MM         : floppy - blocksize = 1024
// -    3MM <= blocks < 512MM         : small - blocksize = 1024
// - 512MM <= blocks < 4*1024*1024MM  : default - blocksize =
// - 4*1024*1024MM <= blocks < 16*1024*1024MM  : big - blocksize =
// - 16*1024*1024MM <= blocks   : huge - blocksize =
//
// the original code from e2fsprogs https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/misc/mke2fs.c
func recalculateBlocksize(numblocks, size int64) (sectorsPerBlock int, blocksize uint32, numBlocksAdjusted int64) {
	var (
		million64     = int64(million)
		sectorSize512 = uint32(SectorSize512)
	)
	switch {
	case 0 <= numblocks && numblocks < 3*million64:
		sectorsPerBlock = 2
		blocksize = 2 * sectorSize512
	case 3*million64 <= numblocks && numblocks < 512*million64:
		sectorsPerBlock = 2
		blocksize = 2 * sectorSize512
	case 512*million64 <= numblocks && numblocks < 4*1024*1024*million64:
		sectorsPerBlock = 2
		blocksize = 2 * sectorSize512
	case 4*1024*1024*million64 <= numblocks && numblocks < 16*1024*1024*million64:
		sectorsPerBlock = 2
		blocksize = 2 * sectorSize512
	case numblocks > 16*1024*1024*million64:
		sectorsPerBlock = 2
		blocksize = 2 * sectorSize512
	}
	return sectorsPerBlock, blocksize, size / int64(blocksize)
}

// mkSubdir make a subdirectory of a given name inside the parent
//
//nolint:revive // params are unused because this still is read-only, but it will be read-write at some point
func (fs *FileSystem) mkSubdir(parent *Directory, name string) (*directoryEntry, error) {
	return nil, errors.New("mksubdir not yet supported")
}
