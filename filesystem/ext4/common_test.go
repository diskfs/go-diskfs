package ext4

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const (
	imgFile             = "testdata/dist/ext4.img"
	imgFileOffset       = "testdata/dist/ext4-offset.img"
	fooDirFile          = "testdata/dist/foo_dir.txt"
	testGDTFile         = "testdata/dist/gdt.bin"
	rootDirFile         = "testdata/dist/root_dir.txt"
	testRootDirFile     = "testdata/dist/root_directory.bin"
	testSuperblockFile  = "testdata/dist/superblock.bin"
	testFilesystemStats = "testdata/dist/stats.txt"
	testKBWrittenFile   = "testdata/dist/lifetime_kb.txt"
)

// TestMain sets up the test environment and runs the tests
func TestMain(m *testing.M) {
	// Check and generate artifacts if necessary
	needGen := false
	if _, err := os.Stat(imgFile); os.IsNotExist(err) {
		needGen = true
	}
	if _, err := os.Stat(imgFileOffset); os.IsNotExist(err) {
		needGen = true
	}
	if needGen {
		// Run the genartifacts.sh script
		cmd := exec.Command("sh", "buildimg.sh")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = "testdata"

		// Execute the command
		if err := cmd.Run(); err != nil {
			println("error generating test artifacts for ext4", err)
			os.Exit(1)
		}
	}

	// Run the tests
	code := m.Run()

	// Exit with the appropriate code
	os.Exit(code)
}

type testGDTLineHandler struct {
	re      *regexp.Regexp
	handler func(*groupDescriptor, []string) error
}

var (
	gdtRELines = []testGDTLineHandler{
		{regexp.MustCompile(`^Group (\d+): \(Blocks (\d+)-(\d+)\) csum 0x([0-9a-f]+) \[(.*)]`), func(gd *groupDescriptor, matches []string) error {
			// group number
			number, err := strconv.ParseUint(matches[1], 10, 16)
			if err != nil {
				return fmt.Errorf("failed to parse group number: %v", err)
			}
			gd.number = uint16(number)
			// parse the flags
			flags := strings.Split(matches[5], ",")
			for _, flag := range flags {
				switch strings.TrimSpace(flag) {
				case "ITABLE_ZEROED":
					gd.flags.inodeTableZeroed = true
				case "INODE_UNINIT":
					gd.flags.inodesUninitialized = true
				case "BLOCK_UNINIT":
					gd.flags.blockBitmapUninitialized = true
				default:
					return fmt.Errorf("unknown flag %s", flag)
				}
			}
			return nil
		}},
		{regexp.MustCompile(`Block bitmap at (\d+) \(.*\), csum (0x[0-9A-Fa-f]+)$`), func(gd *groupDescriptor, matches []string) error {
			// block bitmap
			blockBitmap, err := strconv.ParseUint(matches[1], 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse block bitmap: %v", err)
			}
			gd.blockBitmapLocation = blockBitmap
			// block bitmap checksum
			blockBitmapChecksum, err := strconv.ParseUint(matches[2], 0, 32)
			if err != nil {
				return fmt.Errorf("failed to parse block bitmap checksum: %v", err)
			}
			gd.blockBitmapChecksum = uint32(blockBitmapChecksum)
			return nil

		}},
		{regexp.MustCompile(`Inode bitmap at (\d+) \(.*\), csum (0x[0-9a-fA-F]+)$`), func(gd *groupDescriptor, matches []string) error {
			// inode bitmap
			inodeBitmap, err := strconv.ParseUint(matches[1], 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse inode bitmap: %v", err)
			}
			gd.inodeBitmapLocation = inodeBitmap
			// inode bitmap checksum
			inodeBitmapChecksum, err := strconv.ParseUint(matches[2], 0, 32)
			if err != nil {
				return fmt.Errorf("failed to parse inode bitmap checksum: %v", err)
			}
			gd.inodeBitmapChecksum = uint32(inodeBitmapChecksum)
			return nil

		}},
		{regexp.MustCompile(`Inode table at (\d+)-(\d+) (.*)$`), func(gd *groupDescriptor, matches []string) error {
			// inode table location
			inodeTableStart, err := strconv.ParseUint(matches[1], 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse inode table start: %v", err)
			}
			gd.inodeTableLocation = inodeTableStart
			return nil
		}},
		{regexp.MustCompile(`(\d+) free blocks, (\d+) free inodes, (\d+) directories(, (\d+) unused inodes)?`), func(gd *groupDescriptor, matches []string) error {
			// free blocks
			freeBlocks, err := strconv.ParseUint(matches[1], 10, 32)
			if err != nil {
				return fmt.Errorf("failed to parse free blocks: %v", err)
			}
			gd.freeBlocks = uint32(freeBlocks)
			// free inodes
			freeInodes, err := strconv.ParseUint(matches[2], 10, 32)
			if err != nil {
				return fmt.Errorf("failed to parse free inodes: %v", err)
			}
			gd.freeInodes = uint32(freeInodes)
			// directories
			directories, err := strconv.ParseUint(matches[3], 10, 32)
			if err != nil {
				return fmt.Errorf("failed to parse directories: %v", err)
			}
			gd.usedDirectories = uint32(directories)
			// unused inodes
			if len(matches) > 5 && matches[5] != "" {
				unusedInodes, err := strconv.ParseUint(matches[5], 10, 32)
				if err != nil {
					return fmt.Errorf("failed to parse unused inodes: %v", err)
				}
				gd.unusedInodes = uint32(unusedInodes)
			}
			return nil
		}},
	}
)

type testSuperblockFunc func(*superblock, string) error

var testSuperblockFuncs = map[string]testSuperblockFunc{
	"Filesystem state": func(sb *superblock, value string) error {
		switch value {
		case "clean":
			sb.filesystemState = fsStateCleanlyUnmounted
		default:
			sb.filesystemState = fsStateErrors
		}
		return nil
	},
	"Errors behavior": func(sb *superblock, value string) error {
		switch value {
		case "Continue":
			sb.errorBehaviour = errorsContinue
		default:
			sb.errorBehaviour = errorsPanic
		}
		return nil
	},
	"Last mounted on": func(sb *superblock, value string) error {
		sb.lastMountedDirectory = value
		return nil
	},
	"Filesystem UUID": func(sb *superblock, value string) error {
		uuid, err := uuid.Parse(value)
		if err != nil {
			return err
		}
		sb.uuid = &uuid
		return nil
	},
	"Filesystem magic number": func(_ *superblock, value string) error {
		if value != "0xEF53" {
			return fmt.Errorf("invalid magic number %s", value)
		}
		return nil
	},
	"Filesystem volume name": func(sb *superblock, value string) error {
		if value != "<none>" {
			sb.volumeLabel = value
		}
		return nil
	},
	"Filesystem revision #": func(sb *superblock, value string) error {
		// just need the first part, as it sometimes looks like: 1 (dynamic)
		value = strings.Split(value, " ")[0]
		rev, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("filesystem revision: %w", err)
		}
		sb.revisionLevel = uint32(rev)
		return nil
	},
	"Inode count": func(sb *superblock, value string) error {
		inodeCount, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Inode count: %w", err)
		}
		sb.inodeCount = uint32(inodeCount)
		return nil
	},
	"Block count": func(sb *superblock, value string) error {
		blockCount, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("Block count: %w", err)
		}
		sb.blockCount = blockCount
		return nil
	},
	"Reserved block count": func(sb *superblock, value string) error {
		reservedBlocks, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("Reserved block count: %w", err)
		}
		sb.reservedBlocks = reservedBlocks
		return nil
	},
	"Overhead clusters": func(sb *superblock, value string) error {
		overheadBlocks, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Overhead clusters: %w", err)
		}
		sb.overheadBlocks = uint32(overheadBlocks)
		return nil
	},
	"Free blocks": func(sb *superblock, value string) error {
		freeBlocks, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("Free blocks: %w", err)
		}
		sb.freeBlocks = freeBlocks
		return nil
	},
	"Free inodes": func(sb *superblock, value string) error {
		freeInodes, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Free inodes: %w", err)
		}
		sb.freeInodes = uint32(freeInodes)
		return nil
	},
	"First block": func(sb *superblock, value string) error {
		firstBlock, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("First block: %w", err)
		}
		sb.firstDataBlock = uint32(firstBlock)
		return nil
	},
	"Block size": func(sb *superblock, value string) error {
		blockSize, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Block size: %w", err)
		}
		sb.blockSize = uint32(blockSize)
		return nil
	},
	"Group descriptor size": func(sb *superblock, value string) error {
		groupDescriptorSize, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return fmt.Errorf("Group descriptor size: %w", err)
		}
		sb.groupDescriptorSize = uint16(groupDescriptorSize)
		return nil
	},
	"Reserved GDT blocks": func(sb *superblock, value string) error {
		reservedGDTBlocks, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return fmt.Errorf("Reserved GDT blocks: %w", err)
		}
		sb.reservedGDTBlocks = uint16(reservedGDTBlocks)
		sb.features.reservedGDTBlocksForExpansion = true
		return nil
	},
	"Blocks per group": func(sb *superblock, value string) error {
		blocksPerGroup, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Blocks per group: %w", err)
		}
		sb.blocksPerGroup = uint32(blocksPerGroup)
		return nil
	},
	"Inodes per group": func(sb *superblock, value string) error {
		inodesPerGroup, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Inodes per group: %w", err)
		}
		sb.inodesPerGroup = uint32(inodesPerGroup)
		return nil
	},
	"Flex block group size": func(sb *superblock, value string) error {
		flexBlockGroupSize, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("Flex block group size: %w", err)
		}
		sb.logGroupsPerFlex = flexBlockGroupSize
		return nil
	},
	"Filesystem created": func(sb *superblock, value string) error {
		createTime, err := time.Parse("Mon Jan 2 15:04:05 2006", value)
		if err != nil {
			return err
		}
		sb.mkfsTime = createTime.UTC()
		return nil
	},
	"Last mount time": func(sb *superblock, value string) error {
		mountTime, err := time.Parse("Mon Jan 2 15:04:05 2006", value)
		if err != nil {
			return err
		}
		sb.mountTime = mountTime.UTC()
		return nil
	},
	"Last write time": func(sb *superblock, value string) error {
		writeTime, err := time.Parse("Mon Jan 2 15:04:05 2006", value)
		if err != nil {
			return err
		}
		sb.writeTime = writeTime.UTC()
		return nil
	},
	"Mount count": func(sb *superblock, value string) error {
		mountCount, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return fmt.Errorf("Mount count: %w", err)
		}
		sb.mountCount = uint16(mountCount)
		return nil
	},
	"Maximum mount count": func(sb *superblock, value string) error {
		maxMountCount, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return fmt.Errorf("Maximum mount count: %w", err)
		}
		sb.mountsToFsck = uint16(maxMountCount)
		return nil
	},
	"Last checked": func(sb *superblock, value string) error {
		lastChecked, err := time.Parse("Mon Jan 2 15:04:05 2006", value)
		if err != nil {
			return err
		}
		sb.lastCheck = lastChecked.UTC()
		return nil
	},
	"First inode": func(sb *superblock, value string) error {
		firstInode, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("First inode: %w", err)
		}
		sb.firstNonReservedInode = uint32(firstInode)
		return nil
	},
	"Inode size": func(sb *superblock, value string) error {
		inodeSize, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return fmt.Errorf("Inode size: %w", err)
		}
		sb.inodeSize = uint16(inodeSize)
		return nil
	},
	"Required extra isize": func(sb *superblock, value string) error {
		inodeMinBytes, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return fmt.Errorf("Required extra isize: %w", err)
		}
		sb.inodeMinBytes = uint16(inodeMinBytes)
		return nil
	},
	"Desired extra isize": func(sb *superblock, value string) error {
		inodeReserveBytes, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return fmt.Errorf("Desired extra isize: %w", err)
		}
		sb.inodeReserveBytes = uint16(inodeReserveBytes)
		return nil
	},
	"Journal inode": func(sb *superblock, value string) error {
		journalInode, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Journal inode: %w", err)
		}
		sb.journalInode = uint32(journalInode)
		return nil
	},
	"Default directory hash": func(sb *superblock, value string) error {
		switch value {
		case "half_md4":
			sb.hashVersion = hashHalfMD4
		case "tea":
			sb.hashVersion = hashTea
		case "legacy":
			sb.hashVersion = hashLegacy
		default:
			return fmt.Errorf("unknown directory hash %s", value)
		}

		return nil
	},
	"Directory Hash Seed": func(sb *superblock, value string) error {
		u, err := uuid.Parse(value)
		if err != nil {
			return err
		}
		hashTreeSeedBytes := u[:]
		hashTreeSeed := make([]uint32, 4)
		for i := 0; i < 4; i++ {
			hashTreeSeed[i] = binary.LittleEndian.Uint32(hashTreeSeedBytes[i*4 : (i+1)*4])
		}
		sb.hashTreeSeed = hashTreeSeed

		return nil
	},
	"Checksum type": func(sb *superblock, value string) error {
		switch value {
		case "crc32c":
			sb.checksumType = checkSumTypeCRC32c
		default:
			return fmt.Errorf("unknown checksum type %s", value)
		}
		return nil
	},
	"Checksum seed": func(sb *superblock, value string) error {
		checksumSeed, err := strconv.ParseUint(value, 0, 32)
		if err != nil {
			return fmt.Errorf("Checksum seed: %w", err)
		}
		sb.checksumSeed = uint32(checksumSeed)
		return nil
	},
	"Orphan file inode": func(sb *superblock, value string) error {
		orphanInode, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Orphan file inode: %w", err)
		}
		sb.orphanedInodeInodeNumber = uint32(orphanInode)
		return nil
	},
	"Journal backup": func(sb *superblock, value string) error {
		switch value {
		case "inode blocks":
			// unfortunately, debugfs does not give this to us, so we read it manually
			/*
				   Journal backup inodes: 0x0001f30a, 0x00000004, 0x00000000, 0x00000000, 0x00001000, 0x0000c001, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000,
				      (these are in little-endian already; I converted by running:
				   	`dd if=superblock.bin   bs=1 skip=$((0x10c)) count=$((15 * 4)) | hexdump -e '15/4 "0x%08x, " "\n"'`
				   	)
					they are saved as testdata/dist/journalinodes.txt
			*/

			sb.journalBackup = &journalBackup{
				iBlocks: [15]uint32{0x0001f30a, 0x00000004, 0x00000000, 0x00000000, 0x00001000, 0x0000c001, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000},
				iSize:   uint64(4096 * KB),
			}
		default:
			return fmt.Errorf("unknown journal backup %s", value)
		}
		return nil
	},
	"Reserved blocks uid": func(sb *superblock, value string) error {
		parts := strings.Split(value, " ")
		if len(parts) < 2 {
			return fmt.Errorf("invalid uid string %s", value)
		}
		uid, err := strconv.ParseUint(parts[0], 10, 16)
		if err != nil {
			return fmt.Errorf("Reserved blocks uid: %w", err)
		}
		sb.reservedBlocksDefaultUID = uint16(uid)
		return nil
	},
	"Reserved blocks gid": func(sb *superblock, value string) error {
		parts := strings.Split(value, " ")
		if len(parts) < 2 {
			return fmt.Errorf("invalid gid string %s", value)
		}
		gid, err := strconv.ParseUint(parts[0], 10, 16)
		if err != nil {
			return fmt.Errorf("Reserved blocks gid: %w", err)
		}
		sb.reservedBlocksDefaultGID = uint16(gid)
		return nil
	},
	"Filesystem flags": func(sb *superblock, value string) error {
		flags := strings.Split(value, " ")
		for _, flag := range flags {
			switch flag {
			case "unsigned_directory_hash":
				sb.miscFlags.unsignedDirectoryHash = true
			case "signed_directory_hash":
				sb.miscFlags.signedDirectoryHash = true
			case "test_code":
				sb.miscFlags.developmentTest = true
			default:
				return fmt.Errorf("unknown flag %s", flag)
			}
		}
		return nil
	},
	"Default mount options": func(sb *superblock, value string) error {
		options := strings.Split(value, " ")
		for _, option := range options {
			switch option {
			case "user_xattr":
				sb.defaultMountOptions.userspaceExtendedAttributes = true
			case "acl":
				sb.defaultMountOptions.posixACLs = true
			default:
				return fmt.Errorf("unknown mount option %s", option)
			}
		}
		return nil
	},
	"Filesystem features": func(sb *superblock, value string) error {
		features := strings.Split(value, " ")
		for _, feature := range features {
			switch feature {
			case "has_journal":
				sb.features.hasJournal = true
			case "ext_attr":
				sb.features.extendedAttributes = true
			case "resize_inode":
			case "dir_index":
				sb.features.directoryIndices = true
			case "orphan_file":
				sb.features.orphanFile = true
			case "filetype":
				sb.features.directoryEntriesRecordFileType = true
			case "extent":
				sb.features.extents = true
			case "64bit":
				sb.features.fs64Bit = true
			case "flex_bg":
				sb.features.flexBlockGroups = true
			case "metadata_csum_seed":
				sb.features.metadataChecksumSeedInSuperblock = true
			case "sparse_super":
				sb.features.sparseSuperblock = true
			case "large_file":
				sb.features.largeFile = true
			case "huge_file":
				sb.features.hugeFile = true
			case "dir_nlink":
				sb.features.largeSubdirectoryCount = true
			case "extra_isize":
				sb.features.largeInodes = true
			case "metadata_csum":
				sb.features.metadataChecksums = true
			default:
				return fmt.Errorf("unknown feature %s", feature)
			}
		}
		return nil
	},
}

func testGetValidSuperblockAndGDTs() (sb *superblock, gd []groupDescriptor, superblockBytes, gdtBytes []byte, err error) {
	// get the raw bytes
	superblockBytes, err = os.ReadFile(testSuperblockFile)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to read %s", testSuperblockFile)
	}

	gdtBytes, err = os.ReadFile(testGDTFile)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to read %s", testGDTFile)
	}

	// get the info for the superblock
	stats, err := os.ReadFile(testFilesystemStats)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to read %s", testFilesystemStats)
	}
	// parse the stats
	sb = &superblock{}
	var (
		descs        []groupDescriptor
		inGroups     bool
		currentGroup *groupDescriptor
	)
	scanner := bufio.NewScanner(bytes.NewReader(stats))
	for scanner.Scan() {
		line := scanner.Text()
		if !inGroups {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) < 2 {
				continue
			}
			if parts[0] == "Group 0" {
				inGroups = true
			} else {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				if fn, ok := testSuperblockFuncs[key]; ok {
					if err := fn(sb, value); err != nil {
						return nil, nil, nil, nil, fmt.Errorf("failed to parse %s: %v", key, err)
					}
				}
				continue
			}
		}
		// we are in groups, so parse group section
		for i, gdtLine := range gdtRELines {
			matches := gdtLine.re.FindStringSubmatch(line)
			if len(matches) > 0 {
				if i == 0 {
					// this is the first line, so we need to save the previous group
					if currentGroup != nil {
						descs = append(descs, *currentGroup)
					}
					currentGroup = &groupDescriptor{size: 64}
				}
				if gdtLine.handler != nil {
					if err := gdtLine.handler(currentGroup, matches); err != nil {
						return nil, nil, nil, nil, fmt.Errorf("failed to parse group descriptor line %d: %w", i, err)
					}
				}
				// it matched one line, so do not go on to the next
				break
			}
		}
	}

	// these have been fixed. If they ever change, we will need to modify here.
	sb.errorFirstTime = time.Unix(0, 0).UTC()
	sb.errorLastTime = time.Unix(0, 0).UTC()
	juuid, err := uuid.FromBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Failed to parse journal UUID: %v", err)
	}
	sb.journalSuperblockUUID = &juuid
	sb.clusterSize = 1024

	// lifetime writes in KB is done separately, because debug -R "stats" and dumpe2fs only
	// round it out
	KBWritten, err := os.ReadFile(testKBWrittenFile)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to read %s: %w", testKBWrittenFile, err)
	}
	sb.totalKBWritten, err = strconv.ParseUint(strings.TrimSpace(string(KBWritten)), 10, 64)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to parse KB written: %w", err)
	}

	return sb, descs, superblockBytes, gdtBytes[:64*len(descs)], nil
}

func testDirEntriesFromDebugFS(file string) (dirEntries []*directoryEntry, err error) {
	dirInfo, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("Error opening directory info file %s: %w", dirInfo, err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(dirInfo))
	for scanner.Scan() {
		tokens := strings.Fields(scanner.Text())
		if len(tokens) < 9 {
			continue
		}
		inodeStr := tokens[0]
		filename := tokens[8]
		fileTypeStr := tokens[2]
		// remove the ( ) from the fileType
		fileTypeStr = strings.TrimPrefix(fileTypeStr, "(")
		fileTypeStr = strings.TrimSuffix(fileTypeStr, ")")
		inode, err := strconv.ParseUint(inodeStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("error parsing inode number %s: %w", inodeStr, err)
		}
		fileType, err := strconv.ParseUint(fileTypeStr, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("error parsing file type %s: %w", fileTypeStr, err)
		}
		dirEntries = append(dirEntries, &directoryEntry{inode: uint32(inode), filename: filename, fileType: directoryFileType(fileType)})
	}
	return dirEntries, nil
}
