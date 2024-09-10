package fat32

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	Fat32File           = "./testdata/dist/fat32.img"
	fsckFile            = "./testdata/dist/fsck.txt"
	rootdirFile         = "./testdata/dist/root_dir.txt"
	rootdirFileFLS      = "./testdata/dist/root_dir_fls.txt"
	rootdirEntryPattern = "./testdata/dist/root_dir_istat_%d.txt"
	foodirFile          = "./testdata/dist/foo_dir.txt"
	foodirEntryPattern  = "./testdata/dist/foo_dir_istat_%d.txt"
	serialFile          = "./testdata/dist/serial.txt"
	fsstatFile          = "./testdata/dist/fsstat.txt"
)

type testFSInfo struct {
	bytesPerCluster uint32
	dataStartBytes  uint32
	dataStartSector uint32
	bytesPerSector  uint32
	reservedSectors uint32
	sectorsPerFAT   uint32
	label           string
	serial          uint32
	sectorsPerTrack uint32
	heads           uint32
	hiddenSectors   uint32
	freeSectorCount uint32
	nextFreeSector  uint32
	firstFAT        uint32
	table           *table
}

var (
	testVolumeLabelRE           = regexp.MustCompile(`^\s*Volume in drive\s+:\s+is\s+(.+)\s*$`)
	testDirectoryEntryRE        = regexp.MustCompile(`^\s*(\S+)\s+<DIR>\s+(\d{4}-\d\d-\d\d\s+\d+:\d+)\s*(.*)\s*$`)
	testFileEntryRE             = regexp.MustCompile(`^\s*(\S+)\s*(\S*)\s+(\d+)\s+(\d{4}-\d\d-\d\d\s+\d+:\d+)\s*(.*)\s*$`)
	testWrittenTimeRE           = regexp.MustCompile(`\s*Written:\s+(\d{4}-\d\d-\d\d\s+\d\d:\d\d:\d\d)`)
	testAccessedTimeRE          = regexp.MustCompile(`\s*Accessed:\s+(\d{4}-\d\d-\d\d\s+\d\d:\d\d:\d\d)`)
	testCreatedTimeRE           = regexp.MustCompile(`\s*Created:\s+(\d{4}-\d\d-\d\d\s+\d\d:\d\d:\d\d)`)
	testSectorListStartRE       = regexp.MustCompile(`\s*Sectors:\s*$`)
	testFSCKDataStart           = regexp.MustCompile(`Data area starts at byte (\d+) \(sector (\d+)\)`)
	testFSCKBytesPerSector      = regexp.MustCompile(`^\s*(\d+) bytes per logical sector\s*$`)
	testFSCKBytesPerCluster     = regexp.MustCompile(`^\s*(\d+) bytes per cluster\s*$`)
	testFSCKReservedSectors     = regexp.MustCompile(`^\s*(\d+) reserved sectors\s*$`)
	testFSCKSectorsPerFat       = regexp.MustCompile(`^\s*(\d+) bytes per FAT \(= (\d+) sectors\)\s*$`)
	testFSCKHeadsSectors        = regexp.MustCompile(`^\s*(\d+) sectors/track, (\d+) heads\s*$`)
	testFSCKHiddenSectors       = regexp.MustCompile(`^\s*(\d+) hidden sectors\s*$`)
	testFSCKFirstFAT            = regexp.MustCompile(`^\s*First FAT starts at byte (\d+) \(sector (\d+)\)\s*$`)
	testFSCKFATSize             = regexp.MustCompile(`^\s*(\d+) bytes per FAT \(= (\d+) sectors\)\s*$`)
	testFLSEntryPattern         = regexp.MustCompile(`d/d (\d+):\s+(\S+)\s*.*$`)
	testFSSTATFreeSectorCountRE = regexp.MustCompile(`^\s*Free Sector Count.*: (\d+)\s*$`)
	testFSSTATNextFreeSectorRE  = regexp.MustCompile(`^\s*Next Free Sector.*: (\d+)\s*`)
	testFSSTATClustersStartRE   = regexp.MustCompile(`\s*FAT CONTENTS \(in sectors\)\s*$`)
	testFSSTATClusterLineRE     = regexp.MustCompile(`\s*(\d+)-(\d+) \((\d+)\)\s+->\s+(\S+)\s*$`)

	fsInfo *testFSInfo
)

// TestMain sets up the test environment and runs the tests
func TestMain(m *testing.M) {
	// Check and generate artifacts if necessary
	if _, err := os.Stat(Fat32File); os.IsNotExist(err) {
		// Run the genartifacts.sh script
		cmd := exec.Command("sh", "mkfat32.sh")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = "testdata"

		// Execute the command
		if err := cmd.Run(); err != nil {
			println("error generating test artifacts for fat32", err)
			os.Exit(1)
		}
	}

	// common info
	var err error
	fsInfo, err = testReadFilesystemData()
	if err != nil {
		println("Error reading fsck file", err)
		os.Exit(1)
	}

	// Run the tests
	code := m.Run()

	// Exit with the appropriate code
	os.Exit(code)
}

// GetValidDirectoryEntries get directory entries for the root directory
//
//nolint:revive // yes we are returning an exported type, but that is ok for the tests
func GetValidDirectoryEntries() (entries []*directoryEntry, b []byte, err error) {
	// read correct bytes off of disk

	input, err := os.ReadFile(Fat32File)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading data from fat32 test fixture %s: %v", Fat32File, err)
	}
	start := fsInfo.dataStartBytes // start of root directory in fat32.img
	// we only have 9 actual 32-byte entries, of which 4 are real and 3 are VFAT extensionBytes
	//   the rest are all 0s (as they should be), so we will include to exercise it
	b = make([]byte, fsInfo.bytesPerCluster)
	copy(b, input[start:start+fsInfo.bytesPerCluster])

	entries, err = testGetValidDirectoryEntriesFromFile(rootdirFile, rootdirEntryPattern, fsInfo)

	// in the root directory, add the label entry
	if fsInfo.label != "" {
		filenameShort := fsInfo.label
		extension := ""
		if len(fsInfo.label) > 8 {
			filenameShort = fsInfo.label[:8]
			extension = fsInfo.label[8:]
		}
		de := &directoryEntry{filenameShort: filenameShort, fileExtension: extension, isVolumeLabel: true}
		filename := fmt.Sprintf(rootdirEntryPattern, len(entries))
		if err := testPopulateDirectoryEntryFromIstatFile(de, filename, fsInfo); err != nil {
			return nil, nil, err
		}
		entries = append(entries, de)
	}

	return entries, b, err
}

// getValidDirectoryEntriesExtended get directory entries for a directory where there are so many,
// it has to use the extended structure. Will look for the provided dir,
// but only one step down from root. If you want more, look for it elsewhere.
//
//nolint:revive // yes we are returning an exported type, but that is ok for the tests
func GetValidDirectoryEntriesExtended(dir string) (entries []*directoryEntry, b []byte, err error) {
	// read correct bytes off of disk

	// find the cluster for the given directory
	dir = strings.TrimPrefix(dir, "/")
	dir = strings.TrimPrefix(dir, "\\")
	dir = strings.TrimSuffix(dir, "/")
	dir = strings.TrimSuffix(dir, "\\")

	flsData, err := os.ReadFile(rootdirFileFLS)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading fls data from %s: %w", rootdirFileFLS, err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(flsData))
	var cluster int
	for scanner.Scan() {
		text := scanner.Text()
		match := testFLSEntryPattern.FindStringSubmatch(text)
		if len(match) != 3 || match[2] != dir {
			continue
		}
		cluster, err = strconv.Atoi(match[1])
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cluster number %s: %w", match[1], err)
		}
		break
	}

	input, err := os.ReadFile(Fat32File)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading data from fat32 test fixture %s: %v", Fat32File, err)
	}
	start := fsInfo.dataStartBytes + 1 // start of foo directory in fat32.img
	// we only have 9 actual 32-byte entries, of which 4 are real and 3 are VFAT extensionBytes
	//   the rest are all 0s (as they should be), so we will include to exercise it
	b = make([]byte, fsInfo.bytesPerCluster)
	copy(b, input[start:start+fsInfo.bytesPerCluster])

	entries, err = testGetValidDirectoryEntriesFromFile(foodirFile, foodirEntryPattern, fsInfo)
	// handle . and ..
	if len(entries) > 0 && entries[0].filenameShort == "." {
		entries[0].clusterLocation = uint32(cluster)
	}
	if len(entries) > 1 && entries[1].filenameShort == ".." {
		// root always is 2, but it seems to store it as 0, for reasons I do not know
		entries[1].clusterLocation = 0
	}
	return entries, b, err
}

func testGetValidDirectoryEntriesFromFile(dirFilePath, dirEntryPattern string, fsInfo *testFSInfo) (dirEntries []*directoryEntry, err error) {
	dirInfo, err := os.ReadFile(dirFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening directory info file %s: %w", dirInfo, err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(dirInfo))
	for scanner.Scan() {
		text := scanner.Text()
		dirEntryMatch := testDirectoryEntryRE.FindStringSubmatch(text)
		fileEntryMatch := testFileEntryRE.FindStringSubmatch(text)
		var (
			de *directoryEntry
		)
		switch {
		case len(dirEntryMatch) == 4:
			filenameShort := dirEntryMatch[1]
			de = &directoryEntry{
				filenameShort:  strings.ToUpper(filenameShort),
				isSubdirectory: true,
			}
			if dirEntryMatch[3] != "" {
				de.filenameLong = strings.TrimSpace(dirEntryMatch[3])
				de.longFilenameSlots = calculateSlots(de.filenameLong)
			}
			if filenameShort != "." && filenameShort != ".." && strings.ToLower(filenameShort) == filenameShort {
				de.lowercaseShortname = true
			}
		case len(fileEntryMatch) == 6:
			size, err := strconv.Atoi(fileEntryMatch[3])
			if err != nil {
				return nil, fmt.Errorf("error parsing file size %s: %w", fileEntryMatch[3], err)
			}
			de = &directoryEntry{
				filenameShort:  strings.ToUpper(fileEntryMatch[1]),
				fileExtension:  strings.ToUpper(fileEntryMatch[2]),
				fileSize:       uint32(size),
				isArchiveDirty: true,
			}
			if strings.ToLower(fileEntryMatch[1]) == fileEntryMatch[1] {
				de.lowercaseShortname = true
			}
			if fileEntryMatch[2] != "" && strings.ToLower(fileEntryMatch[2]) == fileEntryMatch[2] {
				de.lowercaseExtension = true
			}
			if fileEntryMatch[5] != "" {
				de.filenameLong = strings.TrimSpace(fileEntryMatch[5])
				de.longFilenameSlots = calculateSlots(de.filenameLong)
			}
		default:
			continue
		}
		dirEntries = append(dirEntries, de)
	}
	// now need to go through the more detailed info from istat and find the dates
	// ignore entries for . and ..
	dirEntriesSubset := dirEntries
	for {
		if len(dirEntriesSubset) == 0 || (dirEntriesSubset[0].filenameShort != "." && dirEntriesSubset[0].filenameShort != "..") {
			break
		}
		dirEntriesSubset = dirEntriesSubset[1:]
	}
	for i, de := range dirEntriesSubset {
		filename := fmt.Sprintf(dirEntryPattern, i)
		if err := testPopulateDirectoryEntryFromIstatFile(de, filename, fsInfo); err != nil {
			return nil, err
		}
	}
	return dirEntries, nil
}

func testPopulateDirectoryEntryFromIstatFile(de *directoryEntry, filename string, fsInfo *testFSInfo) error {
	dirInfo, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error opening directory entry info file %s: %w", filename, err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(dirInfo))
	var inSectors bool
	for scanner.Scan() {
		text := scanner.Text()
		sectorStartMatch := testSectorListStartRE.FindStringSubmatch(text)
		writtenTimeMatch := testWrittenTimeRE.FindStringSubmatch(text)
		accessedTimeMatch := testAccessedTimeRE.FindStringSubmatch(text)
		createdTimeMatch := testCreatedTimeRE.FindStringSubmatch(text)
		switch {
		case inSectors:
			// just split the line and use all non-whitespace as numbers
			if de.clusterLocation != 0 {
				continue
			}
			sectors := strings.Fields(text)
			for _, sector := range sectors {
				sectorNum, err := strconv.Atoi(sector)
				if err != nil {
					return fmt.Errorf("error parsing sector number %s: %w", sector, err)
				}
				de.clusterLocation = uint32(sectorNum) - fsInfo.dataStartSector + 2
				break
			}
		case len(sectorStartMatch) > 0:
			inSectors = true
		case len(writtenTimeMatch) == 2:
			date, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(writtenTimeMatch[1]))
			if err != nil {
				return fmt.Errorf("error parsing written time %s: %w", writtenTimeMatch[1], err)
			}
			de.modifyTime = date
		case len(accessedTimeMatch) == 2:
			date, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(accessedTimeMatch[1]))
			if err != nil {
				return fmt.Errorf("error parsing accessed time %s: %w", accessedTimeMatch[1], err)
			}
			de.accessTime = date
		case len(createdTimeMatch) == 2:
			date, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(createdTimeMatch[1]))
			if err != nil {
				return fmt.Errorf("error parsing accessed time %s: %w", createdTimeMatch[1], err)
			}
			de.createTime = date
		}
	}
	return nil
}

//nolint:gocyclo // we need to call this function from the test, do not care that it is too complex
func testReadFilesystemData() (info *testFSInfo, err error) {
	info = &testFSInfo{}
	fsckInfo, err := os.ReadFile(fsckFile)
	if err != nil {
		return nil, fmt.Errorf("error opening fsck info file %s: %v", fsckFile, err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(fsckInfo))
	for scanner.Scan() {
		text := scanner.Text()
		dataStartMatch := testFSCKDataStart.FindStringSubmatch(text)
		bytesPerClusterMatch := testFSCKBytesPerCluster.FindStringSubmatch(text)
		bytesPerSectorMatch := testFSCKBytesPerSector.FindStringSubmatch(text)
		reservedSectorsMatch := testFSCKReservedSectors.FindStringSubmatch(text)
		sectorsPerFATMatch := testFSCKSectorsPerFat.FindStringSubmatch(text)
		headsSectorMatch := testFSCKHeadsSectors.FindStringSubmatch(text)
		hiddenSectorsMatch := testFSCKHiddenSectors.FindStringSubmatch(text)
		firstFATMatch := testFSCKFirstFAT.FindStringSubmatch(text)
		switch {
		case len(headsSectorMatch) == 3:
			sectorsPerTrack, err := strconv.Atoi(headsSectorMatch[1])
			if err != nil {
				return nil, fmt.Errorf("error parsing sectors per track %s: %v", headsSectorMatch[1], err)
			}
			heads, err := strconv.Atoi(headsSectorMatch[2])
			if err != nil {
				return nil, fmt.Errorf("error parsing heads %s: %v", headsSectorMatch[2], err)
			}
			info.sectorsPerTrack = uint32(sectorsPerTrack)
			info.heads = uint32(heads)
		case len(hiddenSectorsMatch) == 2:
			hiddenSectors, err := strconv.Atoi(hiddenSectorsMatch[1])
			if err != nil {
				return nil, fmt.Errorf("error parsing hidden sectors %s: %v", hiddenSectorsMatch[1], err)
			}
			info.hiddenSectors = uint32(hiddenSectors)
		case len(dataStartMatch) == 3:
			byteStart, err := strconv.Atoi(dataStartMatch[1])
			if err != nil {
				return nil, fmt.Errorf("error parsing data start byte %s: %v", dataStartMatch[1], err)
			}
			sectorStart, err := strconv.Atoi(dataStartMatch[2])
			if err != nil {
				return nil, fmt.Errorf("error parsing data start sector %s: %v", dataStartMatch[2], err)
			}
			info.dataStartBytes = uint32(byteStart)
			info.dataStartSector = uint32(sectorStart)

		case len(bytesPerClusterMatch) == 2:
			bytesPerCluster, err := strconv.Atoi(bytesPerClusterMatch[1])
			if err != nil {
				return nil, fmt.Errorf("error parsing bytes per cluster %s: %v", bytesPerClusterMatch[1], err)
			}
			info.bytesPerCluster = uint32(bytesPerCluster)
		case len(bytesPerSectorMatch) == 2:
			bytesPerSector, err := strconv.Atoi(bytesPerSectorMatch[1])
			if err != nil {
				return nil, fmt.Errorf("error parsing bytes per sector %s: %v", bytesPerSectorMatch[1], err)
			}
			info.bytesPerSector = uint32(bytesPerSector)
		case len(reservedSectorsMatch) == 2:
			reservedSectors, err := strconv.Atoi(reservedSectorsMatch[1])
			if err != nil {
				return nil, fmt.Errorf("error parsing reserved sectors %s: %v", reservedSectorsMatch[1], err)
			}
			info.reservedSectors = uint32(reservedSectors)
		case len(sectorsPerFATMatch) == 3:
			sectorsPerFAT, err := strconv.Atoi(sectorsPerFATMatch[2])
			if err != nil {
				return nil, fmt.Errorf("error parsing sectors per FAT %s: %v", sectorsPerFATMatch[2], err)
			}
			info.sectorsPerFAT = uint32(sectorsPerFAT)
		case len(firstFATMatch) == 3:
			firstFAT, err := strconv.Atoi(firstFATMatch[1])
			if err != nil {
				return nil, fmt.Errorf("error parsing first FAT byte %s: %v", firstFATMatch[1], err)
			}
			info.firstFAT = uint32(firstFAT)
		}
	}

	// get the filesystem label
	dirInfo, err := os.ReadFile(rootdirFile)
	if err != nil {
		println("Error opening directory info file", rootdirFile, err)
		os.Exit(1)
	}

	scanner = bufio.NewScanner(bytes.NewReader(dirInfo))
	for scanner.Scan() {
		text := scanner.Text()
		volLabelMatch := testVolumeLabelRE.FindStringSubmatch(text)
		if len(volLabelMatch) == 2 {
			info.label = strings.TrimSpace(volLabelMatch[1])
			break
		}
	}

	serial, err := os.ReadFile(serialFile)
	if err != nil {
		println("Error reading serial file", serialFile, err)
		os.Exit(1)
	}
	decimal, err := strconv.ParseInt(strings.TrimSpace(string(serial)), 16, 64)
	if err != nil {
		println("Error converting contents of serial file to integer:", err)
		os.Exit(1)
	}
	info.serial = uint32(decimal)

	fsstat, err := os.ReadFile(fsstatFile)
	if err != nil {
		println("Error reading fsstat file", fsstatFile, err)
		os.Exit(1)
	}
	scanner = bufio.NewScanner(bytes.NewReader(fsstat))
	var inClusters bool
	for scanner.Scan() {
		text := scanner.Text()
		freeSectorsMatch := testFSSTATFreeSectorCountRE.FindStringSubmatch(text)
		nextFreeSectorMatch := testFSSTATNextFreeSectorRE.FindStringSubmatch(text)
		clusterStartMatch := testFSSTATClustersStartRE.FindStringSubmatch(text)
		clusterLineMatch := testFSSTATClusterLineRE.FindStringSubmatch(text)
		switch {
		case len(freeSectorsMatch) == 2:
			freeSectors, err := strconv.Atoi(freeSectorsMatch[1])
			if err != nil {
				println("Error parsing free sectors count", freeSectorsMatch[1], err)
				os.Exit(1)
			}
			info.freeSectorCount = uint32(freeSectors)
		case len(nextFreeSectorMatch) == 2:
			nextFreeSector, err := strconv.Atoi(nextFreeSectorMatch[1])
			if err != nil {
				println("Error parsing next free sector", nextFreeSectorMatch[1], err)
				os.Exit(1)
			}
			// make sure to drop by the data start sector, and add 2 for the root and FAT
			info.nextFreeSector = uint32(nextFreeSector) - info.dataStartSector + 2
		case len(clusterStartMatch) > 0:
			inClusters = true
			sectorsPerFat := info.sectorsPerFAT
			sizeInBytes := sectorsPerFat * info.bytesPerSector
			numClusters := sizeInBytes / 4
			info.table = &table{
				fatID:          268435448, // 0x0ffffff8
				eocMarker:      eoc,       // 0x0fffffff
				rootDirCluster: 2,         // root is at cluster 2
				size:           sizeInBytes,
				maxCluster:     numClusters,
				clusters:       make([]uint32, numClusters+1),
			}
		case inClusters && len(clusterLineMatch) > 4:
			start, err := strconv.Atoi(clusterLineMatch[1])
			if err != nil {
				println("Error parsing cluster start", clusterLineMatch[1], err)
				os.Exit(1)
			}
			end, err := strconv.Atoi(clusterLineMatch[2])
			if err != nil {
				println("Error parsing cluster end", clusterLineMatch[2], err)
				os.Exit(1)
			}
			var target uint32
			if clusterLineMatch[4] == "EOF" {
				target = eoc
			} else {
				targetInt, err := strconv.Atoi(clusterLineMatch[4])
				if err != nil {
					println("Error parsing cluster target", clusterLineMatch[4], err)
					os.Exit(1)
				}
				target = uint32(targetInt) - info.dataStartSector + 2
			}
			// 2 is a special case that fsstat does not handle well
			// the start and end might be the same, or it might be a continual chain,
			// with only the last pointing at the target
			for i := start; i < end; i++ {
				startCluster := uint32(i) - info.dataStartSector + 2
				info.table.clusters[startCluster] = startCluster + 1
			}
			endCluster := uint32(end) - info.dataStartSector + 2
			if endCluster == 2 {
				target = eocMin
			}
			info.table.clusters[endCluster] = target
		}
	}
	return info, err
}
