package iso9660

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/deitch/diskfs/util"
)

// finalizeFileInfo is a file info useful for finalization
// fulfills os.FileInfo
//   Name() string       // base name of the file
//   Size() int64        // length in bytes for regular files; system-dependent for others
//   Mode() FileMode     // file mode bits
//   ModTime() time.Time // modification time
//   IsDir() bool        // abbreviation for Mode().IsDir()
//   Sys() interface{}   // underlying data source (can return nil)
type finalizeFileInfo struct {
	path       string
	shortname  string
	extension  string
	location   uint32
	blocks     uint32
	recordSize uint8
	depth      int
	name       string
	size       int64
	mode       os.FileMode
	modTime    time.Time
	isDir      bool
	parent     *finalizeFileInfo
	children   []*finalizeFileInfo
}

func (fi *finalizeFileInfo) Name() string {
	// we are using plain iso9660 (without extensions), so just shortname possibly with extension
	ret := fi.shortname
	if !fi.isDir {
		ret = fmt.Sprintf("%s.%s;1", fi.shortname, fi.extension)
	}
	// shortname already is ucased
	return ret
}
func (fi *finalizeFileInfo) Size() int64 {
	return fi.size
}
func (fi *finalizeFileInfo) Mode() os.FileMode {
	return fi.mode
}
func (fi *finalizeFileInfo) ModTime() time.Time {
	return fi.modTime
}
func (fi *finalizeFileInfo) IsDir() bool {
	return fi.isDir
}
func (fi *finalizeFileInfo) Sys() interface{} {
	return nil
}

func (fi *finalizeFileInfo) toDirectoryEntry(fs *FileSystem, isSelf, isParent bool) *directoryEntry {
	return &directoryEntry{
		extAttrSize:              0,
		location:                 fi.location,
		size:                     uint32(fi.Size()),
		creation:                 fi.ModTime(),
		isHidden:                 false,
		isSubdirectory:           fi.IsDir(),
		isAssociated:             false,
		hasExtendedAttrs:         false,
		hasOwnerGroupPermissions: false,
		hasMoreEntries:           false,
		isSelf:                   isSelf,
		isParent:                 isParent,
		volumeSequence:           1,
		filesystem:               fs,
		filename:                 fi.Name(),
	}
}

// sort all of the directory children recursively - this is for ordering into blocks
func (fi *finalizeFileInfo) collapseAndSortChildren(depth int) ([]*finalizeFileInfo, []*finalizeFileInfo) {
	dirs := make([]*finalizeFileInfo, 0)
	files := make([]*finalizeFileInfo, 0)
	// first extract all of the directories
	for _, e := range fi.children {
		if e.IsDir() {
			dirs = append(dirs, e)
			e.parent = fi
			e.depth = depth + 1
		} else {
			files = append(files, e)
		}
	}

	// next sort them
	sort.Slice(dirs, func(i, j int) bool {
		// just sort by filename; as good as anything else
		return dirs[i].Name() < dirs[j].Name()
	})
	sort.Slice(files, func(i, j int) bool {
		// just sort by filename; as good as anything else
		return files[i].Name() < files[j].Name()
	})
	// finally add in the children going down
	finalDirs := make([]*finalizeFileInfo, 0)
	finalFiles := files
	for _, e := range dirs {
		finalDirs = append(finalDirs, e)
		// now get any children
		d, f := e.collapseAndSortChildren(depth + 1)
		finalDirs = append(finalDirs, d...)
		finalFiles = append(finalFiles, f...)
	}
	return finalDirs, finalFiles
}

func finalizeFileInfoNames(fi []*finalizeFileInfo) []string {
	ret := make([]string, len(fi))
	for i, v := range fi {
		ret[i] = v.name
	}
	return ret
}

// Finalize finalize a read-only filesystem by writing it out to a read-only format
func (fs *FileSystem) Finalize() error {
	if fs.workspace == "" {
		return fmt.Errorf("Cannot finalize an already finalized filesystem")
	}

	/*
		There is nothing in the iso9660 spec about the order of directories and files,
		other than that they must be accessible in the location specified in directory entry and/or path table
		However, most implementations seem to it as follows:
		- each directory follows its parent
		- data (i.e. file) sectors in each directory are immediately after its directory and immediately before the next sibling directory to its parent

		to keep it simple, we will follow what xorriso/mkisofs on linux does, in the following order:
		- volume descriptor set, beginning at sector 16
		- root directory entry
		- all other directory entries, sorted alphabetically, depth first
		- L path table
		- M path table
		- data sectors for files, sorted alphabetically, matching order of directories

		this is where we build our filesystem
		 1- blank out sectors 0-15 for system use
		 2- skip sectors 16-17 for PVD and terminator (fill later)
		 3- calculate how many sectors required for root directory
		 4- calculate each child directory, working our way down, including number of sectors and location
		 5- write path tables (L & M)
		 6- write files for root directory
		 7- write root directory entry into its sector (18)
		 8- repeat steps 6&7 for all other directories
		 9- write PVD
		 10- write volume descriptor set terminator
	*/

	f := fs.file
	blocksize := int(fs.blocksize)

	// 1- blank out sectors 0-15
	b := make([]byte, 15*fs.blocksize)
	n, err := f.WriteAt(b, 0)
	if err != nil {
		return fmt.Errorf("Could not write blank system area: %v", err)
	}
	if n != len(b) {
		return fmt.Errorf("Only wrote %d bytes instead of expected %d to system area", n, len(b))
	}

	// 2- skip sectors 16-17 for PVD and terminator (fill later)

	// 3- build out file tree
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("Could not get pwd: %v", err)
	}
	os.Chdir(fs.Workspace())
	fileList := make([]*finalizeFileInfo, 0, 20)
	dirList := make(map[string]*finalizeFileInfo)
	var entry *finalizeFileInfo
	filepath.Walk(".", func(fp string, fi os.FileInfo, err error) error {
		isRoot := fp == "."
		parts := strings.SplitN(fi.Name(), ".", 2)
		shortname := parts[0]
		extension := ""
		if len(parts) > 1 {
			extension = parts[1]
		}
		// shortname and extension must be upper-case
		shortname = strings.ToUpper(shortname)
		extension = strings.ToUpper(extension)

		name := fi.Name()
		if isRoot {
			name = string([]byte{0x00})
			shortname = name
		}
		entry = &finalizeFileInfo{path: fp, name: name, isDir: fi.IsDir(), modTime: fi.ModTime(), mode: fi.Mode(), size: fi.Size(), shortname: shortname}

		// we will have to save it as its parent
		parentDir := filepath.Dir(fp)
		parentDirInfo := dirList[parentDir]

		if fi.IsDir() {
			entry.children = make([]*finalizeFileInfo, 0, 20)
			dirList[fp] = entry
			if !isRoot {
				parentDirInfo.children = append(parentDirInfo.children, entry)
				dirList[parentDir] = parentDirInfo
			}
		} else {
			// calculate blocks
			size := fi.Size()
			blocks := uint32(size / fs.blocksize)
			// add one for partial?
			if size%fs.blocksize > 0 {
				blocks++
			}
			entry.extension = extension
			entry.blocks = blocks
			fileList = append(fileList, entry)
			parentDirInfo.children = append(parentDirInfo.children, entry)
			dirList[parentDir] = parentDirInfo
		}
		return nil
	})

	// we now have list of all of the files and directories and their properties, as well as children of every directory
	// calculate the sizes of the directories
	for _, dir := range dirList {
		size := 0
		// add for self and parent
		size += 34 + 34
		for _, e := range dir.children {
			// calculate the size of the entry
			namelen := len(e.shortname)
			if !e.IsDir() {
				// add 1 for the separator '.' and 2 for ';1'
				namelen += 1 + len(e.extension) + 2
			}
			if namelen%2 == 0 {
				namelen++
			}
			// add name size to the fixed record size - for now just 33
			recordSize := namelen + 33
			e.recordSize = uint8(recordSize)
			// do not go over a block boundary; pad if necessary
			newlength := size + recordSize
			left := blocksize - size%blocksize
			if left != 0 && newlength/blocksize > size/blocksize {
				size += left
			}
			size += recordSize
		}
		// now we have the total size of the entrylist for this directory - calculate the blocks
		blocks := uint32(size / blocksize)
		// add one?
		if size%blocksize > 0 {
			blocks++
		}
		dir.size = int64(size)
		dir.blocks = blocks
	}

	// we have the list of all files and directories, and the number of blocks required to store each
	// now just sort and store them, beginning with root
	dirs := make([]*finalizeFileInfo, 0, 20)
	root := dirList["."]
	dirs = append(dirs, root)
	subdirs, files := root.collapseAndSortChildren(1)
	dirs = append(dirs, subdirs...)

	// we now have sorted list of block order, with sizes and number of blocks on each
	// next assign the blocks to each, and then we can enter the data in the directory entries
	totalSize := uint32(0)
	// totalSize includes the system area
	totalSize += 16 * uint32(blocksize)
	location := uint32(18)
	for _, e := range dirs {
		e.location = location
		location += e.blocks
	}

	// create the pathtables (L & M)
	// with the list of directories, we can make a path table
	pathTable := createPathTable(dirs)
	// how big is the path table? we will take LSB for now, because they are the same size
	pathTableLBytes := pathTable.toLBytes()
	pathTableMBytes := pathTable.toMBytes()
	pathTableSize := len(pathTableLBytes)
	pathTableBlocks := uint32(pathTableSize / blocksize)
	if pathTableSize%blocksize > 0 {
		pathTableBlocks++
	}
	// we do not do optional path tables yet
	pathTableLLocation := location
	location += pathTableBlocks
	pathTableMLocation := location
	location += pathTableBlocks

	for _, e := range files {
		e.location = location
		location += e.blocks
	}

	// now we can write each one out - dirs first then files
	for _, e := range dirs {
		writeAt := int64(e.location) * int64(blocksize)
		// also need to add self and parent to it
		self := e.toDirectoryEntry(fs, true, false)
		parent := &directoryEntry{}
		if e.parent == nil {
			*parent = *self
			parent.isSelf = false
			parent.isParent = true
		} else {
			parent = e.parent.toDirectoryEntry(fs, false, true)
		}
		entries := []*directoryEntry{self, parent}
		for _, child := range e.children {
			entries = append(entries, child.toDirectoryEntry(fs, false, false))
		}
		d := &Directory{
			directoryEntry: *self,
			entries:        entries,
		}
		// Directory.toBytes() always returns whole blocks
		p, err := d.entriesToBytes()
		totalSize += uint32(len(b))
		if err != nil {
			return fmt.Errorf("Could not convert directory to bytes: %v", err)
		}
		f.WriteAt(p, writeAt)
	}

	// now write out the path tables, L & M
	writeAt := int64(pathTableLLocation) * int64(blocksize)
	f.WriteAt(pathTableLBytes, writeAt)
	writeAt = int64(pathTableMLocation) * int64(blocksize)
	f.WriteAt(pathTableMBytes, writeAt)

	for _, e := range files {
		writeAt := int64(e.location) * int64(blocksize)
		// for file, just copy the data across
		from, err := os.Open(e.path)
		if err != nil {
			return fmt.Errorf("failed to open file for reading %s: %v", e.path, err)
		}
		defer from.Close()
		copied, err := copyFileData(from, f, 0, writeAt)
		if err != nil {
			return fmt.Errorf("failed to copy file to disk %s: %v", e.path, err)
		}
		if copied != int(e.Size()) {
			return fmt.Errorf("error copying file %s to disk, copied %d bytes, expected %d", e.path, copied, e.Size())
		}
		totalSize += e.blocks * uint32(blocksize)
	}

	// create and write the primary volume descriptor and the volume descriptor set terminator
	now := time.Now()
	pvd := &primaryVolumeDescriptor{
		systemIdentifier:           "",
		volumeIdentifier:           "ISOIMAGE",
		volumeSize:                 uint32(totalSize),
		setSize:                    1,
		sequenceNumber:             1,
		blocksize:                  uint16(fs.blocksize),
		pathTableSize:              uint32(pathTableSize),
		pathTableLLocation:         pathTableLLocation,
		pathTableLOptionalLocation: 0,
		pathTableMLocation:         pathTableMLocation,
		pathTableMOptionalLocation: 0,
		volumeSetIdentifier:        "",
		publisherIdentifier:        "",
		preparerIdentifier:         util.AppNameVersion,
		applicationIdentifier:      "",
		copyrightFile:              "", // 37 bytes
		abstractFile:               "", // 37 bytes
		bibliographicFile:          "", // 37 bytes
		creation:                   now,
		modification:               now,
		expiration:                 now,
		effective:                  now,
		rootDirectoryEntry:         root.toDirectoryEntry(fs, true, false),
	}
	b = pvd.toBytes()
	f.WriteAt(b, 16*int64(blocksize))
	terminator := &terminatorVolumeDescriptor{}
	b = terminator.toBytes()
	f.WriteAt(b, 17*int64(blocksize))

	// reset the workspace
	os.Chdir(cwd)

	// finish by setting as finalized
	fs.workspace = ""
	return nil
}

func copyFileData(from, to util.File, fromOffset, toOffset int64) (int, error) {
	buf := make([]byte, 2048)
	copied := 0
	for {
		n, err := from.ReadAt(buf, fromOffset+int64(copied))
		if err != nil && err != io.EOF {
			return copied, err
		}
		if n == 0 {
			break
		}

		if _, err := to.WriteAt(buf[:n], toOffset+int64(copied)); err != nil {
			return copied, err
		}
		copied += n
	}
	return copied, nil
}

// sort path table entries
func sortFinalizeFileInfoPathTable(left, right *finalizeFileInfo) bool {
	switch {
	case left.parent == right.parent:
		// same parents = same depth, just sort on name
		lname := left.Name()
		rname := right.Name()
		maxLen := maxInt(len(lname), len(rname))
		format := fmt.Sprintf("%%-%ds", maxLen)
		return fmt.Sprintf(format, lname) < fmt.Sprintf(format, rname)
	case left.depth < right.depth:
		// different parents with different depth, lower first
		return true
	case right.depth > left.depth:
		return false
	case left.parent == nil && right.parent != nil:
		return true
	case left.parent != nil && right.parent == nil:
		return false
	default:
		// same depth, different parents, it depends on the sort order of the parents
		return sortFinalizeFileInfoPathTable(left.parent, right.parent)
	}
}

// create a path table from a slice of *finalizeFileInfo that are directories
func createPathTable(fi []*finalizeFileInfo) *pathTable {
	// copy so we do not modify the original
	fs := make([]*finalizeFileInfo, len(fi))
	copy(fs, fi)
	// sort via the rules
	sort.Slice(fs, func(i, j int) bool {
		return sortFinalizeFileInfoPathTable(fs[i], fs[j])
	})
	indexMap := make(map[*finalizeFileInfo]int)
	// now that it is sorted, create the ordered path table entries
	entries := make([]*pathTableEntry, 0)
	for i, e := range fs {
		name := e.Name()
		nameSize := len(name)
		size := 8 + uint16(nameSize)
		if nameSize%2 != 0 {
			size++
		}
		ownIndex := i + 1
		indexMap[e] = ownIndex
		// root just points to itself
		parentIndex := ownIndex
		if ip, ok := indexMap[e.parent]; ok {
			parentIndex = ip
		}
		pte := &pathTableEntry{
			nameSize:      uint8(nameSize),
			size:          size,
			extAttrLength: 0,
			location:      e.location,
			parentIndex:   uint16(parentIndex),
			dirname:       name,
		}
		entries = append(entries, pte)
	}
	return &pathTable{
		records: entries,
	}

}
