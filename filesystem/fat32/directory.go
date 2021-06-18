package fat32

import (
	"time"
)

// Directory represents a single directory in a FAT32 filesystem
type Directory struct {
	directoryEntry
	sfnCache map[string]bool
	entries  []*directoryEntry
}

// dirEntriesFromBytes loads the directory entries from the raw bytes
func (d *Directory) entriesFromBytes(b []byte, f *FileSystem) error {
	entries, err := parseDirEntries(b, f)
	if err != nil {
		return err
	}
	d.entries = entries
	return nil
}

// entriesToBytes convert our entries to raw bytes
func (d *Directory) entriesToBytes(bytesPerCluster int) ([]byte, error) {
	b := make([]byte, 0)
	for _, de := range d.entries {
		b2, err := de.toBytes()
		if err != nil {
			return nil, err
		}
		b = append(b, b2...)
	}
	remainder := len(b) % bytesPerCluster
	extra := bytesPerCluster - remainder
	zeroes := make([]byte, extra, extra)
	b = append(b, zeroes...)
	return b, nil
}

// createEntry creates an entry in the given directory, and returns the handle to it
func (d *Directory) createEntry(name string, cluster uint32, dir bool) (*directoryEntry, error) {
	// is it a long filename or a short filename?
	var isLFN bool

	if d.sfnCache == nil {
		d.sfnCache = make(map[string]bool)
		// load all short filenames
		for _, entry := range d.entries {
			d.sfnCache[entry.filenameShort+"."+entry.fileExtension] = true
		}
	}

	shortName, extension, isLFN, _ := convertLfnSfn(name, d.sfnCache)
	lfn := ""
	if isLFN {
		lfn = name
	}

	d.sfnCache[shortName+"."+extension] = true

	// allocate a slot for the new filename in the existing directory
	entry := directoryEntry{
		filenameLong:      lfn,
		longFilenameSlots: -1, // indicate that we do not know how many slots, which will force a recalculation
		filenameShort:     shortName,
		fileExtension:     extension,
		fileSize:          uint32(0),
		clusterLocation:   cluster,
		filesystem:        d.filesystem,
		createTime:        time.Now(),
		modifyTime:        time.Now(),
		accessTime:        time.Now(),
		isSubdirectory:    dir,
		isNew:             true,
	}

	entry.longFilenameSlots = calculateSlots(entry.filenameLong)
	d.entries = append(d.entries, &entry)
	return &entry, nil
}

// createVolumeLabel create a volume label entry in the given directory, and return the handle to it
func (d *Directory) createVolumeLabel(name string) (*directoryEntry, error) {
	// allocate a slot for the new filename in the existing directory
	entry := directoryEntry{
		filenameLong:      "",
		longFilenameSlots: -1, // indicate that we do not know how many slots, which will force a recalculation
		filenameShort:     name[:8],
		fileExtension:     name[8:11],
		fileSize:          uint32(0),
		clusterLocation:   0,
		filesystem:        d.filesystem,
		createTime:        time.Now(),
		modifyTime:        time.Now(),
		accessTime:        time.Now(),
		isSubdirectory:    false,
		isNew:             true,
		isVolumeLabel:     true,
	}

	d.entries = append(d.entries, &entry)
	return &entry, nil
}
