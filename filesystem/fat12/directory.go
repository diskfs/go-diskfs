package fat12

import (
	"fmt"
	"strings"

	"github.com/diskfs/go-diskfs/util/timestamp"
)

// Directory represents a single directory in a FAT12/FAT16 filesystem
type Directory struct {
	directoryEntry
	entries []*directoryEntry
}

// dirEntriesFromBytes loads the directory entries from the raw bytes
func (d *Directory) entriesFromBytes(b []byte) error {
	entries, err := parseDirEntries(b)
	if err != nil {
		return err
	}
	d.entries = entries
	return nil
}

// entriesToBytes convert our entries to raw bytes, padded to a multiple of bytesPerCluster.
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
	if remainder != 0 {
		zeroes := make([]byte, bytesPerCluster-remainder)
		b = append(b, zeroes...)
	}
	return b, nil
}

// entriesToBytesFixed serialises entries into a fixed-size buffer of exactly fixedSize bytes.
// Used for the FAT12/16 root directory region which has a pre-allocated fixed size.
func (d *Directory) entriesToBytesFixed(fixedSize int) ([]byte, error) {
	b := make([]byte, fixedSize)
	offset := 0
	for _, de := range d.entries {
		b2, err := de.toBytes()
		if err != nil {
			return nil, err
		}
		if offset+len(b2) > fixedSize {
			return nil, fmt.Errorf("root directory is full: exceeds fixed size of %d bytes", fixedSize)
		}
		copy(b[offset:], b2)
		offset += len(b2)
	}
	return b, nil
}

// uniqueShortName finds the lowest available numeric-tail short name for the
// given base stem and extension, scanning the provided entries for conflicts.
//
// stem must already be upper-cased and contain only valid 8.3 characters; it
// will be truncated as needed to make room for the tail.  ext is the 0–3 char
// extension (without the dot), also already upper-cased.
//
// The FAT spec numeric-tail algorithm:
//
//	counter 1–9   → stem truncated to 6 chars + "~N"   (total 8 chars)
//	counter 10–99  → stem truncated to 5 chars + "~NN"
//	counter 100–999 → stem truncated to 4 chars + "~NNN"
//	…and so on.
func uniqueShortName(stem, ext string, entries []*directoryEntry) string {
	// Build a set of the existing short names (already uppercase on disk).
	existing := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if e.isVolumeLabel {
			continue
		}
		existing[e.filenameShort+e.fileExtension] = struct{}{}
	}

	for counter := 1; counter <= 999999; counter++ {
		suffix := fmt.Sprintf("~%d", counter)
		maxStem := 8 - len(suffix)
		s := stem
		if len(s) > maxStem {
			s = s[:maxStem]
		}
		candidate := s + suffix
		if _, clash := existing[candidate+ext]; !clash {
			return candidate
		}
	}
	// Unreachable in practice.
	return stem[:6] + "~1"
}

// shortNameFor derives the 8.3 name for a new entry called name. entries are
// the entries already in the directory, whose short names must not be reused.
//
// A name that converts to 8.3 without losing anything but case keeps its plain
// short name as long as that is free. Any other name gets a numeric tail, as
// does one whose plain short name is already taken: a conversion that drops or
// replaces characters, or truncates the extension, can give the same result for
// different names ("a b" and "ab", "foo.conf" and "foo.cons").
func shortNameFor(name string, entries []*directoryEntry) (shortName, extension string, isLFN bool) {
	shortName, extension, isLFN, isTruncated := convertLfnSfn(name)

	stem := shortName
	if isTruncated {
		// convertLfnSfn emits "~1" as the numeric tail unconditionally.
		stem = strings.TrimSuffix(shortName, "~1")
	}
	// The stem of a name with a leading dot is taken from the rest of the name,
	// which is the plain conversion for such names. A clash is still resolved
	// below.
	lossy := isTruncated || !isLosslessShortName(strings.TrimLeft(name, "."), shortName, extension)
	if !lossy && !shortNameTaken(shortName, extension, entries) {
		return shortName, extension, isLFN
	}
	if stem == "" {
		// Nothing usable to build a stem from.
		return shortName, extension, isLFN
	}
	return uniqueShortName(stem, extension, entries), extension, true
}

// isLosslessShortName reports whether shortName.extension is name with at most
// its case changed.
func isLosslessShortName(name, shortName, extension string) bool {
	short := shortName
	if extension != "" {
		short += "." + extension
	}
	return strings.EqualFold(name, short)
}

// shortNameTaken reports whether any of entries already uses the given short name.
func shortNameTaken(shortName, extension string, entries []*directoryEntry) bool {
	for _, e := range entries {
		if e.isVolumeLabel {
			continue
		}
		if strings.EqualFold(e.filenameShort, shortName) && strings.EqualFold(e.fileExtension, extension) {
			return true
		}
	}
	return false
}

// createEntry creates an entry in the given directory and returns a handle to it.
func (d *Directory) createEntry(name string, cluster uint32, dir bool) (*directoryEntry, error) {
	shortName, extension, isLFN := shortNameFor(name, d.entries)

	lfn := ""
	if isLFN {
		lfn = name
	}

	ts := timestamp.GetTime()

	entry := directoryEntry{
		filenameLong:      lfn,
		longFilenameSlots: -1, // will be recalculated
		filenameShort:     shortName,
		fileExtension:     extension,
		fileSize:          0,
		clusterLocation:   cluster,
		filesystem:        d.filesystem,
		createTime:        ts,
		modifyTime:        ts,
		accessTime:        ts,
		isSubdirectory:    dir,
		isNew:             true,
	}
	entry.longFilenameSlots = calculateSlots(entry.filenameLong)
	d.entries = append(d.entries, &entry)
	return &entry, nil
}

// removeEntry removes the entry that matches name (matched by long filename or
// 8.3 short name, case-insensitively) from the directory.
func (d *Directory) removeEntry(name string) error {
	removeEntryIndex := -1
	for i, entry := range d.entries {
		if entry.nameMatches(name) {
			removeEntryIndex = i
			break
		}
	}
	if removeEntryIndex == -1 {
		return fmt.Errorf("cannot find entry for name %s", name)
	}
	d.entries = append(d.entries[:removeEntryIndex], d.entries[removeEntryIndex+1:]...)
	return nil
}

// renameEntry renames the entry that matches oldFileName to newFileName.
// Both names are matched case-insensitively against long and short filenames.
// If newFileName already exists in the directory, that entry is replaced
// (mirroring the behaviour of os.Rename on most operating systems).
// When the new short name must be generated and would conflict with an existing
// entry, a unique numeric tail is assigned — excluding the old entry itself
// from the conflict set, since it is being replaced.
func (d *Directory) renameEntry(oldFileName, newFileName string) error {
	// Build the short name for the new filename before we start mutating entries.
	// Exclude the entry being renamed, and any entry about to be replaced, from
	// the conflict scan: after the rename completes their short name slots are
	// vacated.
	var others []*directoryEntry
	for _, e := range d.entries {
		if !e.nameMatches(oldFileName) && !e.nameMatches(newFileName) {
			others = append(others, e)
		}
	}
	newShort, newExt, newIsLFN := shortNameFor(newFileName, others)
	newLFN := ""
	if newIsLFN {
		newLFN = newFileName
	}

	newEntries := make([]*directoryEntry, 0, len(d.entries))
	isReplaced := false
	for _, entry := range d.entries {
		// Drop any existing entry that has the new name (it is overwritten).
		if entry.nameMatches(newFileName) {
			continue
		}
		if entry.nameMatches(oldFileName) {
			entry.filenameLong = newLFN
			entry.filenameShort = newShort
			entry.fileExtension = newExt
			entry.longFilenameSlots = calculateSlots(newLFN)
			entry.modifyTime = timestamp.GetTime()
			isReplaced = true
		}
		newEntries = append(newEntries, entry)
	}
	if !isReplaced {
		return fmt.Errorf("cannot find file entry for %s", oldFileName)
	}
	d.entries = newEntries
	return nil
}

// createVolumeLabel creates a volume label entry in the directory.
func (d *Directory) createVolumeLabel(name string) (*directoryEntry, error) {
	ts := timestamp.GetTime()
	entry := directoryEntry{
		filenameLong:      "",
		longFilenameSlots: -1,
		filenameShort:     name[:8],
		fileExtension:     name[8:11],
		fileSize:          0,
		clusterLocation:   0,
		filesystem:        d.filesystem,
		createTime:        ts,
		modifyTime:        ts,
		accessTime:        ts,
		isSubdirectory:    false,
		isNew:             true,
		isVolumeLabel:     true,
	}
	d.entries = append(d.entries, &entry)
	return &entry, nil
}
