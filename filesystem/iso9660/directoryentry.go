package iso9660

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

const (
	minDirectoryEntrySize uint8 = 34 // min size is all the required fields (33 bytes) plus 1 byte for the filename
)

// directoryEntry is a single directory entry
// also fulfills os.FileInfo
//   Name() string       // base name of the file
//   Size() int64        // length in bytes for regular files; system-dependent for others
//   Mode() FileMode     // file mode bits
//   ModTime() time.Time // modification time
//   IsDir() bool        // abbreviation for Mode().IsDir()
//   Sys() interface{}   // underlying data source (can return nil)
type directoryEntry struct {
	extAttrSize              uint8
	location                 uint32
	size                     uint32
	creation                 time.Time
	isHidden                 bool
	isSubdirectory           bool
	isAssociated             bool
	hasExtendedAttrs         bool
	hasOwnerGroupPermissions bool
	hasMoreEntries           bool
	isSelf                   bool
	isParent                 bool
	volumeSequence           uint16
	filesystem               *FileSystem
	filename                 string
}

func (de *directoryEntry) toBytes() ([]byte, error) {
	// size includes the ";1" at the end as two bytes if a filename
	var namelen byte
	switch {
	case de.isSelf:
		namelen = 1
	case de.isParent:
		namelen = 1
	default:
		namelen = uint8(len(de.filename))
	}
	// if even, we add one byte of padding to always end on an even byte
	if namelen%2 == 0 {
		namelen++
	}

	recordSize := 33 + namelen

	b := make([]byte, recordSize, recordSize)

	b[0] = recordSize
	b[1] = de.extAttrSize
	binary.LittleEndian.PutUint32(b[2:6], de.location)
	binary.BigEndian.PutUint32(b[6:10], de.location)
	binary.LittleEndian.PutUint32(b[10:14], de.size)
	binary.BigEndian.PutUint32(b[14:18], de.size)
	copy(b[18:25], timeToBytes(de.creation))

	// set the flags
	var flagByte byte = 0x00
	if de.isHidden {
		flagByte = flagByte | 0x01
	}
	if de.isSubdirectory {
		flagByte = flagByte | 0x02
	}
	if de.isAssociated {
		flagByte = flagByte | 0x04
	}
	if de.hasExtendedAttrs {
		flagByte = flagByte | 0x08
	}
	if de.hasOwnerGroupPermissions {
		flagByte = flagByte | 0x10
	}
	if de.hasMoreEntries {
		flagByte = flagByte | 0x80
	}
	b[25] = flagByte
	// volume sequence number - uint16 in both endian
	binary.LittleEndian.PutUint16(b[28:30], de.volumeSequence)
	binary.BigEndian.PutUint16(b[30:32], de.volumeSequence)

	b[32] = namelen

	// save the filename
	var filenameBytes []byte
	var err error
	switch {
	case de.isSelf:
		filenameBytes = []byte{0x00}
	case de.isParent:
		filenameBytes = []byte{0x01}
	default:
		// first validate the filename
		err = validateFilename(de.filename, de.isSubdirectory)
		if err != nil {
			nametype := "filename"
			if de.isSubdirectory {
				nametype = "directory"
			}
			return nil, fmt.Errorf("Invalid %s %s: %v", nametype, de.filename, err)
		}
		filenameBytes, err = stringToASCIIBytes(de.filename)
		if err != nil {
			return nil, fmt.Errorf("Error converting filename to bytes: %v", err)
		}
	}

	// copy it over
	copy(b[33:], filenameBytes)

	return b, nil
}

func dirEntryFromBytes(b []byte) (*directoryEntry, error) {
	// has to be at least 34 bytes
	if len(b) < int(minDirectoryEntrySize) {
		return nil, fmt.Errorf("Cannot read directoryEntry from %d bytes, fewer than minimum of %d bytes", len(b), minDirectoryEntrySize)
	}
	recordSize := b[0]
	// what if it is not the right size?
	if len(b) != int(recordSize) {
		return nil, fmt.Errorf("directoryEntry should be size %d bytes according to first byte, but have %d bytes", recordSize, len(b))
	}
	extAttrSize := b[1]
	location := binary.LittleEndian.Uint32(b[2:6])
	size := binary.LittleEndian.Uint32(b[10:14])
	creation := bytesToTime(b[18:25])

	// get the flags
	flagByte := b[25]
	isHidden := flagByte&0x01 == 0x01
	isSubdirectory := flagByte&0x02 == 0x02
	isAssociated := flagByte&0x04 == 0x04
	hasExtendedAttrs := flagByte&0x08 == 0x08
	hasOwnerGroupPermissions := flagByte&0x10 == 0x10
	hasMoreEntries := flagByte&0x80 == 0x80

	volumeSequence := binary.LittleEndian.Uint16(b[28:30])

	// size includes the ";1" at the end as two bytes and any padding
	namelen := b[32]

	// get the filename itself
	nameBytes := b[33 : 33+namelen]
	if namelen > 1 && nameBytes[namelen-1] == 0x00 {
		nameBytes = nameBytes[:namelen-1]
	}
	var filename string
	var isSelf, isParent bool
	switch {
	case namelen == 1 && nameBytes[0] == 0x00:
		filename = ""
		isSelf = true
	case namelen == 1 && nameBytes[0] == 0x01:
		filename = ""
		isParent = true
	default:
		filename = string(nameBytes)
	}

	return &directoryEntry{
		extAttrSize:              extAttrSize,
		location:                 location,
		size:                     size,
		creation:                 creation,
		isHidden:                 isHidden,
		isSubdirectory:           isSubdirectory,
		isAssociated:             isAssociated,
		hasExtendedAttrs:         hasExtendedAttrs,
		hasOwnerGroupPermissions: hasOwnerGroupPermissions,
		hasMoreEntries:           hasMoreEntries,
		isSelf:                   isSelf,
		isParent:                 isParent,
		volumeSequence:           volumeSequence,
		filename:                 filename,
	}, nil
}

// parseDirEntries takes all of the bytes in a special file (i.e. a directory)
// and gets all of the DirectoryEntry for that directory
// this is, essentially, the equivalent of `ls -l` or if you prefer `dir`
func parseDirEntries(b []byte, f *FileSystem) ([]*directoryEntry, error) {
	dirEntries := make([]*directoryEntry, 0, 20)
	count := 0
	for i := 0; i < len(b); count++ {
		// empty entry means nothing more to read - this might not actually be accurate, but work with it for now
		entryLen := int(b[i+0])
		if entryLen == 0 {
			i += (int(f.blocksize) - i%int(f.blocksize))
			continue
		}
		// get the bytes
		de, err := dirEntryFromBytes(b[i+0 : i+entryLen])
		if err != nil {
			return nil, fmt.Errorf("Invalid directory entry %d at byte %d: %v", count, i, err)
		}
		de.filesystem = f
		dirEntries = append(dirEntries, de)
		i += entryLen
	}
	return dirEntries, nil
}

// get the location of a particular path relative to this directory
func (de *directoryEntry) getLocation(p string) (uint32, uint32, error) {
	// break path down into parts and levels
	parts, err := splitPath(p)
	if err != nil {
		return 0, 0, fmt.Errorf("Could not parse path: %v", err)
	}
	var location, size uint32
	if len(parts) == 0 {
		location = de.location
		size = de.size
	} else {
		current := parts[0]
		// read the directory bytes
		dirb := make([]byte, de.size, de.size)
		n, err := de.filesystem.file.ReadAt(dirb, int64(de.location)*de.filesystem.blocksize)
		if err != nil {
			return 0, 0, fmt.Errorf("Could not read directory: %v", err)
		}
		if n != len(dirb) {
			return 0, 0, fmt.Errorf("Read %d bytes instead of expected %d", n, len(dirb))
		}
		// parse those entries
		dirEntries, err := parseDirEntries(dirb, de.filesystem)
		if err != nil {
			return 0, 0, fmt.Errorf("Could not parse directory: %v", err)
		}
		// find the entry among the children that has the desired name
		for _, entry := range dirEntries {
			if entry.filename == current {
				if len(parts) > 1 {
					// just dig down further
					location, size, err = entry.getLocation(path.Join(parts[1:]...))
					if err != nil {
						return 0, 0, fmt.Errorf("Could not get location: %v", err)
					}
				} else {
					// this is the final one, we found it, keep it
					location = entry.location
					size = entry.size
				}
				break
			}
		}
	}

	return location, size, nil
}

// Name() string       // base name of the file
func (de *directoryEntry) Name() string {
	name := de.filename
	// filenames should have the ';1' stripped off, as well as the leading or trailing '.'
	if !de.IsDir() {
		name = strings.TrimSuffix(name, ";1")
		name = strings.TrimSuffix(name, ".")
		name = strings.TrimPrefix(name, ".")
	}
	return name
}

// Size() int64        // length in bytes for regular files; system-dependent for others
func (de *directoryEntry) Size() int64 {
	return int64(de.size)
}

// Mode() FileMode     // file mode bits
func (de *directoryEntry) Mode() os.FileMode {
	return 0755
}

// ModTime() time.Time // modification time
func (de *directoryEntry) ModTime() time.Time {
	return de.creation
}

// IsDir() bool        // abbreviation for Mode().IsDir()
func (de *directoryEntry) IsDir() bool {
	return de.isSubdirectory
}

// Sys() interface{}   // underlying data source (can return nil)
func (de *directoryEntry) Sys() interface{} {
	return nil
}

// utilities

func bytesToTime(b []byte) time.Time {
	year := int(b[0])
	month := time.Month(b[1])
	date := int(b[2])
	hour := int(b[3])
	minute := int(b[4])
	second := int(b[5])
	offset := int(int8(b[6]))
	location := time.FixedZone("iso", offset*15*60)
	return time.Date(year+1900, month, date, hour, minute, second, 0, location)
}

func timeToBytes(t time.Time) []byte {
	year := t.Year()
	month := t.Month()
	date := t.Day()
	second := t.Second()
	minute := t.Minute()
	hour := t.Hour()
	_, offset := t.Zone()
	b := make([]byte, 7, 7)
	b[0] = byte(year - 1900)
	b[1] = byte(month)
	b[2] = byte(date)
	b[3] = byte(hour)
	b[4] = byte(minute)
	b[5] = byte(second)
	b[6] = byte(int8(offset / 60 / 15))
	return b
}

// convert a string to ascii bytes, but only accept valid d-characters
func validateFilename(s string, isDir bool) error {
	var err error
	// 		return nil, fmt.Errorf("Invalid d-character")
	if isDir {
		// directory only allowed up to 8 characters of A-Z,0-9,_
		re := regexp.MustCompile("^[A-Z0-9_]{1,30}$")
		if !re.MatchString(s) {
			err = fmt.Errorf("Directory name must be of up to 30 characters from A-Z0-9_")
		}
	} else {
		// filename only allowed up to 8 characters of A-Z,0-9,_, plus an optional '.' plus up to 3 characters of A-Z,0-9,_, plus must have ";1"
		re := regexp.MustCompile("^[A-Z0-9_]+(.[A-Z0-9_]*)?;1$")
		switch {
		case !re.MatchString(s):
			err = fmt.Errorf("File name must be of characters from A-Z0-9_, followed by an optional '.' and an extension of the same characters")
		case len(strings.Replace(s, ".", "", -1)) > 30:
			err = fmt.Errorf("File name must be at most 30 characters, not including the separator '.'")
		}
	}
	return err
}

// convert a string to a byte array, if all characters are valid ascii
func stringToASCIIBytes(s string) ([]byte, error) {
	length := len(s)
	b := make([]byte, length, length)
	// convert the name into 11 bytes
	r := []rune(s)
	// take the first 8 characters
	for i := 0; i < length; i++ {
		val := int(r[i])
		// we only can handle values less than max byte = 255
		if val > 255 {
			return nil, fmt.Errorf("Non-ASCII character in name: %s", s)
		}
		b[i] = byte(val)
	}
	return b, nil
}

// converts a string into upper-case with only valid characters
func uCaseValid(name string) string {
	// easiest way to do this is to go through the name one char at a time
	r := []rune(name)
	r2 := make([]rune, 0, len(r))
	for _, val := range r {
		switch {
		case (0x30 <= val && val <= 0x39) || (0x41 <= val && val <= 0x5a) || (val == 0x7e):
			// naturally valid characters
			r2 = append(r2, val)
		case (0x61 <= val && val <= 0x7a):
			// lower-case characters should be upper-cased
			r2 = append(r2, val-32)
		case val == ' ' || val == '.':
			// remove spaces and periods
			continue
		default:
			// replace the rest with _
			r2 = append(r2, '_')
		}
	}
	return string(r2)
}
