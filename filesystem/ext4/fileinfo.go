package ext4

import (
	"os"
	"time"
)

// FileInfo represents the information for an individual file
// it fulfills os.FileInfo interface
type FileInfo struct {
	modTime time.Time
	mode    os.FileMode
	name    string
	size    int64
	isDir   bool
	sys     *StatT
}

// StatT carries ext4-specific metadata returned by FileInfo.Sys().
type StatT struct {
	UID        uint32
	GID        uint32
	Major      uint32
	Minor      uint32
	Ino        uint32
	Nlink      uint16
	Blocks     uint64
	AccessTime time.Time
	ChangeTime time.Time
	CreateTime time.Time
	Flags      InodeFlags
	LinkTarget string
}

// InodeFlags exposes the practically useful subset of ext4 inode flags.
type InodeFlags struct {
	Immutable          bool
	AppendOnly         bool
	NoDump             bool
	NoAtime            bool
	Synchronous        bool
	Compressed         bool
	Encrypted          bool
	HashedIndexes      bool
	JournalData        bool
	HugeFile           bool
	Extents            bool
	ExtendedAttributes bool
	InlineData         bool
	TopDirectory       bool
}

// IsDir abbreviation for Mode().IsDir()
func (fi *FileInfo) IsDir() bool {
	return fi.isDir
}

// ModTime modification time
func (fi *FileInfo) ModTime() time.Time {
	return fi.modTime
}

// Mode returns file mode
func (fi *FileInfo) Mode() os.FileMode {
	return fi.mode
}

// Name base name of the file
//
//	will return the long name of the file. If none exists, returns the shortname and extension
func (fi *FileInfo) Name() string {
	return fi.name
}

// Size length in bytes for regular files
func (fi *FileInfo) Size() int64 {
	return fi.size
}

// Sys returns the underlying *StatT with ext4-specific metadata.
func (fi *FileInfo) Sys() interface{} {
	return fi.sys
}
