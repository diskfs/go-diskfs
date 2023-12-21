package squashfs

import (
	"os"
	"time"
)

// FileStat is the extended data underlying a single file, similar to https://golang.org/pkg/syscall/#Stat_t
type FileStat = *directoryEntry

// directoryEntry is a single directory entry
// it combines information from inode and the actual entry
// also fulfills os.FileInfo
//
//	Name() string       // base name of the file
//	Size() int64        // length in bytes for regular files; system-dependent for others
//	Mode() FileMode     // file mode bits
//	ModTime() time.Time // modification time
//	IsDir() bool        // abbreviation for Mode().IsDir()
//	Sys() interface{}   // underlying data source (can return nil)
type directoryEntry struct {
	isSubdirectory bool
	name           string
	size           int64
	modTime        time.Time
	mode           os.FileMode
	inode          inode
	uid            uint32
	gid            uint32
	xattrs         map[string]string
}

func (d *directoryEntry) equal(o *directoryEntry) bool {
	if o == nil {
		return false
	}
	if d.inode == nil && o.inode == nil {
		return true
	}
	if (d.inode == nil && o.inode != nil) || (d.inode != nil && o.inode == nil) {
		return false
	}
	if !d.inode.equal(o.inode) {
		return false
	}
	return d.isSubdirectory == o.isSubdirectory && d.name == o.name && d.size == o.size && d.modTime == o.modTime && d.mode == o.mode
}

// Name string       // base name of the file
func (d *directoryEntry) Name() string {
	return d.name
}

// Size int64        // length in bytes for regular files; system-dependent for others
func (d *directoryEntry) Size() int64 {
	return d.size
}

// IsDir bool        // abbreviation for Mode().IsDir()
func (d *directoryEntry) IsDir() bool {
	return d.isSubdirectory
}

// ModTime time.Time // modification time
func (d *directoryEntry) ModTime() time.Time {
	return d.modTime
}

// Mode FileMode     // file mode bits
func (d *directoryEntry) Mode() os.FileMode {
	mode := d.mode

	// We need to adjust the Linux mode into a Go mode
	// The bottom 3*3 bits are the traditional unix permissions.

	// Clear the non permissions bits
	mode &= os.ModePerm

	if d.inode == nil {
		return mode
	}
	switch d.inode.inodeType() {
	case inodeBasicDirectory, inodeExtendedDirectory:
		mode |= os.ModeDir // d: is a directory
	case inodeBasicFile, inodeExtendedFile:
		// zero mode
	case inodeBasicSymlink, inodeExtendedSymlink:
		mode |= os.ModeSymlink // L: symbolic link
	case inodeBasicBlock, inodeExtendedBlock:
		mode |= os.ModeDevice // D: device file
	case inodeBasicChar, inodeExtendedChar:
		mode |= os.ModeDevice     // D: device file
		mode |= os.ModeCharDevice // c: Unix character device, when ModeDevice is set
	case inodeBasicFifo, inodeExtendedFifo:
		mode |= os.ModeNamedPipe // p: named pipe (FIFO)
	case inodeBasicSocket, inodeExtendedSocket:
		mode |= os.ModeSocket // S: Unix domain socket
	default:
		mode |= os.ModeIrregular // ?: non-regular file; nothing else is known about this file
	}

	// Not currently translated
	// mode |= os.ModeAppend          // a: append-only
	// mode |= os.ModeExclusive       // l: exclusive use
	// mode |= os.ModeTemporary       // T: temporary file; Plan 9 only
	// mode |= os.ModeSetuid          // u: setuid
	// mode |= os.ModeSetgid          // g: setgid
	// mode |= os.ModeSticky          // t: sticky

	return mode
}

// Sys interface{}   // underlying data source (can return nil)
func (d *directoryEntry) Sys() interface{} {
	return d
}

// UID get uid of file
func (d *directoryEntry) UID() uint32 {
	return d.uid
}

// GID get gid of file
func (d *directoryEntry) GID() uint32 {
	return d.gid
}

// Xattrs get extended attributes of file
func (d *directoryEntry) Xattrs() map[string]string {
	return d.xattrs
}
