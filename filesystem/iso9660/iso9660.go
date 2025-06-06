package iso9660

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/filesystem"
)

const (
	volumeDescriptorSize int64 = 2 * KB  // each volume descriptor is 2KB
	systemAreaSize       int64 = 32 * KB // 32KB system area size
	defaultSectorSize    int64 = 2 * KB
	// MaxBlocks maximum number of blocks allowed in an iso9660 filesystem
	MaxBlocks int64 = 4.294967296e+09 // 2^32
)

// FileSystem implements the FileSystem interface
type FileSystem struct {
	workspace      string
	size           int64
	start          int64
	backend        backend.Storage
	blocksize      int64
	volumes        volumeDescriptors
	pathTable      *pathTable
	rootDir        *directoryEntry
	suspEnabled    bool  // is the SUSP in use?
	suspSkip       uint8 // how many bytes to skip in each directory record
	suspExtensions []suspExtension
}

// Equal compare if two filesystems are equal
func (fsm *FileSystem) Equal(a *FileSystem) bool {
	localMatch := fsm.backend == a.backend && fsm.size == a.size
	vdMatch := fsm.volumes.equal(&a.volumes)
	return localMatch && vdMatch
}

// Workspace get the workspace path
func (fsm *FileSystem) Workspace() string {
	return fsm.workspace
}

// Create creates an ISO9660 filesystem in a given directory
//
// requires the backend.Storage where to create the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the backend.Storage to create the filesystem,
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
// If the provided blocksize is 0, it will use the default of 2 KB.
func Create(b backend.Storage, size, start, blocksize int64, workspace string) (*FileSystem, error) {
	if blocksize == 0 {
		blocksize = defaultSectorSize
	}
	// make sure it is an allowed blocksize
	if err := validateBlocksize(blocksize); err != nil {
		return nil, err
	}
	// size of 0 means to use defaults
	if size != 0 && size > MaxBlocks*blocksize {
		return nil, fmt.Errorf("requested size is larger than maximum allowed ISO9660 size of %d blocks", MaxBlocks)
	}
	// at bare minimum, it must have enough space for the system area, one volume descriptor, one volume decriptor set terminator, and one block of data
	if size != 0 && size < systemAreaSize+2*volumeDescriptorSize+blocksize {
		return nil, fmt.Errorf("requested size is smaller than minimum allowed ISO9660 size: system area (%d), one volume descriptor (%d), one volume descriptor set terminator (%d), and one block (%d)", systemAreaSize, volumeDescriptorSize, volumeDescriptorSize, blocksize)
	}

	var workdir string
	if workspace != "" {
		info, err := os.Stat(workspace)
		if err != nil {
			return nil, fmt.Errorf("could not stat working directory: %v", err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("provided workspace is not a directory: %s", workspace)
		}
		workdir = workspace
	} else {
		// create a temporary working area where we can create the filesystem.
		//  It is only on `Finalize()` that we write it out to the actual disk file
		var err error
		workdir, err = os.MkdirTemp("", "diskfs_iso")
		if err != nil {
			return nil, fmt.Errorf("could not create working directory: %v", err)
		}
	}

	// sometimes, at least on macos, extra separators in path can cause panic
	workdir = filepath.Clean(workdir)

	// create root directory
	// there is nothing in there
	return &FileSystem{
		workspace: workdir,
		start:     start,
		size:      size,
		backend:   b,
		volumes:   volumeDescriptors{},
		blocksize: blocksize,
	}, nil
}

// Read reads a filesystem from a given disk.
//
// requires the backend.File where to read the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the backend.File the filesystem is expected to begin,
// and blocksize is is the physical blocksize to use for reading the filesystem
//
// note that you are *not* required to read a filesystem on the entire disk. You could have a disk of size
// 20GB, and a small filesystem of size 50MB that begins 2GB into the disk.
// This is extremely useful for working with filesystems on disk partitions.
//
// Note, however, that it is much easier to do this using the higher-level APIs at github.com/diskfs/go-diskfs
// which allow you to work directly with partitions, rather than having to calculate (and hopefully not make any errors)
// where a partition starts and ends.
//
// If the provided blocksize is 0, it will use the default of 2K bytes
func Read(b backend.Storage, size, start, blocksize int64) (*FileSystem, error) {
	var read int

	if blocksize == 0 {
		blocksize = defaultSectorSize
	}
	// make sure it is an allowed blocksize
	if err := validateBlocksize(blocksize); err != nil {
		return nil, err
	}
	// default size of 0 means use whatever size is available
	if size != 0 && size > MaxBlocks*blocksize {
		return nil, fmt.Errorf("requested size is larger than maximum allowed ISO9660 size of %d blocks", MaxBlocks)
	}
	// at bare minimum, it must have enough space for the system area, one volume descriptor, one volume decriptor set terminator, and one block of data
	if size != 0 && size < systemAreaSize+2*volumeDescriptorSize+blocksize {
		return nil, fmt.Errorf("requested size is too small to allow for system area (%d), one volume descriptor (%d), one volume descriptor set terminator (%d), and one block (%d)", systemAreaSize, volumeDescriptorSize, volumeDescriptorSize, blocksize)
	}

	// load the information from the disk
	// read system area
	systemArea := make([]byte, systemAreaSize)
	n, err := b.ReadAt(systemArea, start)
	if err != nil {
		return nil, fmt.Errorf("could not read bytes from file: %v", err)
	}
	if uint16(n) < uint16(systemAreaSize) {
		return nil, fmt.Errorf("only could read %d bytes from file", n)
	}
	// we do not do anything with the system area for now

	// next read the volume descriptors, one at a time, until we hit the terminator
	vds := make([]volumeDescriptor, 0, 128)
	terminated := false
	var (
		pvd *primaryVolumeDescriptor
		vd  volumeDescriptor
	)
	for i := 0; !terminated; i++ {
		vdBytes := make([]byte, volumeDescriptorSize)
		// read vdBytes
		read, err = b.ReadAt(vdBytes, start+systemAreaSize+int64(i)*volumeDescriptorSize)
		if err != nil {
			return nil, fmt.Errorf("unable to read bytes for volume descriptor %d: %v", i, err)
		}
		if int64(read) != volumeDescriptorSize {
			return nil, fmt.Errorf("read %d bytes instead of expected %d for volume descriptor %d", read, volumeDescriptorSize, i)
		}
		// convert to a vd structure
		vd, err = volumeDescriptorFromBytes(vdBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading Volume Descriptor: %v", err)
		}
		// is this a terminator?
		//nolint:exhaustive // we only are looking for the terminators; all of the rest are covered by default
		switch vd.Type() {
		case volumeDescriptorTerminator:
			terminated = true
		case volumeDescriptorPrimary:
			vds = append(vds, vd)
			pvd, _ = vd.(*primaryVolumeDescriptor)
		default:
			vds = append(vds, vd)
		}
	}

	// load up our path table and root directory entry
	var (
		pt           *pathTable
		rootDirEntry *directoryEntry
	)
	if pvd != nil {
		rootDirEntry = pvd.rootDirectoryEntry
		pathTableBytes := make([]byte, pvd.pathTableSize)
		pathTableLocation := pvd.pathTableLLocation * uint32(pvd.blocksize)
		read, err = b.ReadAt(pathTableBytes, int64(pathTableLocation))
		if err != nil {
			return nil, fmt.Errorf("unable to read path table of size %d at location %d: %v", pvd.pathTableSize, pathTableLocation, err)
		}
		if read != len(pathTableBytes) {
			return nil, fmt.Errorf("read %d bytes of path table instead of expected %d at location %d", read, pvd.pathTableSize, pathTableLocation)
		}
		pt = parsePathTable(pathTableBytes)
	}

	// is system use enabled?
	location := int64(rootDirEntry.location) * blocksize
	// get the size of the directory entry
	dirEntBytes := make([]byte, 1)
	read, err = b.ReadAt(dirEntBytes, location)
	if err != nil {
		return nil, fmt.Errorf("unable to read root directory size at location %d: %v", location, err)
	}
	if read != len(dirEntBytes) {
		return nil, fmt.Errorf("root directory entry size, read %d bytes instead of expected %d", read, len(dirEntBytes))
	}
	if dirEntBytes[0] == 0 {
		return nil, fmt.Errorf("root directory entry size at location %d was zero, check header and blocksize, given as %d", location, blocksize)
	}
	// now read the whole entry
	dirEntBytes = make([]byte, dirEntBytes[0])
	read, err = b.ReadAt(dirEntBytes, location)
	if err != nil {
		return nil, fmt.Errorf("unable to read root directory entry at location %d: %v", location, err)
	}
	if read != len(dirEntBytes) {
		return nil, fmt.Errorf("root directory entry, read %d bytes instead of expected %d", read, len(dirEntBytes))
	}
	// parse it - we do not have any handlers yet
	de, err := parseDirEntry(dirEntBytes, &FileSystem{
		suspEnabled: true,
		backend:     b,
		blocksize:   blocksize,
	})
	if err != nil {
		return nil, fmt.Errorf("error parsing root entry from bytes: %v", err)
	}
	// is the SUSP in use?
	var (
		suspEnabled  bool
		skipBytes    uint8
		suspHandlers []suspExtension
	)
	for _, ext := range de.extensions {
		if s, ok := ext.(directoryEntrySystemUseExtensionSharingProtocolIndicator); ok {
			suspEnabled = true
			skipBytes = s.SkipBytes()
		}

		// register any extension handlers
		if s, ok := ext.(directoryEntrySystemUseExtensionReference); suspEnabled && ok {
			extHandler := getRockRidgeExtension(s.ExtensionID())
			if extHandler != nil {
				suspHandlers = append(suspHandlers, extHandler)
			}
		}
	}

	fs := &FileSystem{
		workspace: "", // no workspace when we do nothing with it
		start:     start,
		size:      size,
		backend:   b,
		volumes: volumeDescriptors{
			descriptors: vds,
			primary:     pvd,
		},
		blocksize:      blocksize,
		pathTable:      pt,
		rootDir:        rootDirEntry,
		suspEnabled:    suspEnabled,
		suspSkip:       skipBytes,
		suspExtensions: suspHandlers,
	}
	rootDirEntry.filesystem = fs
	return fs, nil
}

// interface guard
var _ filesystem.FileSystem = (*FileSystem)(nil)

// Delete the temporary directory created during the iso9660 image creation
func (fsm *FileSystem) Close() error {
	if fsm.workspace != "" {
		return os.RemoveAll(fsm.workspace)
	}
	return nil
}

// Type returns the type code for the filesystem. Always returns filesystem.TypeFat32
func (fsm *FileSystem) Type() filesystem.Type {
	return filesystem.TypeISO9660
}

// Mkdir make a directory at the given path. It is equivalent to `mkdir -p`, i.e. idempotent, in that:
//
// * It will make the entire tree path if it does not exist
// * It will not return an error if the path already exists
//
// if readonly and not in workspace, will return an error
func (fsm *FileSystem) Mkdir(p string) error {
	if fsm.workspace == "" {
		return filesystem.ErrReadonlyFilesystem
	}
	err := os.MkdirAll(path.Join(fsm.workspace, p), 0o755)
	if err != nil {
		return fmt.Errorf("could not create directory %s: %v", p, err)
	}
	// we are not interesting in returning the entries
	return err
}

// creates a filesystem node (file, device special file, or named pipe) named pathname,
// with attributes specified by mode and dev
//
//nolint:revive // parameters will be used eventually
func (fsm *FileSystem) Mknod(pathname string, mode uint32, dev int) error {
	// Rock Ridge has device files support
	// https://en.wikipedia.org/wiki/ISO_9660#Rock_Ridge
	return filesystem.ErrNotImplemented
}

// creates a new link (also known as a hard link) to an existing file.
func (fsm *FileSystem) Link(_, _ string) error {
	return filesystem.ErrNotSupported
}

// creates a symbolic link named linkpath which contains the string target.
//
//nolint:revive // parameters will be used eventually
func (fsm *FileSystem) Symlink(oldpath, newpath string) error {
	// Rock Ridge has symlink support
	// https://en.wikipedia.org/wiki/ISO_9660#Rock_Ridge
	return filesystem.ErrNotImplemented
}

// Chmod changes the mode of the named file to mode. If the file is a symbolic link,
// it changes the mode of the link's target.
//
//nolint:revive // parameters will be used eventually
func (fsm *FileSystem) Chmod(name string, mode os.FileMode) error {
	// Rock Ridge has UNIX-style file modes support
	// https://en.wikipedia.org/wiki/ISO_9660#Rock_Ridge
	return filesystem.ErrNotImplemented
}

// Chown changes the numeric uid and gid of the named file. If the file is a symbolic link,
// it changes the uid and gid of the link's target. A uid or gid of -1 means to not change that value
//
//nolint:revive // parameters will be used eventually
func (fsm *FileSystem) Chown(name string, uid, gid int) error {
	// Rock Ridge has user ids and group ids support
	// https://en.wikipedia.org/wiki/ISO_9660#Rock_Ridge
	return filesystem.ErrNotImplemented
}

// ReadDir return the contents of a given directory in a given filesystem.
//
// Returns a slice of os.FileInfo with all of the entries in the directory.
//
// Will return an error if the directory does not exist or is a regular file and not a directory
func (fsm *FileSystem) ReadDir(p string) ([]os.FileInfo, error) {
	var fi []os.FileInfo
	// non-workspace: read from iso9660
	// workspace: read from regular filesystem
	if fsm.workspace != "" {
		fullPath := path.Join(fsm.workspace, p)
		// read the entries
		dirEntries, err := os.ReadDir(fullPath)
		if err != nil {
			return nil, fmt.Errorf("could not read directory %s: %v", p, err)
		}
		for _, e := range dirEntries {
			info, err := e.Info()
			if err != nil {
				return nil, fmt.Errorf("could not read directory %s: %v", p, err)
			}
			fi = append(fi, info)
		}
	} else {
		dirEntries, err := fsm.readDirectory(p)
		if err != nil {
			return nil, fmt.Errorf("error reading directory %s: %v", p, err)
		}
		fi = make([]os.FileInfo, 0, len(dirEntries))
		for _, entry := range dirEntries {
			// ignore any entry that is current directory or parent
			if entry.isSelf || entry.isParent {
				continue
			}
			fi = append(fi, entry)
		}
	}
	return fi, nil
}

// OpenFile returns an io.ReadWriter from which you can read the contents of a file
// or write contents to the file
//
// accepts normal os.OpenFile flags
//
// returns an error if the file does not exist
func (fsm *FileSystem) OpenFile(p string, flag int) (filesystem.File, error) {
	var f filesystem.File
	var err error

	// get the path and filename
	dir := path.Dir(p)
	filename := path.Base(p)

	// if the dir == filename, then it is just /
	if dir == filename {
		return nil, fmt.Errorf("cannot open directory %s as file", p)
	}

	// cannot open to write or append or create if we do not have a workspace
	writeMode := flag&os.O_WRONLY != 0 || flag&os.O_RDWR != 0 || flag&os.O_APPEND != 0 || flag&os.O_CREATE != 0 || flag&os.O_TRUNC != 0 || flag&os.O_EXCL != 0
	if fsm.workspace == "" {
		if writeMode {
			return nil, filesystem.ErrReadonlyFilesystem
		}

		// get the directory entries
		var entries []*directoryEntry
		entries, err = fsm.readDirectory(dir)
		if err != nil {
			return nil, fmt.Errorf("could not read directory entries for %s", dir)
		}
		// we now know that the directory exists, see if the file exists
		var targetEntry *directoryEntry
		for _, e := range entries {
			eName := e.Name()
			// cannot do anything with directories
			if eName == filename && e.IsDir() {
				return nil, fmt.Errorf("cannot open directory %s as file", p)
			}
			if eName == filename {
				// if we got this far, we have found the file
				targetEntry = e
				break
			}
		}

		// see if the file exists
		// if the file does not exist, and is not opened for os.O_CREATE, return an error
		if targetEntry == nil {
			return nil, fmt.Errorf("target file %s does not exist", p)
		}
		// now open the file
		f = &File{
			directoryEntry: targetEntry,
			isReadWrite:    false,
			isAppend:       false,
			offset:         0,
		}
	} else {
		f, err = os.OpenFile(path.Join(fsm.workspace, p), flag, 0o644)
		if err != nil {
			return nil, fmt.Errorf("target file %s does not exist: %v", p, err)
		}
	}

	return f, nil
}

// Rename renames (moves) oldpath to newpath. If newpath already exists and is not a directory, Rename replaces it.
func (fsm *FileSystem) Rename(oldpath, newpath string) error {
	if fsm.workspace == "" {
		return filesystem.ErrReadonlyFilesystem
	}
	return os.Rename(path.Join(fsm.workspace, oldpath), path.Join(fsm.workspace, newpath))
}

func (fsm *FileSystem) Remove(p string) error {
	if fsm.workspace == "" {
		return filesystem.ErrReadonlyFilesystem
	}
	return os.Remove(path.Join(fsm.workspace, p))
}

// readDirectory - read directory entry on iso only (not workspace)
func (fsm *FileSystem) readDirectory(p string) ([]*directoryEntry, error) {
	var (
		location, size uint32
		err            error
		n              int
	)

	// try from path table, then walk the directory tree, unless we were told explicitly not to
	usePathtable := true
	for _, e := range fsm.suspExtensions {
		usePathtable = e.UsePathtable()
		if !usePathtable {
			break
		}
	}

	if usePathtable {
		location = fsm.pathTable.getLocation(p)
	}

	// if we found it, read the first directory entry to get the size
	if location != 0 {
		// we need 4 bytes to read the size of the directory; it is at offset 10 from beginning
		dirb := make([]byte, 4)
		n, err = fsm.backend.ReadAt(dirb, int64(location)*fsm.blocksize+10)
		if err != nil {
			return nil, fmt.Errorf("could not read directory %s: %v", p, err)
		}
		if n != len(dirb) {
			return nil, fmt.Errorf("read %d bytes instead of expected %d", n, len(dirb))
		}
		// convert to uint32
		size = binary.LittleEndian.Uint32(dirb)
	} else {
		// if we could not find the location in the path table, try reading directly from the disk
		//   it is slow, but this is how Unix does it, since many iso creators *do* create illegitimate disks
		location, size, err = fsm.rootDir.getLocation(p)
		if err != nil {
			return nil, fmt.Errorf("unable to read directory tree for %s: %v", p, err)
		}
	}

	// did we still not find it?
	if location == 0 {
		return nil, fmt.Errorf("could not find directory %s", p)
	}

	// we have a location, let's read the directories from it
	b := make([]byte, size)
	n, err = fsm.backend.ReadAt(b, int64(location)*fsm.blocksize)
	if err != nil {
		return nil, fmt.Errorf("could not read directory entries for %s: %v", p, err)
	}
	if n != int(size) {
		return nil, fmt.Errorf("reading directory %s returned %d bytes read instead of expected %d", p, n, size)
	}
	// parse the entries
	entries, err := parseDirEntries(b, fsm)
	if err != nil {
		return nil, fmt.Errorf("could not parse directory entries for %s: %v", p, err)
	}
	return entries, nil
}

func validateBlocksize(blocksize int64) error {
	switch blocksize {
	case 0, 2048, 4096, 8192:
		return nil
	default:
		return fmt.Errorf("blocksize for ISO9660 must be one of 2048, 4096, 8192")
	}
}

func (fsm *FileSystem) Label() string {
	if fsm.volumes.primary == nil {
		return ""
	}
	return fsm.volumes.primary.volumeIdentifier
}

func (fsm *FileSystem) SetLabel(string) error {
	return fmt.Errorf("ISO9660 filesystem is read-only")
}
