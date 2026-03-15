package fat12

import (
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/filesystem"
)

const (
	minClusterSize int = 128
	maxClusterSize int = 65529
)

// FileSystem implements filesystem.FileSystem for FAT12 (and, via embedding, FAT16 and FAT32).
type FileSystem struct {
	bootSector      msDosBootSector
	table           FATTable
	dataStart       uint32
	bytesPerCluster int
	size            int64
	start           int64
	backend         backend.Storage
	// rootDirOffset and rootDirMaxEntries implement the fixed-size root
	// directory region used by FAT12 and FAT16. When rootDirMaxEntries == 0
	// the filesystem treats the root directory as a regular cluster chain
	// (FAT32 behaviour).
	rootDirOffset     int64
	rootDirMaxEntries int
	// fatPrimaryStart and fatSecondaryStart are the byte offsets of the two
	// FAT copies within the partition. Stored explicitly so WriteFat does not
	// need to re-derive them from the BPB at every call.
	fatPrimaryStart   uint64
	fatSecondaryStart uint64
	// WriteBootSectorFn, when non-nil, is called by WriteBootSector instead of
	// the default fat12/16 boot-sector writer. FAT32 uses this to write the
	// dos71EBPB-format boot sector (including backup sector).
	WriteBootSectorFn func() error
	// AfterWriteFAT, when non-nil, is called at the end of WriteFat. FAT32
	// uses this to flush the FSInformationSector after every FAT change.
	AfterWriteFAT func() error
}

// NewFileSystem constructs a FileSystem with caller-supplied table and layout
// parameters. Called by fat16 and fat32 to reuse all high-level methods.
//
// bpb may be nil when the caller (fat32) manages its own boot-sector format
// and supplies WriteBootSectorFn / AfterWriteFAT hooks after construction.
func NewFileSystem(
	b backend.Storage,
	bpb *Dos40EBPB,
	tbl FATTable,
	dataStart uint32,
	bytesPerCluster int,
	size, start int64,
	rootDirOffset int64,
	rootDirMaxEntries int,
	fatPrimaryStart, fatSecondaryStart uint64,
) *FileSystem {
	bs := msDosBootSector{
		oemName:            "godiskfs",
		jumpInstruction:    [3]byte{0xeb, 0x3c, 0x90},
		biosParameterBlock: bpb,
		bootCode:           []byte{},
	}
	return &FileSystem{
		bootSector:        bs,
		table:             tbl,
		dataStart:         dataStart,
		bytesPerCluster:   bytesPerCluster,
		size:              size,
		start:             start,
		backend:           b,
		rootDirOffset:     rootDirOffset,
		rootDirMaxEntries: rootDirMaxEntries,
		fatPrimaryStart:   fatPrimaryStart,
		fatSecondaryStart: fatSecondaryStart,
	}
}

// Equal compares two FileSystems.
func (fs *FileSystem) Equal(a *FileSystem) bool {
	if fs == nil && a == nil {
		return true
	}
	if fs == nil || a == nil {
		return false
	}
	return fs.backend == a.backend &&
		fs.dataStart == a.dataStart &&
		fs.bytesPerCluster == a.bytesPerCluster &&
		fs.bootSector.equal(&a.bootSector)
}

// ── Create ────────────────────────────────────────────────────────────────────

// Create creates a FAT12 filesystem.
func Create(b backend.Storage, size, start, blocksize int64, volumeLabel string, reproducible bool) (*FileSystem, error) {
	if blocksize != int64(SectorSize512) && blocksize > 0 {
		return nil, fmt.Errorf("blocksize for FAT12 must be 512 or 0, not %d", blocksize)
	}
	if size > Fat12MaxSize {
		return nil, fmt.Errorf("size %d exceeds FAT12 maximum of %d", size, Fat12MaxSize)
	}
	if size < int64(SectorSize512)*4 {
		return nil, fmt.Errorf("size %d is too small for a FAT12 filesystem", size)
	}

	var volid uint32
	if !reproducible {
		now := time.Now()
		volid = uint32(now.Unix()<<20 | (now.UnixNano() / 1000000))
	}

	bytesPerSector := uint16(SectorSize512)
	totalSectors := uint32(size / int64(SectorSize512))

	// Cluster size per Microsoft FAT spec for FAT12.
	// The constraint is: dataClusters < 4085.
	// With 512-byte sectors and reservedSectors=1, rootDirSectors≈14:
	//   totalSectors/sectorsPerCluster must stay well below 4085.
	var sectorsPerCluster uint8
	switch {
	case size <= 2*MB:
		sectorsPerCluster = 1 // ≈2046 clusters at 2 MB
	case size <= 4*MB:
		sectorsPerCluster = 2 // ≈2045 clusters at 4 MB
	case size < 8*MB:
		sectorsPerCluster = 4 // ≈3580 clusters at 7 MB; exactly 8 MB overflows with s/c=4
	case size <= 16*MB:
		sectorsPerCluster = 8 // ≈2044 clusters at 8 MB
	case size <= 32*MB:
		sectorsPerCluster = 16
	case size <= 64*MB:
		sectorsPerCluster = 32
	default:
		sectorsPerCluster = 64
	}

	// Standard root directory entry count for FAT12.
	// 1.44 MB floppy: 224; others: 112 or 224.
	var rootDirEntries uint16 = 224
	if size <= 512*KB {
		rootDirEntries = 112
	}

	// Reserved sectors: 1 for FAT12/16.
	reservedSectors := uint16(1)
	fatCount := uint8(2)
	mediaType := uint8(MediaFixedDisk)
	if size <= 2*MB {
		mediaType = uint8(Media35Inch)
	}

	// Number of data clusters (estimate; iterate once to get sectorsPerFat).
	// sectorsPerFat for FAT12: ceil(numClusters * 3 / 2 / bytesPerSector).
	// numClusters depends on sectorsPerFat, so we iterate once.
	rootDirSectors := (uint32(rootDirEntries)*32 + uint32(bytesPerSector) - 1) / uint32(bytesPerSector)
	// Initial estimate.
	dataSectors := totalSectors - uint32(reservedSectors) - rootDirSectors
	numClusters := dataSectors / uint32(sectorsPerCluster)
	sectorsPerFat := uint16((numClusters*3/2 + uint32(bytesPerSector) - 1) / uint32(bytesPerSector))
	// Refine once with the FAT space now known.
	dataSectors = totalSectors - uint32(reservedSectors) - rootDirSectors - uint32(fatCount)*uint32(sectorsPerFat)
	numClusters = dataSectors / uint32(sectorsPerCluster)

	if numClusters >= 4085 {
		return nil, fmt.Errorf("computed cluster count %d is too large for FAT12 (max 4084); use FAT16", numClusters)
	}

	var ts16 uint16
	var ts32 uint32
	if totalSectors <= 0xFFFF {
		ts16 = uint16(totalSectors)
	} else {
		ts32 = totalSectors
	}

	dos20 := &Dos20BPB{
		BytesPerSector:       SectorSize512,
		SectorsPerCluster:    sectorsPerCluster,
		ReservedSectors:      reservedSectors,
		FatCount:             fatCount,
		RootDirectoryEntries: rootDirEntries,
		TotalSectors:         ts16,
		MediaType:            mediaType,
		SectorsPerFat:        sectorsPerFat,
	}
	dos331 := &Dos331BPB{
		Dos20BPB:        dos20,
		SectorsPerTrack: 18,
		Heads:           2,
		HiddenSectors:   0,
		TotalSectors32:  ts32,
	}
	bpb := &Dos40EBPB{
		Dos331BPB:          dos331,
		DriveNumber:        0x00,
		ReservedFlags:      0,
		ExtBootSignature:   longEBPB,
		VolumeSerialNumber: volid,
		VolumeLabel:        "NO NAME    ",
		FileSystemType:     "FAT12   ",
	}

	// Layout:
	//   [reserved sectors] [FAT1] [FAT2] [root dir region] [data clusters]
	fatPrimaryStart := uint32(reservedSectors) * uint32(bytesPerSector)
	fatSize := uint32(sectorsPerFat) * uint32(bytesPerSector)
	fatSecondaryStart := fatPrimaryStart + fatSize
	rootDirOff := fatSecondaryStart + fatSize
	dataStart := rootDirOff + rootDirSectors*uint32(bytesPerSector)
	bytesPerCluster := int(sectorsPerCluster) * int(bytesPerSector)

	// FAT ID: lower byte is media type.
	fatIDBase := uint32(0x0F00)
	fatID := fatIDBase | uint32(mediaType)

	tbl := newFat12Table(fatID, fatSize)
	// Mark cluster 2 (root dir cluster in FAT — though FAT12/16 root is fixed,
	// some tools still expect this cluster chain header to exist).
	tbl.clusters[2] = tbl.eoc

	fs := NewFileSystem(b, bpb, tbl,
		dataStart, bytesPerCluster, size, start,
		int64(rootDirOff), int(rootDirEntries),
		uint64(fatPrimaryStart), uint64(fatSecondaryStart))

	writableFile, err := b.Writable()
	if err != nil {
		return nil, err
	}

	// Write boot sector.
	if err := fs.WriteBootSector(); err != nil {
		return nil, fmt.Errorf("failed to write boot sector: %w", err)
	}

	// Write both FAT copies.
	if err := fs.WriteFat(); err != nil {
		return nil, fmt.Errorf("failed to write FAT: %w", err)
	}

	// Zero out the root directory region.
	rootDirSize := int(rootDirEntries) * 32
	zeros := make([]byte, rootDirSize)
	if _, err := writableFile.WriteAt(zeros, int64(rootDirOff)+start); err != nil {
		return nil, fmt.Errorf("failed to zero root directory: %w", err)
	}

	// Set volume label.
	if err := fs.SetLabel(volumeLabel); err != nil {
		return nil, fmt.Errorf("failed to set volume label: %w", err)
	}

	return fs, nil
}

// ── Read ──────────────────────────────────────────────────────────────────────

// Read reads a FAT12 filesystem from the backend.
// Returns an error if the image is not a valid FAT12 filesystem (e.g. it is FAT16/FAT32),
// so that disk.GetFilesystem() can fall through to the next candidate.
func Read(b backend.Storage, size, start, blocksize int64) (*FileSystem, error) {
	if blocksize != 0 && blocksize != int64(SectorSize512) {
		return nil, fmt.Errorf("blocksize for FAT12 must be 0 or 512, not %d", blocksize)
	}

	raw := make([]byte, SectorSize512)
	if _, err := b.ReadAt(raw, start); err != nil {
		return nil, fmt.Errorf("could not read boot sector: %w", err)
	}

	bs, err := msDosBootSectorFromBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("not a FAT12/16 filesystem (boot sector parse failed): %w", err)
	}

	bpb := bs.biosParameterBlock
	dos20 := bpb.Dos331BPB.Dos20BPB

	// FAT12/16 must have non-zero rootDirectoryEntries.
	if dos20.RootDirectoryEntries == 0 {
		return nil, errors.New("not a FAT12 filesystem: rootDirectoryEntries is zero (FAT32?)")
	}

	bytesPerSector := uint32(dos20.BytesPerSector)
	sectorsPerCluster := uint32(dos20.SectorsPerCluster)
	reservedSectors := uint32(dos20.ReservedSectors)
	fatCount := uint32(dos20.FatCount)
	sectorsPerFat := uint32(dos20.SectorsPerFat)
	rootDirEntries := uint32(dos20.RootDirectoryEntries)

	totalSectors := bpb.TotalSectors()
	rootDirSectors := (rootDirEntries*32 + bytesPerSector - 1) / bytesPerSector
	fatSectors := fatCount * sectorsPerFat
	dataSectors := totalSectors - reservedSectors - fatSectors - rootDirSectors
	numClusters := dataSectors / sectorsPerCluster

	if numClusters >= 4085 {
		return nil, fmt.Errorf("not a FAT12 filesystem: cluster count %d >= 4085", numClusters)
	}

	fatPrimaryStart := reservedSectors * bytesPerSector
	fatSize := sectorsPerFat * bytesPerSector
	fatSecondaryStart := fatPrimaryStart + fatSize
	rootDirOff := fatSecondaryStart + fatSize
	dataStart := rootDirOff + rootDirSectors*bytesPerSector
	bytesPerCluster := int(sectorsPerCluster) * int(bytesPerSector)

	fatIDBase := uint32(0x0F00)
	fatID := fatIDBase | uint32(dos20.MediaType)

	tbl := newFat12Table(fatID, fatSize)
	fatBytes := make([]byte, fatSize)
	if _, err := b.ReadAt(fatBytes, int64(fatPrimaryStart)+start); err != nil {
		return nil, fmt.Errorf("could not read FAT: %w", err)
	}
	tbl.FromBytes(fatBytes)

	return NewFileSystem(b, bpb, tbl,
		dataStart, bytesPerCluster, size, start,
		int64(rootDirOff), int(rootDirEntries),
		uint64(fatPrimaryStart), uint64(fatSecondaryStart)), nil
}

// ── internal write helpers ────────────────────────────────────────────────────

// Backend returns the storage backend. Used by fat32 to write FSIS.
func (fs *FileSystem) Backend() backend.Storage { return fs.backend }

// Start returns the filesystem start offset within the backend. Used by fat32.
func (fs *FileSystem) Start() int64 { return fs.start }

// BytesPerCluster returns the cluster size in bytes.
func (fs *FileSystem) BytesPerCluster() int { return fs.bytesPerCluster }

// DataStart returns the byte offset of the first data cluster.
func (fs *FileSystem) DataStart() uint32 { return fs.dataStart }

// WriteBootSector writes the boot sector to disk.
// When WriteBootSectorFn is set (by fat32), that function is called instead of
// the default fat12/16 writer.
func (fs *FileSystem) WriteBootSector() error {
	if fs.WriteBootSectorFn != nil {
		return fs.WriteBootSectorFn()
	}
	return fs.defaultWriteBootSector()
}

func (fs *FileSystem) defaultWriteBootSector() error {
	writableFile, err := fs.backend.Writable()
	if err != nil {
		return err
	}
	b, err := fs.bootSector.toBytes()
	if err != nil {
		return fmt.Errorf("error serialising boot sector: %w", err)
	}
	if _, err := writableFile.WriteAt(b, fs.start); err != nil {
		return fmt.Errorf("error writing boot sector: %w", err)
	}
	return nil
}

// WriteFat writes both FAT copies to disk. When AfterWriteFAT is set (by
// fat32), it is called afterwards to flush the FSInformationSector.
func (fs *FileSystem) WriteFat() error {
	if err := fs.defaultWriteFAT(); err != nil {
		return err
	}
	if fs.AfterWriteFAT != nil {
		return fs.AfterWriteFAT()
	}
	return nil
}

func (fs *FileSystem) defaultWriteFAT() error {
	fatBytes := fs.table.Bytes()
	writableFile, err := fs.backend.Writable()
	if err != nil {
		return err
	}
	if _, err := writableFile.WriteAt(fatBytes, int64(fs.fatPrimaryStart)+fs.start); err != nil {
		return fmt.Errorf("unable to write primary FAT: %w", err)
	}
	if _, err := writableFile.WriteAt(fatBytes, int64(fs.fatSecondaryStart)+fs.start); err != nil {
		return fmt.Errorf("unable to write secondary FAT: %w", err)
	}
	return nil
}

// ── filesystem.FileSystem interface ──────────────────────────────────────────

var _ filesystem.FileSystem = (*FileSystem)(nil)

func (fs *FileSystem) Close() error { return nil }

// Type returns filesystem.TypeFat12. fat16.FileSystem overrides this.
func (fs *FileSystem) Type() filesystem.Type { return filesystem.TypeFat12 }

func (fs *FileSystem) Mkdir(p string) error {
	_, _, err := fs.readDirWithMkdir(p, true)
	return err
}

func (fs *FileSystem) Mknod(_ string, _ uint32, _ int) error {
	return filesystem.ErrNotSupported
}
func (fs *FileSystem) Link(_, _ string) error              { return filesystem.ErrNotSupported }
func (fs *FileSystem) Symlink(_, _ string) error           { return filesystem.ErrNotSupported }
func (fs *FileSystem) Chmod(_ string, _ os.FileMode) error { return filesystem.ErrNotSupported }
func (fs *FileSystem) Chown(_ string, _, _ int) error      { return filesystem.ErrNotSupported }

func (fs *FileSystem) Chtimes(p string, ctime, atime, mtime time.Time) error {
	dir := path.Dir(p)
	filename := path.Base(p)
	if dir == filename {
		return fmt.Errorf("cannot open directory %s as file", p)
	}
	parentDir, entries, err := fs.readDirWithMkdir(dir, false)
	if err != nil {
		return fmt.Errorf("could not read directory entries for %s: %w", dir, err)
	}
	var entry *directoryEntry
	for _, e := range entries {
		if !e.nameMatches(filename) {
			continue
		}
		entry = e
	}
	if entry == nil {
		return fmt.Errorf("path %s not found", p)
	}
	entry.accessTime = atime
	entry.modifyTime = mtime
	entry.createTime = ctime
	return fs.writeDirectoryEntries(parentDir)
}

// GetArchiveBit returns the current state of the FAT archive attribute.
func (fs *FileSystem) GetArchiveBit(p string) (bool, error) {
	dir := path.Dir(p)
	filename := path.Base(p)
	if dir == filename {
		return false, fmt.Errorf("cannot get archive bit on root directory %s", p)
	}
	_, entries, err := fs.readDirWithMkdir(dir, false)
	if err != nil {
		return false, fmt.Errorf("could not read directory entries for %s: %w", dir, err)
	}
	for _, e := range entries {
		if !e.nameMatches(filename) {
			continue
		}
		return e.isArchiveDirty, nil
	}
	return false, fmt.Errorf("path %s not found", p)
}

// SetArchiveBit sets or clears the FAT archive attribute on the named file or directory.
func (fs *FileSystem) SetArchiveBit(p string, set bool) error {
	dir := path.Dir(p)
	filename := path.Base(p)
	if dir == filename {
		return fmt.Errorf("cannot set archive bit on root directory %s", p)
	}
	parentDir, entries, err := fs.readDirWithMkdir(dir, false)
	if err != nil {
		return fmt.Errorf("could not read directory entries for %s: %w", dir, err)
	}
	var entry *directoryEntry
	for _, e := range entries {
		if !e.nameMatches(filename) {
			continue
		}
		entry = e
	}
	if entry == nil {
		return fmt.Errorf("path %s not found", p)
	}
	entry.isArchiveDirty = set
	return fs.writeDirectoryEntries(parentDir)
}

func (fs *FileSystem) ReadDir(p string) ([]iofs.DirEntry, error) {
	if err := validatePath(p); err != nil {
		return nil, err
	}
	_, entries, err := fs.readDirWithMkdir(p, false)
	if err != nil {
		return nil, fmt.Errorf("error reading directory %s: %w", p, err)
	}
	ret := make([]iofs.DirEntry, 0, len(entries))
	for _, e := range entries {
		if e.isVolumeLabel || e.filenameShort == "" || e.filenameShort == ".." || e.filenameShort == "." {
			continue
		}
		ret = append(ret, e)
	}
	return ret, nil
}

func (fs *FileSystem) Open(p string) (iofs.File, error) {
	return fs.OpenFile(p, os.O_RDONLY)
}

func (fs *FileSystem) OpenFile(p string, flag int) (filesystem.File, error) {
	dir := path.Dir(p)
	filename := path.Base(p)
	parentDir, entries, err := fs.readDirWithMkdir(dir, false)
	if err != nil {
		return nil, fmt.Errorf("could not read directory entries for %s: %w", dir, err)
	}
	var targetEntry *directoryEntry
	if filename == dir {
		targetEntry = &parentDir.directoryEntry
	} else {
		for _, e := range entries {
			if !e.nameMatches(filename) {
				continue
			}
			targetEntry = e
		}
	}
	if targetEntry == nil {
		if flag&os.O_CREATE == 0 {
			return nil, fmt.Errorf("target file %s does not exist and was not asked to create", p)
		}
		targetEntry, err = fs.mkFile(parentDir, filename)
		if err != nil {
			return nil, fmt.Errorf("failed to create file %s: %w", p, err)
		}
		if err = fs.writeDirectoryEntries(parentDir); err != nil {
			return nil, fmt.Errorf("error writing directory file %s to disk: %w", p, err)
		}
	}
	offset := int64(0)
	if flag&os.O_TRUNC == os.O_TRUNC && targetEntry.fileSize != 0 {
		targetEntry.fileSize = 0
		if err := fs.writeDirectoryEntries(parentDir); err != nil {
			return nil, fmt.Errorf("error writing directory file %s to disk: %w", p, err)
		}
		if _, err := fs.allocateSpace(1, targetEntry.clusterLocation); err != nil {
			return nil, fmt.Errorf("unable to resize cluster list: %w", err)
		}
	}
	if flag&os.O_APPEND == os.O_APPEND {
		offset = int64(targetEntry.fileSize)
	}
	return &File{
		directoryEntry: targetEntry,
		isReadWrite:    flag&os.O_RDWR != 0,
		isAppend:       flag&os.O_APPEND != 0,
		offset:         offset,
		filesystem:     fs,
		parent:         parentDir,
	}, nil
}

func (fs *FileSystem) ReadFile(name string) ([]byte, error) {
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

func (fs *FileSystem) Remove(pathname string) error {
	dir := path.Dir(pathname)
	filename := path.Base(pathname)
	if dir == filename {
		return fmt.Errorf("cannot remove directory %s as file", pathname)
	}
	parentDir, entries, err := fs.readDirWithMkdir(dir, false)
	if err != nil {
		return fmt.Errorf("could not read directory entries for %s", dir)
	}
	var targetEntry *directoryEntry
	for _, e := range entries {
		if !e.nameMatches(filename) {
			continue
		}
		if e.isSubdirectory {
			content, err := fs.ReadDir(pathname)
			if err != nil {
				return fmt.Errorf("error while checking if directory to delete is empty: %w", err)
			}
			if len(content) > 0 {
				return fmt.Errorf("cannot remove non-empty directory %s", pathname)
			}
		}
		targetEntry = e
	}
	if targetEntry == nil {
		return fmt.Errorf("target file %s does not exist", pathname)
	}
	if err = parentDir.removeEntry(filename); err != nil {
		return fmt.Errorf("failed to remove file %s: %v", pathname, err)
	}
	if _, err = fs.allocateSpace(uint64(parentDir.fileSize), parentDir.clusterLocation); err != nil {
		return fmt.Errorf("failed to allocate clusters: %v", err)
	}
	return fs.writeDirectoryEntries(parentDir)
}

func (fs *FileSystem) Rename(oldpath, newpath string) error {
	dir := path.Dir(oldpath)
	filename := path.Base(oldpath)
	newDir := path.Dir(newpath)
	newname := path.Base(newpath)
	if dir != newDir {
		return errors.New("can only rename files within the same directory")
	}
	if dir == filename {
		return fmt.Errorf("cannot rename directory %s as file", oldpath)
	}
	parentDir, entries, err := fs.readDirWithMkdir(dir, false)
	if err != nil {
		return fmt.Errorf("could not read directory entries for %s", dir)
	}
	var targetEntry *directoryEntry
	for _, e := range entries {
		if !e.nameMatches(filename) {
			continue
		}
		targetEntry = e
	}
	if targetEntry == nil {
		return fmt.Errorf("target file %s does not exist", oldpath)
	}
	if err = parentDir.renameEntry(filename, newname); err != nil {
		return fmt.Errorf("failed to rename file %s: %v", oldpath, err)
	}
	if _, err = fs.allocateSpace(uint64(parentDir.fileSize), parentDir.clusterLocation); err != nil {
		return fmt.Errorf("failed to allocate clusters: %v", err)
	}
	return fs.writeDirectoryEntries(parentDir)
}

func (fs *FileSystem) Stat(name string) (iofs.FileInfo, error) {
	dir := path.Dir(name)
	basename := path.Base(name)
	des, err := fs.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("could not read directory %s: %v", dir, err)
	}
	if dir == basename && (basename == "/" || basename == ".") {
		rootDir, _, err := fs.readDirWithMkdir("/", false)
		if err != nil {
			return nil, fmt.Errorf("could not read root directory: %v", err)
		}
		return rootDir.Info()
	}
	for _, de := range des {
		if de.Name() == basename {
			return de.Info()
		}
	}
	return nil, &iofs.PathError{Op: "stat", Path: name, Err: fmt.Errorf("file %s not found in directory %s", basename, dir)}
}

func (fs *FileSystem) Label() string {
	_, dirEntries, err := fs.readDirWithMkdir("/", false)
	if err != nil {
		return ""
	}
	for _, entry := range dirEntries {
		if entry.isVolumeLabel {
			return entry.filenameShort + entry.fileExtension
		}
	}
	return ""
}

func (fs *FileSystem) SetLabel(volumeLabel string) error {
	if volumeLabel == "" {
		volumeLabel = "NO NAME"
	}
	volumeLabel = fmt.Sprintf("%-11.11s", volumeLabel)

	bpb := fs.bootSector.biosParameterBlock
	if bpb == nil {
		return fmt.Errorf("failed to load the boot sector")
	}
	bpb.VolumeLabel = volumeLabel
	if err := fs.WriteBootSector(); err != nil {
		return fmt.Errorf("failed to write the boot sector: %w", err)
	}

	return fs.SetRootDirLabel(volumeLabel)
}

// SetRootDirLabel updates the volume-label directory entry in the root directory
// without touching the boot sector. Used by fat32, which manages its own boot
// sector format, to reuse the root-directory update logic.
func (fs *FileSystem) SetRootDirLabel(volumeLabel string) error {
	volumeLabel = fmt.Sprintf("%-11.11s", volumeLabel)

	rootDir, dirEntries, err := fs.readDirWithMkdir("/", false)
	if err != nil {
		return fmt.Errorf("failed to locate root directory: %w", err)
	}
	var labelEntry *directoryEntry
	for _, entry := range dirEntries {
		if entry.isVolumeLabel {
			labelEntry = entry
		}
	}
	if labelEntry != nil {
		labelEntry.filenameShort = volumeLabel[:8]
		labelEntry.fileExtension = volumeLabel[8:11]
	} else {
		if _, err = fs.mkLabel(rootDir, volumeLabel); err != nil {
			return fmt.Errorf("failed to create volume label entry '%s': %w", volumeLabel, err)
		}
	}
	return fs.writeDirectoryEntries(rootDir)
}

// ── internal cluster / directory helpers ──────────────────────────────────────

func (fs *FileSystem) getClusterList(firstCluster uint32) ([]uint32, error) {
	if firstCluster > fs.table.MaxCluster() || fs.table.ClusterValue(firstCluster) == 0 {
		return nil, fmt.Errorf("invalid start cluster: %d", firstCluster)
	}
	clusterList := make([]uint32, 0, 5)
	cluster := firstCluster
	for {
		clusterList = append(clusterList, cluster)
		next := fs.table.ClusterValue(cluster)
		if fs.table.IsEOC(next) {
			break
		}
		if next > fs.table.MaxCluster() {
			return nil, fmt.Errorf("invalid cluster chain at %d", next)
		}
		if cluster < 2 {
			return nil, fmt.Errorf("invalid cluster chain at %d", cluster)
		}
		cluster = next
	}
	return clusterList, nil
}

// isRootCluster reports whether the given cluster location refers to the root directory.
// For FAT12/16, the root directory is stored in a fixed region and logically addressed as cluster 0.
func (fs *FileSystem) isRootCluster(clusterLocation uint32) bool {
	return fs.rootDirMaxEntries > 0 && clusterLocation == 0
}

func (fs *FileSystem) readDirectory(dir *Directory) ([]*directoryEntry, error) {
	// Fixed root directory for FAT12/16.
	if fs.isRootCluster(dir.clusterLocation) {
		size := fs.rootDirMaxEntries * 32
		b := make([]byte, size)
		if _, err := fs.backend.ReadAt(b, fs.rootDirOffset+fs.start); err != nil {
			return nil, fmt.Errorf("could not read root directory: %w", err)
		}
		if err := dir.entriesFromBytes(b); err != nil {
			return nil, err
		}
		return dir.entries, nil
	}
	// Cluster-chain directory.
	clusterList, err := fs.getClusterList(dir.clusterLocation)
	if err != nil {
		return nil, fmt.Errorf("could not read cluster list: %w", err)
	}
	byteCount := len(clusterList) * fs.bytesPerCluster
	b := make([]byte, 0, byteCount)
	for _, cluster := range clusterList {
		clusterStart := fs.start + int64(fs.dataStart) + int64(cluster-2)*int64(fs.bytesPerCluster)
		tmp := make([]byte, fs.bytesPerCluster)
		_, _ = fs.backend.ReadAt(tmp, clusterStart)
		b = append(b, tmp...)
	}
	if err := dir.entriesFromBytes(b); err != nil {
		return nil, err
	}
	return dir.entries, nil
}

func (fs *FileSystem) writeDirectoryEntries(dir *Directory) error {
	// Fixed root directory for FAT12/16.
	if fs.isRootCluster(dir.clusterLocation) {
		b, err := dir.entriesToBytesFixed(fs.rootDirMaxEntries * 32)
		if err != nil {
			return fmt.Errorf("could not serialise root directory entries: %w", err)
		}
		if len(b) > fs.rootDirMaxEntries*32 {
			return fmt.Errorf("root directory full: max %d entries for this FAT12/16 volume", fs.rootDirMaxEntries)
		}
		writableFile, err := fs.backend.Writable()
		if err != nil {
			return err
		}
		if _, err := writableFile.WriteAt(b, fs.rootDirOffset+fs.start); err != nil {
			return fmt.Errorf("error writing root directory: %w", err)
		}
		return nil
	}

	// Cluster-chain directory (all subdirectories for FAT12/16, and root for FAT32-style).
	b, err := dir.entriesToBytes(fs.bytesPerCluster)
	if err != nil {
		return fmt.Errorf("could not create byte stream for directory entries: %w", err)
	}
	writableFile, err := fs.backend.Writable()
	if err != nil {
		return err
	}
	clusterList, err := fs.getClusterList(dir.clusterLocation)
	if err != nil {
		return fmt.Errorf("unable to get clusters for directory: %w", err)
	}
	if len(b) > len(clusterList)*fs.bytesPerCluster {
		clusters, err := fs.allocateSpace(uint64(len(b)), clusterList[0])
		if err != nil {
			return fmt.Errorf("unable to allocate space for directory entries: %w", err)
		}
		clusterList = clusters
	}
	for i, cluster := range clusterList {
		clusterStart := fs.start + int64(fs.dataStart) + int64(cluster-2)*int64(fs.bytesPerCluster)
		bStart := i * fs.bytesPerCluster
		written, err := writableFile.WriteAt(b[bStart:bStart+fs.bytesPerCluster], clusterStart)
		if err != nil {
			return fmt.Errorf("error writing directory entries: %w", err)
		}
		if written != fs.bytesPerCluster {
			return fmt.Errorf("wrote %d bytes to cluster %d instead of expected %d", written, cluster, fs.bytesPerCluster)
		}
	}
	return nil
}

func (fs *FileSystem) mkSubdir(parent *Directory, name string) (*directoryEntry, error) {
	clusters, err := fs.allocateSpace(1, 0)
	if err != nil {
		return nil, fmt.Errorf("could not allocate disk space for dir %s: %w", name, err)
	}
	return parent.createEntry(name, clusters[0], true)
}

func (fs *FileSystem) mkFile(parent *Directory, name string) (*directoryEntry, error) {
	clusters, err := fs.allocateSpace(1, 0)
	if err != nil {
		return nil, fmt.Errorf("could not allocate disk space for file %s: %w", name, err)
	}
	return parent.createEntry(name, clusters[0], false)
}

func (fs *FileSystem) mkLabel(parent *Directory, name string) (*directoryEntry, error) {
	return parent.createVolumeLabel(name)
}

func (fs *FileSystem) readDirWithMkdir(p string, doMake bool) (*Directory, []*directoryEntry, error) {
	paths := splitPath(p)
	// Root directory: clusterLocation = 0 for FAT12/16 (fixed region).
	rootCluster := uint32(0)
	if fs.rootDirMaxEntries == 0 {
		// FAT32-style: use cluster 2.
		rootCluster = fs.table.RootDirCluster()
	}
	currentDir := &Directory{
		directoryEntry: directoryEntry{
			clusterLocation: rootCluster,
			isSubdirectory:  true,
			filesystem:      fs,
		},
	}
	entries, err := fs.readDirectory(currentDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read root directory: %w", err)
	}
	if p == "." {
		return currentDir, entries, nil
	}
	for i, subp := range paths {
		found := false
		for _, e := range entries {
			if e.isVolumeLabel {
				continue
			}
			if !e.nameMatches(subp) {
				continue
			}
			if !e.isSubdirectory {
				return nil, nil, fmt.Errorf("cannot create directory at %s since it is a file",
					"/"+strings.Join(paths[0:i+1], "/"))
			}
			found = true
			currentDir = &Directory{directoryEntry: *e}
			break
		}
		if !found {
			if doMake {
				subdirEntry, err := fs.mkSubdir(currentDir, subp)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to create subdirectory %s: %w",
						"/"+strings.Join(paths[0:i+1], "/"), err)
				}
				currentDir.modifyTime = subdirEntry.createTime
				parentCluster := currentDir.clusterLocation
				dir := &Directory{
					directoryEntry: directoryEntry{clusterLocation: subdirEntry.clusterLocation},
					entries: []*directoryEntry{
						{filenameShort: ".", isSubdirectory: true, clusterLocation: subdirEntry.clusterLocation,
							createTime: subdirEntry.createTime, modifyTime: subdirEntry.modifyTime, accessTime: subdirEntry.accessTime},
						{filenameShort: "..", isSubdirectory: true, clusterLocation: parentCluster,
							createTime: currentDir.createTime, modifyTime: currentDir.modifyTime, accessTime: currentDir.accessTime},
					},
				}
				if err = fs.writeDirectoryEntries(dir); err != nil {
					return nil, nil, fmt.Errorf("error writing new directory entries: %w", err)
				}
				if err = fs.writeDirectoryEntries(currentDir); err != nil {
					return nil, nil, fmt.Errorf("error writing directory entries: %w", err)
				}
				currentDir = &Directory{directoryEntry: *subdirEntry}
			} else {
				return nil, nil, fmt.Errorf("path %s not found", "/"+strings.Join(paths[0:i+1], "/"))
			}
		}
		entries, err = fs.readDirectory(currentDir)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read directory %s: %w",
				"/"+strings.Join(paths[0:i+1], "/"), err)
		}
	}
	return currentDir, entries, nil
}

func (fs *FileSystem) allocateSpace(size uint64, previous uint32) ([]uint32, error) {
	if previous > fs.table.MaxCluster() {
		return nil, fmt.Errorf("invalid cluster chain at %d", previous)
	}
	count := int(size / uint64(fs.bytesPerCluster))
	if size%uint64(fs.bytesPerCluster) > 0 {
		count++
	}
	extraCount := count
	clusters := make([]uint32, 0, 20)
	allocated := make([]uint32, 0, 20)

	if previous >= 2 {
		var err error
		clusters, err = fs.getClusterList(previous)
		if err != nil {
			return nil, fmt.Errorf("unable to get cluster list: %w", err)
		}
		extraCount = count - len(clusters)
		previous = clusters[len(clusters)-1]
	}
	if extraCount == 0 {
		return clusters, nil
	}

	maxCluster := fs.table.MaxCluster()
	if extraCount > 0 {
		for i := uint32(2); i < maxCluster && len(allocated) < extraCount; i++ {
			if fs.table.ClusterValue(i) == 0 {
				allocated = append(allocated, i)
			}
		}
		if len(allocated) < extraCount {
			return nil, errors.New("no space left on device")
		}
		lastAlloc := len(allocated) - 1
		if previous > 0 {
			fs.table.SetCluster(previous, allocated[0])
		}
		for i := 0; i < lastAlloc; i++ {
			fs.table.SetCluster(allocated[i], allocated[i+1])
		}
		fs.table.SetCluster(allocated[lastAlloc], fs.table.EOCMarker())
	} else {
		toRemove := -extraCount
		lastAlloc := len(clusters) - toRemove - 1
		if lastAlloc < 0 {
			lastAlloc = 0
		}
		deallocated := clusters[lastAlloc+1:]
		if uint32(lastAlloc) > maxCluster || clusters[lastAlloc] > maxCluster {
			return nil, fmt.Errorf("invalid cluster chain at %d", lastAlloc)
		}
		fs.table.SetCluster(clusters[lastAlloc], fs.table.EOCMarker())
		for _, cl := range deallocated {
			if cl > maxCluster {
				return nil, fmt.Errorf("invalid cluster chain at %d", cl)
			}
			fs.table.SetCluster(cl, fs.table.UnusedMarker())
		}
	}

	if err := fs.WriteFat(); err != nil {
		return nil, fmt.Errorf("failed to write FAT: %w", err)
	}
	return append(clusters, allocated...), nil
}

func validatePath(name string) error {
	if !iofs.ValidPath(name) {
		return iofs.ErrInvalid
	}
	return nil
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
