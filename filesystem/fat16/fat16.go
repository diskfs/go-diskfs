package fat16

import (
	"errors"
	"fmt"
	"time"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat12"
)

// FileSystem is a FAT16 filesystem. It embeds *fat12.FileSystem to inherit
// all high-level methods: OpenFile, ReadDir, Mkdir, Remove, Rename, Label,
// SetLabel, allocateSpace, etc. Only Type() is overridden here.
type FileSystem struct {
	*fat12.FileSystem
}

// interface guard
var _ filesystem.FileSystem = (*FileSystem)(nil)

// StatT is an alias for fat12.StatT, the metadata returned by FileInfo.Sys().
type StatT = fat12.StatT

// Type returns filesystem.TypeFat16, overriding fat12.FileSystem.Type().
func (fs *FileSystem) Type() filesystem.Type { return filesystem.TypeFat16 }

// Create creates a FAT16 filesystem on the given backend.
func Create(b backend.Storage, size, start, blocksize int64, volumeLabel string, reproducible bool) (*FileSystem, error) {
	if blocksize != int64(fat12.SectorSize512) && blocksize > 0 {
		return nil, fmt.Errorf("blocksize for FAT16 must be 512 or 0, not %d", blocksize)
	}
	if size > fat12.Fat16MaxSize {
		return nil, fmt.Errorf("size %d exceeds FAT16 maximum of %d", size, fat12.Fat16MaxSize)
	}
	if size < int64(fat12.SectorSize512)*8 {
		return nil, fmt.Errorf("size %d is too small for a FAT16 filesystem", size)
	}

	var volid uint32
	if !reproducible {
		now := time.Now()
		volid = uint32(now.Unix()<<20 | (now.UnixNano() / 1000000))
	}

	bytesPerSector := uint16(fat12.SectorSize512)
	totalSectors := uint32(size / int64(fat12.SectorSize512))

	// Cluster size per Microsoft FAT spec for FAT16.
	// The constraint is: 4085 ≤ dataClusters < 65525.
	// Each FAT16 entry is 2 bytes; with 512-byte sectors:
	//   max usable size at s/c=2 is ~32 MB (≈32 632 clusters)
	//   max usable size at s/c=4 is ~128 MB (≈65 404 clusters)
	//   max usable size at s/c=8 is ~256 MB (≈65 470 clusters)
	//   max usable size at s/c=16 is ~512 MB (≈65 503 clusters)
	//   max usable size at s/c=32 is ~1 GB (≈65 519 clusters)
	//   max usable size at s/c=64 is ~2 GB
	var sectorsPerCluster uint8
	switch {
	case size <= 32*fat12.MB:
		sectorsPerCluster = 2
	case size <= 128*fat12.MB:
		sectorsPerCluster = 4
	case size <= 256*fat12.MB:
		sectorsPerCluster = 8
	case size <= 512*fat12.MB:
		sectorsPerCluster = 16
	case size <= fat12.GB:
		sectorsPerCluster = 32
	default: // up to 2 GB
		sectorsPerCluster = 64
	}

	const rootDirEntries uint16 = 512
	const reservedSectors uint16 = 4
	const fatCount uint8 = 2
	mediaType := uint8(fat12.MediaFixedDisk)

	// sectorsPerFat for FAT16: each entry is 2 bytes.
	rootDirSectors := (uint32(rootDirEntries)*32 + uint32(bytesPerSector) - 1) / uint32(bytesPerSector)
	dataSectors := totalSectors - uint32(reservedSectors) - rootDirSectors
	numClusters := dataSectors / uint32(sectorsPerCluster)
	sectorsPerFat := uint16((numClusters*2 + uint32(bytesPerSector) - 1) / uint32(bytesPerSector))
	// Refine with actual FAT overhead.
	dataSectors = totalSectors - uint32(reservedSectors) - rootDirSectors - uint32(fatCount)*uint32(sectorsPerFat)
	numClusters = dataSectors / uint32(sectorsPerCluster)

	if numClusters >= 65525 {
		return nil, fmt.Errorf("computed cluster count %d too large for FAT16 (max 65524)", numClusters)
	}
	if numClusters < 4085 {
		return nil, fmt.Errorf("computed cluster count %d too small for FAT16 (min 4085); use FAT12", numClusters)
	}

	var ts16 uint16
	var ts32 uint32
	if totalSectors <= 0xFFFF {
		ts16 = uint16(totalSectors)
	} else {
		ts32 = totalSectors
	}

	dos20 := &fat12.Dos20BPB{
		BytesPerSector:       fat12.SectorSize512,
		SectorsPerCluster:    sectorsPerCluster,
		ReservedSectors:      reservedSectors,
		FatCount:             fatCount,
		RootDirectoryEntries: rootDirEntries,
		TotalSectors:         ts16,
		MediaType:            mediaType,
		SectorsPerFat:        sectorsPerFat,
	}
	dos331 := &fat12.Dos331BPB{
		Dos20BPB:        dos20,
		SectorsPerTrack: 63,
		Heads:           255,
		HiddenSectors:   0,
		TotalSectors32:  ts32,
	}
	bpb := &fat12.Dos40EBPB{
		Dos331BPB:          dos331,
		DriveNumber:        0x80,
		ReservedFlags:      0,
		ExtBootSignature:   0x29,
		VolumeSerialNumber: volid,
		VolumeLabel:        "NO NAME    ",
		FileSystemType:     "FAT16   ",
	}

	fatPrimaryStart := uint32(reservedSectors) * uint32(bytesPerSector)
	fatSize := uint32(sectorsPerFat) * uint32(bytesPerSector)
	fatSecondaryStart := fatPrimaryStart + fatSize
	rootDirOff := fatSecondaryStart + fatSize
	dataStart := rootDirOff + rootDirSectors*uint32(bytesPerSector)
	bytesPerCluster := int(sectorsPerCluster) * int(bytesPerSector)

	fatID := uint32(0xFF00) | uint32(mediaType)
	tbl := newFat16Table(fatID, fatSize)
	// Mark root cluster (cluster 2) as EOC.
	tbl.SetCluster(2, tbl.EOCMarker())

	base := fat12.NewFileSystem(b, bpb, tbl,
		dataStart, bytesPerCluster, size, start,
		int64(rootDirOff), int(rootDirEntries),
		uint64(fatPrimaryStart), uint64(fatSecondaryStart))

	fs := &FileSystem{FileSystem: base}

	// Write the boot sector and FAT via the exported helpers on fat12.FileSystem.
	if err := fs.WriteBootSector(); err != nil {
		return nil, fmt.Errorf("failed to write boot sector: %w", err)
	}
	if err := fs.WriteFat(); err != nil {
		return nil, fmt.Errorf("failed to write FAT: %w", err)
	}

	// Zero the root directory region.
	writableFile, err := b.Writable()
	if err != nil {
		return nil, err
	}
	zeros := make([]byte, int(rootDirEntries)*32)
	if _, err := writableFile.WriteAt(zeros, int64(rootDirOff)+start); err != nil {
		return nil, fmt.Errorf("failed to zero root directory: %w", err)
	}

	if err := fs.SetLabel(volumeLabel); err != nil {
		return nil, fmt.Errorf("failed to set volume label: %w", err)
	}
	return fs, nil
}

// Read reads a FAT16 filesystem from the backend.
// Returns an error if the image is not a valid FAT16 filesystem so that
// disk.GetFilesystem() can fall through to the next candidate.
func Read(b backend.Storage, size, start, blocksize int64) (*FileSystem, error) {
	if blocksize != 0 && blocksize != int64(fat12.SectorSize512) {
		return nil, fmt.Errorf("blocksize for FAT16 must be 0 or 512, not %d", blocksize)
	}

	raw := make([]byte, fat12.SectorSize512)
	if _, err := b.ReadAt(raw, start); err != nil {
		return nil, fmt.Errorf("could not read boot sector: %w", err)
	}

	bpb, err := fat12.Dos40EBPBFromBytesOnly(raw[11:])
	if err != nil {
		return nil, fmt.Errorf("not a FAT16 filesystem (BPB parse failed): %w", err)
	}

	dos20 := bpb.Dos331BPB.Dos20BPB
	if dos20.RootDirectoryEntries == 0 {
		return nil, errors.New("not a FAT16 filesystem: rootDirectoryEntries is zero (FAT32?)")
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

	if numClusters < 4085 {
		return nil, fmt.Errorf("not a FAT16 filesystem: cluster count %d < 4085", numClusters)
	}
	if numClusters >= 65525 {
		return nil, fmt.Errorf("not a FAT16 filesystem: cluster count %d >= 65525", numClusters)
	}

	fatPrimaryStart := reservedSectors * bytesPerSector
	fatSize := sectorsPerFat * bytesPerSector
	fatSecondaryStart := fatPrimaryStart + fatSize
	rootDirOff := fatSecondaryStart + fatSize
	dataStart := rootDirOff + rootDirSectors*bytesPerSector
	bytesPerCluster := int(sectorsPerCluster) * int(bytesPerSector)

	fatID := uint32(0xFF00) | uint32(dos20.MediaType)
	tbl := newFat16Table(fatID, fatSize)
	fatBytes := make([]byte, fatSize)
	if _, err := b.ReadAt(fatBytes, int64(fatPrimaryStart)+start); err != nil {
		return nil, fmt.Errorf("could not read FAT: %w", err)
	}
	tbl.FromBytes(fatBytes)

	base := fat12.NewFileSystem(b, bpb, tbl,
		dataStart, bytesPerCluster, size, start,
		int64(rootDirOff), int(rootDirEntries),
		uint64(fatPrimaryStart), uint64(fatSecondaryStart))
	return &FileSystem{FileSystem: base}, nil
}
