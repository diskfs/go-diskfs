// Package disk provides utilities for working directly with a disk
//
// Most of the provided functions are intelligent wrappers around implementations of
// github.com/diskfs/go-diskfs/partition and github.com/diskfs/go-diskfs/filesystem
package disk

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat32"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
	"github.com/diskfs/go-diskfs/partition"
)

// Disk is a reference to a single disk block device or image that has been Create() or Open()
type Disk struct {
	File              *os.File
	Info              os.FileInfo
	Type              Type
	Size              int64
	LogicalBlocksize  int64
	PhysicalBlocksize int64
	Table             partition.Table
}

// Type represents the type of disk this is
type Type int

const (
	// File is a file-based disk image
	File Type = iota
	// Device is an OS-managed block device
	Device
)

// GetPartitionTable retrieves a PartitionTable for a Disk
//
// returns an error if the Disk is invalid or does not exist, or the partition table is unknown
func (d *Disk) GetPartitionTable() (partition.Table, error) {
	return partition.Read(d.File, int(d.LogicalBlocksize), int(d.PhysicalBlocksize))
}

// Partition applies a partition.Table implementation to a Disk
//
// The Table can have zero, one or more Partitions, each of which is unique to its
// implementation. E.g. MBR partitions in mbr.Table look different from GPT partitions in gpt.Table
//
// Actual writing of the table is delegated to the individual implementation
func (d *Disk) Partition(table partition.Table) error {
	// fill in the uuid
	err := table.Write(d.File, d.Info.Size())
	if err != nil {
		return fmt.Errorf("Failed to write partition table: %v", err)
	}
	d.Table = table
	return nil
}

// WritePartitionContents writes the contents of an io.Reader to a given partition
//
// if successful, returns the number of bytes written
//
// returns an error if there was an error writing to the disk, reading from the reader, the table
// is invalid, or the partition is invalid
func (d *Disk) WritePartitionContents(partition int, reader io.Reader) (int64, error) {
	if d.Table == nil {
		return -1, fmt.Errorf("cannot write contents of a partition on a disk without a partition table")
	}
	if partition < 0 {
		return -1, fmt.Errorf("cannot write contents of a partition without specifying a partition")
	}
	written, err := d.Table.WritePartitionContents(partition, d.File, reader)
	return int64(written), err
}

// ReadPartitionContents reads the contents of a partition to an io.Writer
//
// if successful, returns the number of bytes read
//
// returns an error if there was an error reading from the disk, writing to the writer, the table
// is invalid, or the partition is invalid
func (d *Disk) ReadPartitionContents(partition int, writer io.Writer) (int64, error) {
	if d.Table == nil {
		return -1, fmt.Errorf("cannot read contents of a partition on a disk without a partition table")
	}
	if partition < 0 {
		return -1, fmt.Errorf("cannot read contents of a partition without specifying a partition")
	}
	return d.Table.ReadPartitionContents(partition, d.File, writer)
}

// CreateFilesystem creates a filesystem on a disk image, the equivalent of mkfs.
//
// pass the desired partition number, or 0 to create the filesystem on the entire block device / disk image,
// as well as the filesystem type from github.com/diskfs/go-diskfs/filesystem
//
// if successful, returns a filesystem-implementing structure for the given filesystem type
//
// returns error if there was an error creating the filesystem, or the partition table is invalid and did not
// request the entire disk.
func (d *Disk) CreateFilesystem(partition int, fstype filesystem.Type) (filesystem.FileSystem, error) {
	return d.CreateFilesystemSpecial(FilesystemSpec{Partition: partition, FSType: fstype})
}

// FilesystemSpec represents the details of a filesystem to be created
type FilesystemSpec struct {
	Partition   int
	FSType      filesystem.Type
	VolumeLabel string
}

// CreateFilesystemSpecial creates a filesystem on a disk image, the equivalent of mkfs.
//
// Required:
// * desired partition number, or 0 to create the filesystem on the entire block device or
//   disk image,
// * the filesystem type from github.com/diskfs/go-diskfs/filesystem
//
// Optional:
// * volume label for those filesystems that support it; under Linux this shows
//   in '/dev/disks/by-label/<label>'
//
// if successful, returns a filesystem-implementing structure for the given filesystem type
//
// returns error if there was an error creating the filesystem, or the partition table is invalid and did not
// request the entire disk.
func (d *Disk) CreateFilesystemSpecial(spec FilesystemSpec) (filesystem.FileSystem, error) {
	// find out where the partition starts and ends, or if it is the entire disk
	var (
		size, start int64
		err         error
	)
	switch {
	case spec.Partition == 0:
		size = d.Size
		start = 0
	case d.Table == nil:
		return nil, fmt.Errorf("cannot create filesystem on a partition without a partition table")
	default:
		size, err = d.Table.GetPartitionSize(spec.Partition)
		if err != nil {
			return nil, fmt.Errorf("error getting size of partition %d: %v", spec.Partition, err)
		}
		start, err = d.Table.GetPartitionStart(spec.Partition)
		if err != nil {
			return nil, fmt.Errorf("error getting start of partition %d: %v", spec.Partition, err)
		}
	}

	switch spec.FSType {
	case filesystem.TypeFat32:
		return fat32.Create(d.File, size, start, d.LogicalBlocksize, spec.VolumeLabel)
	case filesystem.TypeISO9660:
		return iso9660.Create(d.File, size, start, d.LogicalBlocksize)
	default:
		return nil, errors.New("Unknown filesystem type requested")
	}
}

// GetFilesystem gets the filesystem that already exists on a disk image
//
// pass the desired partition number, or 0 to create the filesystem on the entire block device / disk image,
//
// if successful, returns a filesystem-implementing structure for the given filesystem type
//
// returns error if there was an error reading the filesystem, or the partition table is invalid and did not
// request the entire disk.
func (d *Disk) GetFilesystem(partition int) (filesystem.FileSystem, error) {
	// find out where the partition starts and ends, or if it is the entire disk
	var (
		size, start int64
		err         error
	)

	switch {
	case partition == 0:
		size = d.Size
		start = 0
	case d.Table == nil:
		return nil, fmt.Errorf("cannot read filesystem on a partition without a partition table")
	default:
		size, err = d.Table.GetPartitionSize(partition)
		if err != nil {
			return nil, fmt.Errorf("error getting size of partition %d: %v", partition, err)
		}
		start, err = d.Table.GetPartitionStart(partition)
		if err != nil {
			return nil, fmt.Errorf("error getting start of partition %d: %v", partition, err)
		}
	}

	// just try each type
	fat32FS, err := fat32.Read(d.File, size, start, d.LogicalBlocksize)
	if err == nil {
		return fat32FS, nil
	}
	iso9660FS, err := iso9660.Read(d.File, size, start, d.LogicalBlocksize)
	if err == nil {
		return iso9660FS, nil
	}
	return nil, fmt.Errorf("Unknown filesystem on partition %d", partition)
}
