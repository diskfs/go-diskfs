package partition

import (
	"github.com/diskfs/go-diskfs/partition/part"
	"github.com/diskfs/go-diskfs/storage"
)

// Table reference to a partitioning table on disk
type Table interface {
	Type() string
	Write(storage.WritableFile, int64) error
	GetPartitions() []part.Partition
	Repair(diskSize uint64) error
	Verify(f storage.File, diskSize uint64) error
	UUID() string
}
