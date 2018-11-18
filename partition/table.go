package partition

import (
	"io"

	"github.com/diskfs/go-diskfs/util"
)

// Table reference to a partitioning table on disk
type Table interface {
	Type() string
	Write(util.File, int64) error
	GetPartitionSize(int) (int64, error)
	GetPartitionStart(int) (int64, error)
	ReadPartitionContents(int, util.File, io.Writer) (int64, error)
	WritePartitionContents(int, util.File, io.Reader) (uint64, error)
}
