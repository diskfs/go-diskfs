package part

import (
	"io"

	"github.com/diskfs/go-diskfs/backend"
)

// Partition reference to an individual partition on disk
type Partition interface {
	GetIndex() int // Index of the partition in the table, starting at 1
	GetSize() int64
	GetStart() int64
	ReadContents(backend.File, io.Writer) (int64, error)
	WriteContents(backend.WritableFile, io.Reader) (uint64, error)
	UUID() string
	Label() string
}
