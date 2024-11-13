package part

import (
	"io"

	"github.com/diskfs/go-diskfs/storage"
)

// Partition reference to an individual partition on disk
type Partition interface {
	GetSize() int64
	GetStart() int64
	ReadContents(storage.File, io.Writer) (int64, error)
	WriteContents(storage.WritableFile, io.Reader) (uint64, error)
	UUID() string
}
