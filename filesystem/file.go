package filesystem

import (
	"io"
	"io/fs"
)

// File a reference to a single file on disk
type File interface {
	fs.ReadDirFile
	io.Writer
	io.Seeker
	// io.ReaderAt
	// io.WriterAt
}
