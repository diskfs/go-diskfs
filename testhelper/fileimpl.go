package testhelper

type reader func(b []byte, offset int64) (int, error)
type writer func(b []byte, offset int64) (int, error)

// FileImpl implement github.com/deitch/diskfs/util/File
// used for testing to enable stubbing out files
type FileImpl struct {
	Reader reader
	Writer writer
}

// ReadAt read at a particular offset
func (f *FileImpl) ReadAt(b []byte, offset int64) (int, error) {
	return f.Reader(b, offset)
}

// WriteAt write at a particular offset
func (f *FileImpl) WriteAt(b []byte, offset int64) (int, error) {
	return f.Writer(b, offset)
}
