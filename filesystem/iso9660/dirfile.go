package iso9660

import (
	"io"
	iofs "io/fs"
)

// dirFile represents an opened directory in an iso9660 filesystem.
// It implements fs.File and fs.ReadDirFile, allowing directory listing
// via Open() as required by the fs.FS contract.
type dirFile struct {
	entry   *directoryEntry
	fs      *FileSystem
	path    string          // fs.FS-style path (e.g., "." or "boot/grub")
	entries []iofs.DirEntry // lazily loaded
	dirPos  int             // cursor for sequential ReadDir(n) calls
	closed  bool
}

var _ iofs.File = (*dirFile)(nil)
var _ iofs.ReadDirFile = (*dirFile)(nil)

// Read on a directory always returns fs.ErrInvalid.
func (df *dirFile) Read([]byte) (int, error) {
	return 0, iofs.ErrInvalid
}

// Close marks the directory handle as closed.
func (df *dirFile) Close() error {
	df.closed = true
	return nil
}

// Stat returns the FileInfo for this directory.
func (df *dirFile) Stat() (iofs.FileInfo, error) {
	if df.path == "." {
		return rootDirInfo{df.entry}, nil
	}
	return df.entry, nil
}

// ReadDir reads the contents of the directory.
// If n > 0, it returns at most n entries and advances the cursor.
// If n <= 0, it returns all remaining entries.
func (df *dirFile) ReadDir(n int) ([]iofs.DirEntry, error) {
	// Lazy-load entries on first call
	if df.entries == nil {
		entries, err := df.fs.ReadDir(df.path)
		if err != nil {
			return nil, err
		}
		df.entries = entries
	}

	// All remaining entries
	if n <= 0 {
		if df.dirPos >= len(df.entries) {
			return nil, nil
		}
		rest := df.entries[df.dirPos:]
		df.dirPos = len(df.entries)
		return rest, nil
	}

	// Up to n entries
	if df.dirPos >= len(df.entries) {
		return nil, io.EOF
	}
	end := df.dirPos + n
	if end > len(df.entries) {
		end = len(df.entries)
	}
	result := df.entries[df.dirPos:end]
	df.dirPos = end

	if df.dirPos >= len(df.entries) {
		return result, io.EOF
	}
	return result, nil
}

// rootDirInfo wraps a directoryEntry to override Name() for the root directory.
// The root directoryEntry has filename="" but fs.FileInfo requires Name() == ".".
type rootDirInfo struct {
	iofs.FileInfo
}

func (ri rootDirInfo) Name() string {
	return "."
}
