package erofs

import (
	"io/fs"
	"time"
)

// StatT is the EROFS-specific metadata returned by FileInfo.Sys().
type StatT struct {
	UID       uint32
	GID       uint32
	Inode     uint64
	LinkCount uint32
	DeviceID  uint32
}

type fileInfo struct {
	name string
	node *inode
}

var _ fs.FileInfo = (*fileInfo)(nil)

func (fi *fileInfo) Name() string { return fi.name }
func (fi *fileInfo) Size() int64  { return fi.node.size }
func (fi *fileInfo) Mode() fs.FileMode {
	mode := goFileMode(fi.node.mode)
	return mode
}
func (fi *fileInfo) ModTime() time.Time { return fi.node.mtime }
func (fi *fileInfo) IsDir() bool        { return fi.node.isDir() }
func (fi *fileInfo) Sys() any {
	return &StatT{
		UID:       fi.node.uid,
		GID:       fi.node.gid,
		Inode:     fi.node.nid,
		LinkCount: fi.node.nlink,
		DeviceID:  fi.node.rdev,
	}
}

func (fi *fileInfo) Equal(other fs.FileInfo) bool {
	if fi.Name() != other.Name() || fi.Size() != other.Size() || fi.Mode() != other.Mode() ||
		!fi.ModTime().Equal(other.ModTime()) || fi.IsDir() != other.IsDir() {
		return false
	}
	var stat *StatT
	switch sys := other.Sys().(type) {
	case *StatT:
		stat = sys
	default:
		return false
	}
	return fi.node.uid == stat.UID && fi.node.gid == stat.GID && fi.node.nid == stat.Inode
}
