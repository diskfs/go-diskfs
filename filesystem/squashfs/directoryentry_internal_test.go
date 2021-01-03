package squashfs

import (
	"testing"
	"time"
)

func TestDirectoryEntry(t *testing.T) {
	de := &directoryEntry{
		isSubdirectory: true,
		name:           "abc",
		size:           8675309,
		modTime:        time.Now(),
		mode:           0766,
	}
	switch {
	case de.Name() != de.name:
		t.Errorf("Mismatched Name(), actual '%s' expected '%s'", de.Name(), de.name)
	case de.Size() != de.size:
		t.Errorf("Mismatched Size(), actual %d expected %d", de.Size(), de.size)
	case de.IsDir() != de.isSubdirectory:
		t.Errorf("Mismatched IsDir(), actual %v expected %v", de.IsDir(), de.isSubdirectory)
	case de.ModTime() != de.modTime:
		t.Errorf("Mismatched ModTime(), actual %v expected %v", de.ModTime(), de.modTime)
	case de.Mode() != de.mode:
		t.Errorf("Mismatched Mode(), actual %v expected %v", de.Mode(), de.mode)
	case de.Sys() == nil:
		t.Errorf("Mismatched Sys(), unexpected nil")
	}
	// check that Sys() is convertible
	if _, ok := de.Sys().(FileStat); !ok {
		t.Errorf("Mismatched Sys(), could not convert to FileStat")
	}
}

func TestFileStatUID(t *testing.T) {
	fs := FileStat{uid: 32}
	uid := fs.UID()
	if uid != fs.uid {
		t.Errorf("Mismatched UID, actual %d expected %d", uid, fs.uid)
	}
}

func TestFileStatGID(t *testing.T) {
	fs := FileStat{gid: 32}
	gid := fs.GID()
	if gid != fs.gid {
		t.Errorf("Mismatched GID, actual %d expected %d", gid, fs.gid)
	}
}
