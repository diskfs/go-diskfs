package squashfs

import (
	"reflect"
	"testing"
	"time"
)

func TestDirectoryEntry(t *testing.T) {
	de := &directoryEntry{
		isSubdirectory: true,
		name:           "abc",
		size:           8675309,
		modTime:        time.Now(),
		mode:           0o766,
		uid:            32,
		gid:            33,
		xattrs:         map[string]string{"test": "value"},
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
	st, ok := de.Sys().(*StatT)
	if !ok {
		t.Fatalf("Sys() did not return *StatT")
	}
	if st.UID != de.uid {
		t.Errorf("Mismatched UID, actual %d expected %d", st.UID, de.uid)
	}
	if st.GID != de.gid {
		t.Errorf("Mismatched GID, actual %d expected %d", st.GID, de.gid)
	}
	if !reflect.DeepEqual(st.Xattrs, de.xattrs) {
		t.Errorf("Mismatched Xattrs, actual %+v expected %+v", st.Xattrs, de.xattrs)
	}
	if st.InodeType != "unknown" {
		t.Errorf("Expected InodeType %q for nil inode, got %q", "unknown", st.InodeType)
	}
}
