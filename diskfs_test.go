package diskfs_test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
)

const oneMB = 10 * 1024 * 1024

func tmpDisk(source string) (*os.File, error) {
	filename := "disk_test"
	f, err := ioutil.TempFile("", filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to create tempfile %s :%v", filename, err)
	}

	// either copy the contents of the source file over, or make a file of appropriate size
	if source == "" {
		// make it a 10MB file
		_ = f.Truncate(10 * 1024 * 1024)
	} else {
		b, err := os.ReadFile(source)
		if err != nil {
			return nil, fmt.Errorf("Failed to read contents of %s: %v", source, err)
		}
		written, err := f.Write(b)
		if err != nil {
			return nil, fmt.Errorf("Failed to write contents of %s to %s: %v", source, filename, err)
		}
		if written != len(b) {
			return nil, fmt.Errorf("wrote only %d bytes of %s to %s instead of %d", written, source, filename, len(b))
		}
	}

	return f, nil
}

//nolint:thelper // this is not a helper function
func checkDiskfsErrs(t *testing.T, msg string, err, tterr error, d, ttd *disk.Disk) {
	switch {
	case (err == nil && tterr != nil) || (err != nil && tterr == nil) || (err != nil && tterr != nil && !strings.HasPrefix(err.Error(), tterr.Error())):
		t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tterr)
	case (d == nil && ttd != nil) || (d != nil && ttd == nil):
		t.Errorf("%s: mismatched disk, actual %v expected %v", msg, d, ttd)
	case d != nil && (d.LogicalBlocksize != ttd.LogicalBlocksize || d.PhysicalBlocksize != ttd.PhysicalBlocksize || d.Size != ttd.Size || d.Type != ttd.Type):
		t.Errorf("%s: mismatched disk, actual then expected", msg)
		t.Logf("%v", d)
		t.Logf("%v", ttd)
	}
}

func TestOpen(t *testing.T) {
	f, err := tmpDisk("./partition/mbr/testdata/mbr.img")
	if err != nil {
		t.Fatalf("error creating new temporary disk: %v", err)
	}
	defer f.Close()
	path := f.Name()
	defer os.Remove(path)
	fileInfo, err := f.Stat()
	if err != nil {
		t.Fatalf("unable to stat temporary file %s: %v", path, err)
	}
	size := fileInfo.Size()

	tests := []struct {
		path string
		disk *disk.Disk
		err  error
	}{
		{"", nil, fmt.Errorf("must pass device name")},
		{"/tmp/foo/bar/232323/23/2322/disk.img", nil, fmt.Errorf("")},
		{path, &disk.Disk{Type: disk.File, LogicalBlocksize: 512, PhysicalBlocksize: 512, Size: size}, nil},
	}

	// open default
	for i, tt := range tests {
		d, err := diskfs.Open(tt.path)
		msg := fmt.Sprintf("%d: Open(%s)", i, tt.path)
		checkDiskfsErrs(t, msg, err, tt.err, d, tt.disk)
	}

	// open WithOpenMode
	for i, tt := range tests {
		d, err := diskfs.Open(tt.path, diskfs.WithOpenMode(diskfs.ReadOnly))
		msg := fmt.Sprintf("%d: Open WithOpenMode(%s, diskfs.ReadOnly)", i, tt.path)
		checkDiskfsErrs(t, msg, err, tt.err, d, tt.disk)
	}
}

func TestCreate(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		size       int64
		format     diskfs.Format
		sectorSize diskfs.SectorSize
		disk       *disk.Disk
		err        error
	}{
		{"no file", "", 10 * oneMB, diskfs.Raw, diskfs.SectorSizeDefault, nil, fmt.Errorf("must pass device name")},
		{"zero size", "disk", 0, diskfs.Raw, diskfs.SectorSizeDefault, nil, fmt.Errorf("must pass valid device size to create")},
		{"negative size", "disk", -1, diskfs.Raw, diskfs.SectorSizeDefault, nil, fmt.Errorf("must pass valid device size to create")},
		{"directory does not exist", "foo/bar/232323/23/2322/disk", 10 * oneMB, diskfs.Raw, diskfs.SectorSizeDefault, nil, fmt.Errorf("could not create device")},
		{"10MB with default sector size", "disk", 10 * oneMB, diskfs.Raw, diskfs.SectorSizeDefault, &disk.Disk{LogicalBlocksize: 512, PhysicalBlocksize: 512, Size: 10 * oneMB, Type: disk.File}, nil},
		{"10MB with 512 sector size", "disk", 10 * oneMB, diskfs.Raw, diskfs.SectorSize512, &disk.Disk{LogicalBlocksize: 512, PhysicalBlocksize: 512, Size: 10 * oneMB, Type: disk.File}, nil},
		{"10MB with 2048 sector size", "disk", 10 * oneMB, diskfs.Raw, diskfs.SectorSize4k, &disk.Disk{LogicalBlocksize: 4096, PhysicalBlocksize: 4096, Size: 10 * oneMB, Type: disk.File}, nil},
	}

	for i, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var filename string
			if tt.path != "" {
				filename = testTmpFilename(t, "diskfs_test"+tt.path, ".img")
			}
			d, err := diskfs.Create(filename, tt.size, tt.format, tt.sectorSize)
			defer os.RemoveAll(filename)
			msg := fmt.Sprintf("%d: Create(%s, %d, %v, %d)", i, filename, tt.size, tt.format, tt.sectorSize)
			checkDiskfsErrs(t, msg, err, tt.err, d, tt.disk)
		})
	}
}

func testTmpFilename(t *testing.T, prefix, suffix string) string {
	t.Helper()
	randBytes := make([]byte, 16)
	_, _ = rand.Read(randBytes)
	return filepath.Join(os.TempDir(), prefix+hex.EncodeToString(randBytes)+suffix)
}
