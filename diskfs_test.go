package diskfs_test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
)

const oneMB = 10 * 1024 * 1024

func tmpDisk(source string, padding int64) (*os.File, error) {
	filename := "disk_test"
	f, err := os.CreateTemp("", filename)
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
		if padding != 0 {
			data := make([]byte, padding) // Initialize an empty byte slice
			writtenPadding, err := f.Write(data)
			written += writtenPadding
			if err != nil {
				return nil, fmt.Errorf("Failed to write contents of %s to %s: %v", source, filename, err)
			}
			if written != len(b)+len(data) {
				return nil, fmt.Errorf("Wrote only %d bytes of %s to %s instead of %d", written, source, filename, len(b))
			}
		} else if written != len(b) {
			return nil, fmt.Errorf("Wrote only %d bytes of %s to %s instead of %d", written, source, filename, len(b))
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

func TestGPTOpen(t *testing.T) {
	f, err := tmpDisk("./partition/gpt/testdata/gpt.img", 0)
	if err != nil {
		t.Fatalf("Error creating new temporary disk: %v", err)
	}
	defer f.Close()
	path := f.Name()
	defer os.Remove(path)
	fileInfo, err := f.Stat()
	if err != nil {
		t.Fatalf("Unable to stat temporary file %s: %v", path, err)
	}
	size := fileInfo.Size()

	// Create a padded file, where "disk" has additional space after what the GPT table
	// thinks should be the final sector
	fPadded, err := tmpDisk("./partition/gpt/testdata/gpt.img", 1024*1024)
	if err != nil {
		t.Fatalf("Error creating new temporary disk: %v", err)
	}
	defer fPadded.Close()
	filePadded := fPadded.Name()
	defer os.Remove(path)
	filePaddedInfo, err := fPadded.Stat()
	if err != nil {
		t.Fatalf("Unable to stat temporary file %s: %v", path, err)
	}
	filePaddedSize := filePaddedInfo.Size()

	tests := []struct {
		path string
		disk *disk.Disk
		err  error
	}{
		{"", nil, fmt.Errorf("must pass device name")},
		{"/tmp/foo/bar/232323/23/2322/disk.img", nil, fmt.Errorf("")},
		{path, &disk.Disk{Type: disk.File, LogicalBlocksize: 512, PhysicalBlocksize: 512, Size: size}, nil},
		{filePadded, &disk.Disk{Type: disk.File, LogicalBlocksize: 512, PhysicalBlocksize: 512, Size: filePaddedSize}, nil},
	}

	for _, tt := range tests {
		d, err := diskfs.Open(tt.path)
		msg := fmt.Sprintf("Open(%s)", tt.path)
		checkDiskfsErrs(t, msg, err, tt.err, d, tt.disk)
		if d != nil {
			table, err := d.GetPartitionTable()
			if err != nil {
				t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
			}

			// Verify will compare the GPT table to the disk and attempt to read the secondary header if possible
			err = table.Verify(d.File, uint64(tt.disk.Size))
			if err != nil {
				// We log this as it's epected to be an error
				t.Logf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
			}

			// Will correct the internal structures of the primary GPT table
			err = table.Repair(uint64(tt.disk.Size))
			if err != nil {
				t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
			}

			// Update both tables on disk
			err = table.Write(d.File, tt.disk.Size)
			if err != nil {
				t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
			}

			// Check that things are as expected.
			err = table.Verify(d.File, uint64(tt.disk.Size))
			if err != nil {
				t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
			}
		}
	}

	for i, tt := range tests {
		d, err := diskfs.Open(tt.path, diskfs.WithOpenMode(diskfs.ReadOnly))
		msg := fmt.Sprintf("%d: Open(%s)", i, tt.path)
		checkDiskfsErrs(t, msg, err, tt.err, d, tt.disk)
	}
}

func TestMBROpen(t *testing.T) {
	f, err := tmpDisk("./partition/mbr/testdata/mbr.img", 1024*1024)
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
