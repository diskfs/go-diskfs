package diskfs_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
)

const oneMB = 10 * 1024 * 1024

func tmpDisk(source string, padding int64) (*os.File, error) {
	filename := "disk_test"
	f, err := ioutil.TempFile("", filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to create tempfile %s :%v", filename, err)
	}

	// either copy the contents of the source file over, or make a file of appropriate size
	if source == "" {
		// make it a 10MB file
		f.Truncate(10 * 1024 * 1024)
	} else {
		b, err := ioutil.ReadFile(source)
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
		} else {
			if written != len(b) {
				return nil, fmt.Errorf("Wrote only %d bytes of %s to %s instead of %d", written, source, filename, len(b))
			}
		}
	}

	return f, nil
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
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
		case (d == nil && tt.disk != nil) || (d != nil && tt.disk == nil):
			t.Errorf("%s: mismatched disk, actual %v expected %v", msg, d, tt.disk)
		case d != nil && (d.LogicalBlocksize != tt.disk.LogicalBlocksize || d.PhysicalBlocksize != tt.disk.PhysicalBlocksize || d.Size != tt.disk.Size || d.Type != tt.disk.Type):
			t.Errorf("%s: mismatched disk, actual then expected", msg)
			t.Logf("%v", d)
			t.Logf("%v", tt.disk)
		}
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
		d, err := diskfs.OpenWithMode(tt.path, diskfs.ReadOnly)
		msg := fmt.Sprintf("%d: Open(%s)", i, tt.path)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
		case (d == nil && tt.disk != nil) || (d != nil && tt.disk == nil):
			t.Errorf("%s: mismatched disk, actual %v expected %v", msg, d, tt.disk)
		case d != nil && (d.LogicalBlocksize != tt.disk.LogicalBlocksize || d.PhysicalBlocksize != tt.disk.PhysicalBlocksize || d.Size != tt.disk.Size || d.Type != tt.disk.Type):
			t.Errorf("%s: mismatched disk, actual then expected", msg)
			t.Logf("%v", d)
			t.Logf("%v", tt.disk)
		}
	}
}

func TestMBROpen(t *testing.T) {
	f, err := tmpDisk("./partition/mbr/testdata/mbr.img", 1024*1024)
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

	tests := []struct {
		path string
		disk *disk.Disk
		err  error
	}{
		{"", nil, fmt.Errorf("must pass device name")},
		{"/tmp/foo/bar/232323/23/2322/disk.img", nil, fmt.Errorf("")},
		{path, &disk.Disk{Type: disk.File, LogicalBlocksize: 512, PhysicalBlocksize: 512, Size: size}, nil},
	}

	for _, tt := range tests {
		d, err := diskfs.Open(tt.path)
		msg := fmt.Sprintf("Open(%s)", tt.path)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
		case (d == nil && tt.disk != nil) || (d != nil && tt.disk == nil):
			t.Errorf("%s: mismatched disk, actual %v expected %v", msg, d, tt.disk)
		case d != nil && (d.LogicalBlocksize != tt.disk.LogicalBlocksize || d.PhysicalBlocksize != tt.disk.PhysicalBlocksize || d.Size != tt.disk.Size || d.Type != tt.disk.Type):
			t.Errorf("%s: mismatched disk, actual then expected", msg)
			t.Logf("%v", d)
			t.Logf("%v", tt.disk)
		}
	}

	for i, tt := range tests {
		d, err := diskfs.OpenWithMode(tt.path, diskfs.ReadOnly)
		msg := fmt.Sprintf("%d: Open(%s)", i, tt.path)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
		case (d == nil && tt.disk != nil) || (d != nil && tt.disk == nil):
			t.Errorf("%s: mismatched disk, actual %v expected %v", msg, d, tt.disk)
		case d != nil && (d.LogicalBlocksize != tt.disk.LogicalBlocksize || d.PhysicalBlocksize != tt.disk.PhysicalBlocksize || d.Size != tt.disk.Size || d.Type != tt.disk.Type):
			t.Errorf("%s: mismatched disk, actual then expected", msg)
			t.Logf("%v", d)
			t.Logf("%v", tt.disk)
		}
	}
}

func TestCreate(t *testing.T) {
	tests := []struct {
		path   string
		size   int64
		format diskfs.Format
		disk   *disk.Disk
		err    error
	}{
		{"", 10 * oneMB, diskfs.Raw, nil, fmt.Errorf("must pass device name")},
		{"/tmp/disk.img", 0, diskfs.Raw, nil, fmt.Errorf("must pass valid device size to create")},
		{"/tmp/disk.img", -1, diskfs.Raw, nil, fmt.Errorf("must pass valid device size to create")},
		{"/tmp/foo/bar/232323/23/2322/disk.img", 10 * oneMB, diskfs.Raw, nil, fmt.Errorf("Could not create device")},
		{"/tmp/disk.img", 10 * oneMB, diskfs.Raw, &disk.Disk{LogicalBlocksize: 512, PhysicalBlocksize: 512, Size: 10 * oneMB, Type: disk.File}, nil},
	}

	for i, tt := range tests {
		disk, err := diskfs.Create(tt.path, tt.size, tt.format)
		msg := fmt.Sprintf("%d: Create(%s, %d, %v)", i, tt.path, tt.size, tt.format)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%s: mismatched errors, actual %v expected %v", msg, err, tt.err)
		case (disk == nil && tt.disk != nil) || (disk != nil && tt.disk == nil):
			t.Errorf("%s: mismatched disk, actual %v expected %v", msg, disk, tt.disk)
		case disk != nil && (disk.LogicalBlocksize != tt.disk.LogicalBlocksize || disk.PhysicalBlocksize != tt.disk.PhysicalBlocksize || disk.Size != tt.disk.Size || disk.Type != tt.disk.Type):
			t.Errorf("%s: mismatched disk, actual then expected", msg)
			t.Logf("%#v", disk)
			t.Logf("%#v", tt.disk)
		}
		if disk != nil {
			os.Remove(tt.path)
		}
	}
}
