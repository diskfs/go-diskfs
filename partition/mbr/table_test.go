package mbr_test

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/partition/mbr"
	"github.com/diskfs/go-diskfs/testhelper"
)

const (
	mbrFile = "./testdata/mbr.img"
	tenMB   = 10 * 1024 * 1024
)

var (
	intImage     = os.Getenv("TEST_IMAGE")
	keepTmpFiles = os.Getenv("KEEPTESTFILES") == ""
)

func tmpDisk(source string, size int64) (*os.File, error) {
	filename := "disk_test"
	f, err := os.CreateTemp("", filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to create tempfile %s :%v", filename, err)
	}

	// either copy the contents of the source file over, or make a file of appropriate size
	if source == "" {
		// make it a 10MB file
		_ = f.Truncate(size)
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

// compareMBRBytes compare bytes from 446:512
// need compare function because we ignore cylinder/head/sector geometry
func compareMBRBytes(b1, b2 []byte) bool {
	if (b1 == nil && b2 != nil) || (b2 == nil && b1 != nil) {
		return false
	}
	if b1 == nil && b2 == nil {
		return true
	}
	if len(b1) != 66 || len(b2) != 66 {
		return false
	}
	// need to compare each of the partition arrays
	if !mbr.PartitionEqualBytes(b1[0:16], b2[0:16]) {
		return false
	}
	if !mbr.PartitionEqualBytes(b1[16:32], b2[16:32]) {
		return false
	}
	if !mbr.PartitionEqualBytes(b1[32:48], b2[32:48]) {
		return false
	}
	if !mbr.PartitionEqualBytes(b1[48:64], b2[48:64]) {
		return false
	}
	if !bytes.Equal(b1[64:66], b2[64:66]) {
		return false
	}
	return true
}

func TestTableType(t *testing.T) {
	expected := "mbr"
	table := mbr.GetValidTable()
	tableType := table.Type()
	if tableType != expected {
		t.Errorf("Type() returned unexpected table type, actual %s expected %s", tableType, expected)
	}
}

func TestTableRead(t *testing.T) {
	t.Run("error reading file", func(t *testing.T) {
		expected := "error reading MBR from file"
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Reader signatire
			Reader: func(b []byte, offset int64) (int, error) {
				return 0, errors.New(expected)
			},
		}
		table, err := mbr.Read(f, 512, 512)
		if table != nil {
			t.Errorf("returned table instead of nil")
		}
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("insufficient data read", func(t *testing.T) {
		size := 100
		expected := fmt.Sprintf("read only %d bytes of MBR", size)
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Reader signatire
			Reader: func(b []byte, offset int64) (int, error) {
				return size, nil
			},
		}
		table, err := mbr.Read(f, 512, 512)
		if table != nil {
			t.Errorf("returned table instead of nil")
		}
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("successful read", func(t *testing.T) {
		f, err := os.Open(mbrFile)
		if err != nil {
			t.Fatalf("error opening file %s to read: %v", mbrFile, err)
		}
		defer f.Close()
		table, err := mbr.Read(f, 512, 512)
		if err != nil {
			t.Errorf("returned error %v instead of nil", err)
		}
		if table == nil {
			t.Errorf("returned nil instead of table")
		}
		expected := mbr.GetValidTable()
		if table == nil && expected != nil || !table.Equal(expected) {
			t.Errorf("actual table was %v instead of expected %v", table, expected)
		}
	})
}
func TestTableWrite(t *testing.T) {
	t.Run("error writing file", func(t *testing.T) {
		table := mbr.GetValidTable()
		expected := "error writing partition table to disk"
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Writer signatire
			Writer: func(b []byte, offset int64) (int, error) {
				return 0, errors.New(expected)
			},
		}
		err := table.Write(f, tenMB)
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("insufficient data written", func(t *testing.T) {
		table := mbr.GetValidTable()
		var size int
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Writer signatire
			Writer: func(b []byte, offset int64) (int, error) {
				size = len(b) - 1
				return size, nil
			},
		}
		err := table.Write(f, tenMB)
		expected := fmt.Sprintf("partition table wrote %d bytes to disk", size)
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("successful write", func(t *testing.T) {
		table := mbr.GetValidTable()
		mbrFileHandle, err := os.Open(mbrFile)
		if err != nil {
			t.Fatalf("error opening file %s: %v", mbrFile, err)
		}
		defer mbrFileHandle.Close()
		mbrBytes := make([]byte, 512)
		read, err := mbrFileHandle.ReadAt(mbrBytes, 0)
		if err != nil {
			t.Fatalf("error reading MBR from file %s: %v", mbrFile, err)
		}
		if read != len(mbrBytes) {
			t.Fatalf("read %d instead of %d bytes MBR from file %s", read, len(mbrBytes), mbrFile)
		}
		bootloader := mbrBytes[:446]
		remainder := mbrBytes[446:]
		tableBytes := make([]byte, 0, len(remainder))

		f := &testhelper.FileImpl{
			Writer: func(b []byte, offset int64) (int, error) {
				switch offset {
				case 446:
					tableBytes = append(tableBytes, b...)
				default:
					t.Fatalf("Attempted to write at position %d instead of %d", offset, 446)
				}
				return len(b), nil
			},
		}
		err = table.Write(f, tenMB)
		if err != nil {
			t.Errorf("returned error %v instead of nil", err)
		}
		if !compareMBRBytes(remainder, tableBytes) {
			t.Log(remainder)
			t.Log(tableBytes)
			t.Errorf("mismatched MBR")
		}
		// need to check that bootloader was unchanged
		bootloaderBytes := make([]byte, 446)
		read, err = mbrFileHandle.ReadAt(bootloaderBytes, 0)
		if err != nil {
			t.Fatalf("error reading bootloader from file %s: %v", mbrFile, err)
		}
		if read != len(bootloaderBytes) {
			t.Fatalf("read %d instead of %d bytes bootloader from file %s", read, len(bootloaderBytes), mbrFile)
		}
		if !bytes.Equal(bootloader, bootloaderBytes) {
			t.Error("bootloader was changed when it should not be")
		}
	})
	t.Run("successful full test", func(t *testing.T) {
		f, err := tmpDisk("", 10*1024*1024)
		if err != nil {
			t.Fatalf("error creating new temporary disk: %v", err)
		}
		defer f.Close()

		if keepTmpFiles {
			defer os.Remove(f.Name())
		} else {
			fmt.Println(f.Name())
		}

		fileInfo, err := f.Stat()
		if err != nil {
			t.Fatalf("error reading info on temporary disk: %v", err)
		}

		// this is partition start and end in sectors, not bytes
		sectorSize := 512
		partitionStart := uint32(2048)
		// make it a 5MB partition
		partitionSize := uint32(5000)
		table := &mbr.Table{
			LogicalSectorSize:  sectorSize,
			PhysicalSectorSize: sectorSize,
			Partitions: []*mbr.Partition{
				{Bootable: true, Type: mbr.Linux, Start: partitionStart, Size: partitionSize},
			},
		}

		err = table.Write(f, fileInfo.Size())
		switch {
		case err != nil:
			t.Errorf("unexpected err: %v", err)
		default:
			// we only run this if we have a real image
			if intImage == "" {
				return
			}

			output := new(bytes.Buffer)
			mpath := "/file.img"
			mounts := map[string]string{
				f.Name(): mpath,
			}
			err := testhelper.DockerRun(nil, output, false, true, mounts, intImage, "sfdisk", "-l", mpath)
			outString := output.String()
			if err != nil {
				t.Errorf("unexpected err: %v", err)
				t.Log(outString)
			}

			/* expected output format
			Disk /file.img: 10 MiB, 10485760 bytes, 20480 sectors
			Units: sectors of 1 * 512 = 512 bytes
			Sector size (logical/physical): 512 bytes / 512 bytes
			I/O size (minimum/optimal): 512 bytes / 512 bytes
			Disklabel type: dos
			Disk identifier: 0x00000000

			Device     Boot Start   End Sectors  Size Id Type
			/file.img1 *     2048  7047    5000  2.5M 83 Linux
			*/
			partitionMatcher := regexp.MustCompile(`/file.img(\d)\s+(\s|\*)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+)`)

			partitionParts := partitionMatcher.FindStringSubmatch(outString)

			if len(partitionParts) < 9 {
				t.Errorf("unable to retrieve partition parts %v", partitionParts)
				return
			}

			// partition number should be "1"
			if partitionParts[1] != "1" {
				t.Errorf("mismatched partition number, actual %s, expected %d", partitionParts[1], 1)
			}
			// partition should be bootable
			if partitionParts[2] != "*" {
				t.Errorf("partition not marked as bootable")
			}
			// partition start should match
			if partitionParts[3] != strconv.Itoa(int(partitionStart)) {
				t.Errorf("mismatched partition start, actual %s, expected %d", partitionParts[3], partitionStart)
			}
			// partition size should match
			if partitionParts[5] != strconv.Itoa(int(partitionSize)) {
				t.Errorf("mismatched partition size, actual %s, expected %d", partitionParts[5], partitionSize)
			}
			// skip partitionParts[6] ; we do not look at the size in bytes,
			// partition type code should match
			if partitionParts[7] != fmt.Sprintf("%x", mbr.Linux) {
				t.Errorf("mismatched partition type, actual %s, expected %x", partitionParts[7], mbr.Linux)
			}
			// partition type name should match
			if partitionParts[8] != "Linux" {
				t.Errorf("mismatched partition type name, actual %s, expected %s", partitionParts[8], "Linux")
			}
		}
	})
}
func TestGetPartitionSize(t *testing.T) {
	table := mbr.GetValidTable()
	maxPart := len(table.Partitions)
	request := maxPart - 1
	size := table.Partitions[request].GetSize()
	expected := table.Partitions[request].Size
	if size != int64(expected) {
		t.Errorf("received size %d instead of %d", size, expected)
	}
}
func TestGetPartitionStart(t *testing.T) {
	table := mbr.GetValidTable()
	maxPart := len(table.Partitions)
	request := maxPart - 1
	start := table.Partitions[request].GetStart()
	expected := table.Partitions[request].Start
	if start != int64(expected) {
		t.Errorf("received start %d instead of %d", start, expected)
	}
}
func TestReadPartitionContents(t *testing.T) {
	table := mbr.GetValidTable()
	maxPart := len(table.Partitions)
	request := maxPart - 1
	var b bytes.Buffer
	writer := bufio.NewWriter(&b)
	size := 100
	b2 := make([]byte, size)
	_, _ = rand.Read(b2)
	f := &testhelper.FileImpl{
		//nolint:revive // b is unused, but we keep it here for the consistent io.Reader signatire
		Reader: func(b []byte, offset int64) (int, error) {
			copy(b, b2)
			return size, io.EOF
		},
	}
	read, err := table.Partitions[request].ReadContents(f, writer)
	if read != int64(size) {
		t.Errorf("returned %d bytes read instead of %d", read, size)
	}
	if err != nil {
		t.Errorf("error was not nil")
	}
	writer.Flush()
	if !bytes.Equal(b.Bytes(), b2) {
		t.Errorf("Mismatched bytes data")
		t.Log(b.Bytes())
		t.Log(b2)
	}
}
func TestWritePartitionContents(t *testing.T) {
	table := mbr.GetValidTable()
	request := 0
	size := table.Partitions[request].Size * uint32(table.LogicalSectorSize)
	b := make([]byte, size)
	_, _ = rand.Read(b)
	reader := bytes.NewReader(b)
	b2 := make([]byte, 0, size)
	f := &testhelper.FileImpl{
		//nolint:revive // b is unused, but we keep it here for the consistent io.Writer signatire
		Writer: func(b []byte, offset int64) (int, error) {
			b2 = append(b2, b...)
			return len(b), nil
		},
	}
	written, err := table.Partitions[request].WriteContents(f, reader)
	if written != uint64(size) {
		t.Errorf("returned %d bytes written instead of %d", written, size)
	}
	if err != nil {
		t.Errorf("error was not nil: %v", err)
	}
	if !bytes.Equal(b2, b) {
		t.Errorf("Bytes mismatch")
		t.Log(b)
		t.Log(b2)
	}
}
