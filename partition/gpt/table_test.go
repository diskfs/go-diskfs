package gpt_test

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/partition/gpt"
	"github.com/diskfs/go-diskfs/testhelper"
)

const (
	gptFile = "./testdata/gpt.img"
	gptSize = 128 * 128
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

// compareProtectiveMBRBytes compare specially because we ignore sector/head/cylinder
func compareProtectiveMBRBytes(b1, b2 []byte) bool {
	if (b1 == nil && b2 != nil) || (b2 == nil && b1 != nil) {
		return false
	}
	if b1 == nil && b2 == nil {
		return true
	}
	if len(b1) != len(b2) {
		return false
	}
	return b1[0] == b2[0] &&
		b1[4] == b2[4] &&
		bytes.Equal(b1[8:12], b2[8:12]) &&
		bytes.Equal(b1[12:16], b2[12:16]) &&
		bytes.Equal(b1[16:], b2[16:])
}

//nolint:deadcode,unused // useful function for internal tests
func compareGPTBytes(b1, b2 []byte) bool {
	sizeMatch := len(b1) == len(b2)
	// everything before the MBR partition entries is ignored
	// everything from second partition entry onwards should be compared
	// the one and only partition entry should compare only: bootable flag, type, start LBA, end LBA
	mbr1, mbr2 := b1[:512], b2[:512]
	gptSectionMatch := bytes.Equal(b1[512:], b2[512:])
	mbrPostPart1Match := bytes.Equal(mbr1[446+16:], mbr2[446+16:])
	part1, part2 := b1[446:446+16], b2[446:446+16]
	bootableMatch := part1[0] == part2[0]
	typeMatch := part1[4] == part2[4]
	startLBAMatch := bytes.Equal(part1[8:12], part2[8:12])
	endLBAMatch := bytes.Equal(part1[12:16], part2[12:16])

	return sizeMatch && gptSectionMatch && mbrPostPart1Match && bootableMatch && typeMatch && startLBAMatch && endLBAMatch
}

func TestTableType(t *testing.T) {
	expected := "gpt"
	table := gpt.GetValidTable()
	tableType := table.Type()
	if tableType != expected {
		t.Errorf("Type() returned unexpected table type, actual %s expected %s", tableType, expected)
	}
}

func TestTableRead(t *testing.T) {
	t.Run("error reading file", func(t *testing.T) {
		expected := "error reading GPT from file"
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Reader signatire
			Reader: func(b []byte, offset int64) (int, error) {
				return 0, errors.New(expected)
			},
		}
		table, err := gpt.Read(f, 512, 512)
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
		expected := fmt.Sprintf("read only %d bytes of GPT", size)
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Reader signatire
			Reader: func(b []byte, offset int64) (int, error) {
				return size, nil
			},
		}
		table, err := gpt.Read(f, 512, 512)
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
		f, err := os.Open(gptFile)
		if err != nil {
			t.Fatalf("error opening file %s to read: %v", gptFile, err)
		}
		table, err := gpt.Read(f, 512, 512)
		if table == nil {
			t.Errorf("returned nil instead of table")
		}
		if err != nil {
			t.Errorf("returned error %v instead of nil", err)
		}
		expected := gpt.GetValidTable()
		if table == nil || !table.Equal(expected) {
			t.Errorf("actual table was %v instead of expected %v", table, expected)
		}
	})
}

//nolint:gocyclo // we really do not care about the cyclomatic complexity of a test function. Maybe someday we will improve it.
func TestTableWrite(t *testing.T) {
	t.Run("error writing file", func(t *testing.T) {
		table := gpt.GetValidTable()
		expected := "error writing protective MBR to disk"
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
		table := gpt.GetValidTable()
		var size int
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Writer signatire
			Writer: func(b []byte, offset int64) (int, error) {
				size = len(b) - 1
				return size, nil
			},
		}
		err := table.Write(f, tenMB)
		expected := fmt.Sprintf("wrote %d bytes of protective MBR", size)
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("successful write", func(t *testing.T) {
		table := gpt.GetValidTable()
		gptFileRef, err := os.Open(gptFile)
		if err != nil {
			t.Fatalf("unable to open gpt file: %v", err)
		}
		defer gptFileRef.Close()
		if err != nil {
			t.Fatalf("error opening file %s: %v", gptFile, err)
		}
		firstBytes := make([]byte, gptSize+512*2)
		firstRead, err := gptFileRef.ReadAt(firstBytes, 0)
		if err != nil {
			t.Fatalf("error reading primary header from file %s: %v", gptFile, err)
		}
		if firstRead != len(firstBytes) {
			t.Fatalf("read %d instead of %d bytes primary header from file %s", firstRead, len(firstBytes), gptFile)
		}
		protectiveMBR := firstBytes[446:512]
		primaryHeader := firstBytes[512:1024]
		primaryArray := firstBytes[1024:]
		tableProtectiveMBR := make([]byte, 0, len(protectiveMBR))
		tablePrimaryHeader := make([]byte, 0, len(primaryHeader))
		tablePrimaryArray := make([]byte, 0, len(primaryArray))

		f := &testhelper.FileImpl{
			Writer: func(b []byte, offset int64) (int, error) {
				switch offset {
				case 446:
					tableProtectiveMBR = append(tableProtectiveMBR, b...)
				case 512:
					tablePrimaryHeader = append(tablePrimaryHeader, b...)
				case 1024:
					tablePrimaryArray = append(tablePrimaryArray, b...)
				}
				return len(b), nil
			},
		}
		err = table.Write(f, tenMB)
		if err != nil {
			t.Errorf("returned error %v instead of nil", err)
		}
		if !compareProtectiveMBRBytes(tableProtectiveMBR, protectiveMBR) {
			t.Errorf("mismatched protective MBR")
		}
		if !bytes.Equal(tablePrimaryHeader, primaryHeader) {
			t.Errorf("mismatched primary header")
		}
		if !bytes.Equal(tablePrimaryArray, primaryArray) {
			t.Errorf("mismatched primary array")
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
		partitionStart := uint64(2048)
		// make it a 5MB partition
		partitionEnd := uint64(5*1024*1024/sectorSize) + partitionStart
		name := "EFI System Tester"
		table := &gpt.Table{
			Partitions: []*gpt.Partition{
				{Start: partitionStart, End: partitionEnd, Type: gpt.EFISystemPartition, Name: name},
			},
			LogicalSectorSize: sectorSize,
			ProtectiveMBR:     true,
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
			err := testhelper.DockerRun(nil, output, false, true, mounts, intImage, "sgdisk", "-i", "1", mpath)
			outString := output.String()
			if err != nil {
				t.Errorf("unexpected err: %v", err)
				t.Log(outString)
			}

			/* expected output format
			Partition GUID code: C12A7328-F81F-11D2-BA4B-00A0C93EC93B (EFI System)
			Partition unique GUID: 8E01DC62-9FB2-4C9D-811D-77B96B9DBDE4
			First sector: 2048 (at 1024.0 KiB)
			Last sector: 5242880 (at 2.5 GiB)
			Partition size: 5240833 sectors (2.5 GiB)
			Attribute flags: 0000000000000000
			Partition name: 'EFI System'
			*/
			partitionTypeMatcher := regexp.MustCompile(`Partition GUID code: ([A-F0-9\-]+) `)
			partitionGUIDMatcher := regexp.MustCompile(`Partition unique GUID: ([A-F0-9\-]+)\n`)
			firstSectorMatcher := regexp.MustCompile(`First sector: (\d+) `)
			lastSectorMatcher := regexp.MustCompile(`Last sector: (\d+) `)
			partitionNameMatcher := regexp.MustCompile(`Partition name: '([^']+)'`)

			partitionType := partitionTypeMatcher.FindStringSubmatch(outString)
			partitionGUID := partitionGUIDMatcher.FindStringSubmatch(outString)
			firstSector := firstSectorMatcher.FindStringSubmatch(outString)
			lastSector := lastSectorMatcher.FindStringSubmatch(outString)
			partitionName := partitionNameMatcher.FindStringSubmatch(outString)

			switch {
			case len(partitionType) < 2:
				t.Errorf("unable to retrieve partition type %v", partitionType)
			case partitionType[1] != string(gpt.EFISystemPartition):
				t.Errorf("Mismatched partition type, actual %s expected %s", partitionType[1], gpt.EFISystemPartition)
			}

			switch {
			case len(partitionGUID) < 2:
				t.Errorf("unable to retrieve partition guid %v", partitionGUID)
			case len(partitionGUID[1]) < 36:
				t.Errorf("invalid partition GUID: %s", partitionGUID[1])
			}

			switch {
			case len(firstSector) < 2:
				t.Errorf("unable to retrieve partition first sector %v", firstSector)
			case firstSector[1] != strconv.Itoa(int(partitionStart)):
				t.Errorf("Mismatched partition sector start, actual %s expected %d", firstSector[1], partitionStart)
			}

			switch {
			case len(lastSector) < 2:
				t.Errorf("unable to retrieve partition last sector %v", lastSector)
			case lastSector[1] != strconv.Itoa(int(partitionEnd)):
				t.Errorf("Mismatched partition sector end, actual %s expected %d", lastSector[1], partitionEnd)
			}

			switch {
			case len(partitionName) < 2:
				t.Errorf("unable to retrieve partition name %v", partitionName)
			case partitionName[1] != name:
				t.Errorf("Mismatched partition name, actual %s expected %s", partitionName[1], name)
			}

			err = testhelper.DockerRun(nil, output, false, true, mounts, intImage, "sgdisk", "--print", mpath)
			outString = output.String()
			if err != nil {
				t.Errorf("unexpected err: %v", err)
				t.Log(outString)
			}

			/* expected output format
			Sector size (logical): 512 bytes
			Disk identifier (GUID): E818397E-4D04-45D0-8CE9-B1D355EABCE5
			Partition table holds up to 128 entries
			Main partition table begins at sector 2 and ends at sector 33
			First usable sector is 34, last usable sector is 20446
			Partitions will be aligned on 2048-sector boundaries
			Total free space is 10172 sectors (5.0 MiB)

			Number  Start (sector)    End (sector)  Size       Code  Name
			   1            2048           12288   5.0 MiB     EF00  EFI System Tester
			*/

			usableSectorMatcher := regexp.MustCompile(`First usable sector is (\d+), last usable sector is (\d+)`)
			usableSector := usableSectorMatcher.FindStringSubmatch(outString)
			firstUsableSectorExpected := "34"
			lastUsableSectorExpected := "20446"

			switch {
			case len(usableSector) < 3:
				t.Errorf("unable to get usable sectors %v", usableSector)
			case usableSector[1] != firstUsableSectorExpected:
				t.Errorf("Mismatched first usable sector, actual %s expected %s", usableSector[1], firstUsableSectorExpected)
			case usableSector[2] != lastUsableSectorExpected:
				t.Errorf("Mismatched last usable sector, actual %s expected %s", usableSector[2], lastUsableSectorExpected)
			}
		}
	})
}
func TestGetPartitionSize(t *testing.T) {
	table := gpt.GetValidTable()
	request := 0
	size := table.Partitions[request].GetSize()
	expected := int64(table.Partitions[request].Size)
	if size != expected {
		t.Errorf("received size %d instead of %d", size, expected)
	}
}
func TestGetPartitionStart(t *testing.T) {
	table := gpt.GetValidTable()
	maxPart := len(table.Partitions)
	request := maxPart - 1
	start := table.Partitions[request].GetStart()
	expected := int64(table.Partitions[request].Start * uint64(table.LogicalSectorSize))
	if start != expected {
		t.Errorf("received start %d instead of %d", start, expected)
	}
}
func TestReadPartitionContents(t *testing.T) {
	table := gpt.GetValidTable()
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
	table := gpt.GetValidTable()
	request := 0
	size := table.Partitions[request].Size
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
	if written != size {
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

func TestResizeTableAndExpandPartition(t *testing.T) {
	const newSize = 11 * 1024 * 1024

	blkFile, err := os.Open(gptFile)
	if err != nil {
		t.Errorf("cannot open file: %v", err)
	}
	defer blkFile.Close()
	table, err := gpt.Read(blkFile, 512, 512)
	if err != nil {
		t.Errorf("cannot read gpt: %v", err)
	}
	table.Resize(newSize)

	tmpDir := t.TempDir()
	tmpImgPath := filepath.Join(tmpDir, "gpt.img")
	tmpImgFile, err := os.Create(tmpImgPath)
	if err != nil {
		t.Errorf("cannot create output image file: %v", err)
	}
	err = os.Truncate(tmpImgPath, newSize)
	if err != nil {
		t.Errorf("cannot truncate file: %v", err)
	}

	table.Partitions[0].Expand(100)
	err = table.Write(tmpImgFile, newSize)
	if err != nil {
		t.Errorf("cannot write table back: %v", err)
	}
	newTable, err := gpt.Read(tmpImgFile, 512, 512)
	if err != nil {
		t.Errorf("cannot read table back: %v", err)
	}
	if newTable.Partitions[0].Start != 2048 {
		t.Fail()
	}
	if newTable.Partitions[0].End != 3148 {
		t.Fail()
	}
	if newTable.Partitions[0].Size != 563712 {
		t.Fail()
	}
}

func TestResize(t *testing.T) {
	const newSize = 1024 * 1024
	table := gpt.GetValidTable()
	table.Resize(newSize)
	resultSize := table.TotalSize()
	if resultSize != newSize {
		t.Fail()
	}
	resultLastDataSector := table.LastDataSector()
	expectedLastDataSector := uint64((newSize / 512) - 34)
	if resultLastDataSector != expectedLastDataSector {
		t.Fail()
	}
}

func TestExpandPartition(t *testing.T) {
	table := gpt.GetValidTable()
	part := table.Partitions[0]
	part.Expand(100)
	if part.End != 3148 {
		t.Fail()
	}
	if part.Size != (3148-2048+1)*512 {
		t.Fail()
	}
}
