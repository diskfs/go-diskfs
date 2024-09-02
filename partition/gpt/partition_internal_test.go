package gpt

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/testhelper"
)

const (
	gptPartitionFile = "./testdata/gpt_partition.dat"
)

func TestFromBytes(t *testing.T) {
	t.Run("Short byte slice", func(t *testing.T) {
		b := make([]byte, PartitionEntrySize-1)
		_, _ = rand.Read(b)
		partition, err := partitionFromBytes(b, 2048, 2048)
		if partition != nil {
			t.Error("should return nil partition")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := fmt.Sprintf("data for partition was %d bytes instead of expected %d", len(b), PartitionEntrySize)
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("Long byte slice", func(t *testing.T) {
		b := make([]byte, PartitionEntrySize+1)
		_, _ = rand.Read(b)
		partition, err := partitionFromBytes(b, 2048, 2048)
		if partition != nil {
			t.Error("should return nil partition")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := fmt.Sprintf("data for partition was %d bytes instead of expected %d", len(b), PartitionEntrySize)
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("Valid partition", func(t *testing.T) {
		b, err := os.ReadFile(gptPartitionFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptPartitionFile, err)
		}
		partition, err := partitionFromBytes(b, 0, 0)
		if partition == nil {
			t.Error("should not return nil partition")
		}
		if err != nil {
			t.Errorf("returned non-nil error: %v", err)
		}
		// check out data
		expected := Partition{
			Start:      2048,
			End:        3048,
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		if !partition.Equal(&expected) {
			t.Errorf("actual partition was %v instead of expected %v", partition, expected)
		}
	})
}

func TestToBytes(t *testing.T) {
	t.Run("invalid ID GUID", func(t *testing.T) {
		partition := Partition{
			Start:      2048,
			End:        3048,
			Name:       "EFI System",
			GUID:       "5CA3360B",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		b, err := partition.toBytes()
		if b != nil {
			t.Error("should return nil bytes")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "unable to parse partition identifier GUID"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("invalid Type GUID", func(t *testing.T) {
		partition := Partition{
			Start:      2048,
			End:        3048,
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       "ABCDEF",
		}
		b, err := partition.toBytes()
		if b != nil {
			t.Error("should return nil bytes")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := "unable to parse partition type GUID"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("too long name", func(t *testing.T) {
		partition := Partition{
			Start:      2048,
			End:        3048,
			Name:       "This is a very long name, as long as it is longer than 36 unicode character points, it should fail. Since that is 72 bytes, we are going to make it >72 chars.",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		b, err := partition.toBytes()
		if b != nil {
			t.Error("should return nil bytes")
		}
		if err == nil {
			t.Error("should not return nil error")
		}
		expected := fmt.Sprintf("cannot use %s as partition name, has %d Unicode code units, maximum size is 36", partition.Name, len(partition.Name))
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("Valid partition", func(t *testing.T) {
		partition := Partition{
			Start:      2048,
			End:        3048,
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		b, err := partition.toBytes()
		if b == nil {
			t.Error("should not return nil bytes")
		}
		if err != nil {
			t.Error("should return nil error")
		}
		expected, err := os.ReadFile(gptPartitionFile)
		if err != nil {
			t.Fatalf("unable to read test fixture file %s: %v", gptPartitionFile, err)
		}
		if !bytes.Equal(expected, b) {
			t.Errorf("returned byte %v instead of expected %v", b, expected)
		}
	})
}

func TestInitEntry(t *testing.T) {
	validGUID := regexp.MustCompile(`^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$`)
	goodGUID := "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51"
	var start, end, size uint64 = 2048, 3047, 1000 * 512

	t.Run("missing GUID", func(t *testing.T) {
		p := Partition{
			Start:      start,
			End:        end,
			Size:       size,
			Name:       "EFI System",
			GUID:       "",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		err := p.initEntry(512, 2048)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !validGUID.MatchString(p.GUID) {
			t.Errorf("did not initialize valid GUID, remains: %s", p.GUID)
		}
	})

	t.Run("existing GUID", func(t *testing.T) {
		p := Partition{
			Start:      start,
			End:        end,
			Size:       size,
			Name:       "EFI System",
			GUID:       goodGUID,
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		err := p.initEntry(512, 2048)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if p.GUID != goodGUID {
			t.Errorf("reset GUID even thought good ones existed")
		}
	})

	t.Run("no size", func(t *testing.T) {
		p := Partition{
			Start:      start,
			End:        end,
			Size:       0,
			Name:       "EFI System",
			GUID:       goodGUID,
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		err := p.initEntry(512, 2048)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if p.Size == 0 {
			t.Errorf("Did not reset size even though 0")
		}
		if p.Size != size {
			t.Errorf("size set to %d instead of %d", p.Size, size)
		}
	})

	t.Run("no end", func(t *testing.T) {
		p := Partition{
			Start:      start,
			End:        0,
			Size:       size,
			Name:       "EFI System",
			GUID:       goodGUID,
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		err := p.initEntry(512, 2048)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if p.End == 0 {
			t.Errorf("Did not reset end even though 0")
		}
		if p.End != end {
			t.Errorf("end set to %d instead of %d", p.End, end)
		}
	})

	t.Run("only size", func(t *testing.T) {
		var starting uint64 = 2048
		p := Partition{
			Start:      0,
			End:        0,
			Size:       size,
			Name:       "EFI System",
			GUID:       goodGUID,
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		err := p.initEntry(512, starting)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if p.End == 0 {
			t.Errorf("Did not reset end even though 0")
		}
		if p.Start == 0 {
			t.Errorf("Did not reset start even though 0")
		}
		if p.End != end {
			t.Errorf("end set to %d instead of %d", p.End, end)
		}
		if p.Start != start {
			t.Errorf("start set to %d instead of %d", p.Start, start)
		}
	})

	t.Run("mismatched sizes", func(t *testing.T) {
		var starting uint64 = 2048
		p := Partition{
			Start:      start,
			End:        end,
			Size:       size + 1,
			Name:       "EFI System",
			GUID:       goodGUID,
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		err := p.initEntry(512, starting)
		if err == nil {
			t.Fatal("returned unexpected nil error")
		}
		expected := "invalid partition entry"
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
}

func TestWriteContents(t *testing.T) {
	t.Run("mismatched size", func(t *testing.T) {
		partition := Partition{
			Start:      2048,
			End:        3048,
			Size:       500,
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		var b bytes.Buffer
		reader := bufio.NewReader(&b)
		expected := "cannot reconcile partition size"
		f := &testhelper.FileImpl{}
		written, err := partition.WriteContents(f, reader)
		if written != 0 {
			t.Errorf("returned %d bytes written instead of 0", written)
		}
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("error writing file", func(t *testing.T) {
		size := 512000
		partition := Partition{
			Start:      2048,
			End:        3047,
			Size:       uint64(size),
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		b := make([]byte, size)
		_, _ = rand.Read(b)
		reader := bytes.NewReader(b)
		expected := "error writing to file"
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Writer signatire
			Writer: func(b []byte, offset int64) (int, error) {
				return 0, errors.New(expected)
			},
		}
		written, err := partition.WriteContents(f, reader)
		if written != 0 {
			t.Errorf("returned %d bytes written instead of 0", written)
		}
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
			return
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("too large for partition", func(t *testing.T) {
		partition := Partition{
			Start:      2048,
			End:        2048,
			Size:       uint64(512),
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		// make a byte array that is too big
		b := make([]byte, 2*512)
		_, _ = rand.Read(b)
		reader := bytes.NewReader(b)
		expected := "requested to write at least"
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Writer signatire
			Writer: func(b []byte, offset int64) (int, error) {
				return len(b), nil
			},
		}
		read, err := partition.WriteContents(f, reader)
		if read != partition.Size {
			t.Errorf("returned %d bytes read instead of %d", read, partition.Size)
		}
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
			return
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})

	t.Run("successful write", func(t *testing.T) {
		size := 512000
		partition := Partition{
			Start:      2048,
			End:        3047,
			Size:       uint64(size),
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		b := make([]byte, size)
		_, _ = rand.Read(b)
		b2 := make([]byte, 0, size)
		reader := bytes.NewReader(b)
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Writer signatire
			Writer: func(b []byte, offset int64) (int, error) {
				b2 = append(b2, b...)
				return len(b), nil
			},
		}
		written, err := partition.WriteContents(f, reader)
		if written != uint64(size) {
			t.Errorf("returned %d bytes written instead of %d", written, size)
		}
		if err != nil {
			t.Errorf("returned error instead of nil")
			return
		}
		if !bytes.Equal(b2, b) {
			t.Errorf("Bytes mismatch")
			t.Log(b)
			t.Log(b2)
		}
	})
}

func TestReadContents(t *testing.T) {
	t.Run("error reading file", func(t *testing.T) {
		partition := Partition{
			Start:      2048,
			End:        3048,
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
		var b bytes.Buffer
		writer := bufio.NewWriter(&b)
		expected := "error reading from file"
		f := &testhelper.FileImpl{
			//nolint:revive // b is unused, but we keep it here for the consistent io.Reader signatire
			Reader: func(b []byte, offset int64) (int, error) {
				return 0, errors.New(expected)
			},
		}
		read, err := partition.ReadContents(f, writer)
		if read != 0 {
			t.Errorf("returned %d bytes read instead of 0", read)
		}
		if err == nil {
			t.Errorf("returned nil error instead of actual errors")
		}
		if !strings.HasPrefix(err.Error(), expected) {
			t.Errorf("error type %s instead of expected %s", err.Error(), expected)
		}
	})
	t.Run("successful read", func(t *testing.T) {
		partition := Partition{
			Start:      2048,
			End:        3048,
			Name:       "EFI System",
			GUID:       "5CA3360B-5DE6-4FCF-B4CE-419CEE433B51",
			Attributes: 0,
			Type:       EFISystemPartition,
		}
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
		read, err := partition.ReadContents(f, writer)
		if read != int64(size) {
			t.Errorf("returned %d bytes read instead of %d", read, size)
		}
		if err != nil {
			t.Errorf("returned error instead of expected nil")
		}
		writer.Flush()
		if !bytes.Equal(b.Bytes(), b2) {
			t.Errorf("Mismatched bytes data")
			t.Log(b.Bytes())
			t.Log(b2)
		}
	})
}
