package fat32

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/diskfs/go-diskfs/util"
)

var (
	timeDateTimeTests = []struct {
		date uint16
		time uint16
		rfc  string
	}{
		// see reference at https://en.wikipedia.org/wiki/Design_of_the_FAT_file_system#DIR_OFS_10h and https://en.wikipedia.org/wiki/Design_of_the_FAT_file_system#DIR_OFS_0Eh
		{0x0022, 0x7472, "1980-01-02T14:35:36Z"}, // date: 0b0000000 0001 00010 / 0x0022 ; time: 0b01110 100011 10010 / 0x7472
		{0x1f79, 0x0203, "1995-11-25T00:16:07Z"}, // date: 0b0001111 1011 11001 / 0x1f79 ; time: 0b00000 010000 00011 / 0x0203
		{0xf2de, 0x6000, "2101-06-30T12:00:00Z"}, // date: 0b1111001 0110 11110 / 0xf2de ; time: 0b01100 000000 00000 / 0x6000
	}

	unarcBytes = []byte{
		0x43, 0x6f, 0x00, 0x2e, 0x00, 0x64, 0x00, 0x61, 0x00, 0x74, 0x00, 0x0f, 0x00, 0xb3, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
		0x02, 0x6e, 0x00, 0x20, 0x00, 0x6e, 0x00, 0x6f, 0x00, 0x6d, 0x00, 0x0f, 0x00, 0xb3, 0x62, 0x00, 0x72, 0x00, 0x65, 0x00, 0x20, 0x00, 0x6c, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72, 0x00, 0x67, 0x00,
		0x01, 0x55, 0x00, 0x6e, 0x00, 0x20, 0x00, 0x61, 0x00, 0x72, 0x00, 0x0f, 0x00, 0xb3, 0x63, 0x00, 0x68, 0x00, 0x69, 0x00, 0x76, 0x00, 0x6f, 0x00, 0x20, 0x00, 0x00, 0x00, 0x63, 0x00, 0x6f, 0x00,
	}

	lfnBytesTests = []struct {
		lfn string
		err error
		b   []byte
	}{
		// first 2 are too short and too long - rest are normal
		{"", fmt.Errorf("longFilenameEntryFromBytes only can parse byte of length 32"), []byte{0x43, 0x6f, 0x00, 0x2e, 0x00, 0x64, 0x00, 0x61, 0x00, 0x74, 0x00, 0x0f, 0x00, 0xb3, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff}},
		{"", fmt.Errorf("longFilenameEntryFromBytes only can parse byte of length 32"), []byte{0x43, 0x6f, 0x00, 0x2e, 0x00, 0x64, 0x00, 0x61, 0x00, 0x74, 0x00, 0x0f, 0x00, 0xb3, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00}},
		// normal are taken from ./testdata/README.md
		{"o.dat", nil, unarcBytes[0:32]},
		{"n nombre larg", nil, unarcBytes[32:64]},
		{"Un archivo co", nil, unarcBytes[64:96]},
		{"o", nil, []byte{0x42, 0x6f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x00, 0x59, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff}},
		{"tercer_archiv", nil, []byte{0x01, 0x74, 0x00, 0x65, 0x00, 0x72, 0x00, 0x63, 0x00, 0x65, 0x00, 0x0f, 0x00, 0x59, 0x72, 0x00, 0x5f, 0x00, 0x61, 0x00, 0x72, 0x00, 0x63, 0x00, 0x68, 0x00, 0x00, 0x00, 0x69, 0x00, 0x76, 0x00}},
		// this one adds some unicode
		{"edded_nameא", nil, []byte{0x42, 0x65, 0x00, 0x64, 0x00, 0x64, 0x00, 0x65, 0x00, 0x64, 0x00, 0x0f, 0x00, 0x60, 0x5f, 0x00, 0x6e, 0x00, 0x61, 0x00, 0x6d, 0x00, 0x65, 0x00, 0xd0, 0x05, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff}},
		{"some_long_emb", nil, []byte{0x01, 0x73, 0x00, 0x6f, 0x00, 0x6d, 0x00, 0x65, 0x00, 0x5f, 0x00, 0x0f, 0x00, 0x60, 0x6c, 0x00, 0x6f, 0x00, 0x6e, 0x00, 0x67, 0x00, 0x5f, 0x00, 0x65, 0x00, 0x00, 0x00, 0x6d, 0x00, 0x62, 0x00}},
	}

	sfnBytesTests = []struct {
		shortName string
		extension string
		lfn       string
		b         []byte
		err       error
	}{
		// first several tests use invalid shortname char or too long
		{"foo", "TXT", "very long filename indeed", nil, fmt.Errorf("could not calculate checksum for 8.3 filename: invalid shortname character in filename")},
		{"א", "TXT", "very long filename indeed", nil, fmt.Errorf("could not calculate checksum for 8.3 filename: invalid shortname character in filename")},
		{"abcdefghuk", "TXT", "very long filename indeed", nil, fmt.Errorf("could not calculate checksum for 8.3 filename: invalid shortname character in filename")},
		{"FOO", "א", "very long filename indeed", nil, fmt.Errorf("could not calculate checksum for 8.3 filename: invalid shortname character in extension")},
		{"FOO", "TXT234", "very long filename indeed", nil, fmt.Errorf("could not calculate checksum for 8.3 filename: extension for file is longer")},
		{"FOO", "txt", "very long filename indeed", nil, fmt.Errorf("could not calculate checksum for 8.3 filename: invalid shortname character in extension")},
		// rest are valid
		{"UNARCH~1", "DAT", "Un archivo con nombre largo.dat", unarcBytes, nil},
	}
)

func compareDirectoryEntriesIgnoreDates(a, b *directoryEntry) bool {
	now := time.Now()
	// copy values so we do not mess up the originals
	c := &directoryEntry{}
	d := &directoryEntry{}
	*c = *a
	*d = *b

	// unify fields we let be equal
	c.createTime = now
	d.createTime = now
	c.accessTime = now
	d.accessTime = now
	c.modifyTime = now
	d.modifyTime = now

	return *c == *d
}

func TestDirectoryEntryLongFilenameBytes(t *testing.T) {
	for _, tt := range sfnBytesTests {
		output, err := longFilenameBytes(tt.lfn, tt.shortName, tt.extension)
		if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())) {
			t.Log(err)
			t.Log(tt.err)
			t.Errorf("mismatched err expected, actual: %v, %v", tt.err, err)
		}
		if !bytes.Equal(output, tt.b) {
			t.Errorf("longFilenameBytes(%s, %s, %s) bytes mismatch", tt.lfn, tt.shortName, tt.extension)
			t.Logf("actual  : % x", output)
			t.Logf("expected: % x", tt.b)
		}
	}
}

func TestDirectoryEntryLongFilenameEntryFromBytes(t *testing.T) {
	for i, tt := range lfnBytesTests {
		output, err := longFilenameEntryFromBytes(tt.b)
		if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())) {
			t.Errorf("mismatched err expected, actual: %v, %v", tt.err, err)
		}
		if output != tt.lfn {
			t.Errorf("%d: longFilenameEntryFromBytes() returned %s instead of %s from %v", i, output, tt.lfn, tt.b)
		}
	}
}

func TestDateTimeToTime(t *testing.T) {
	for _, tt := range timeDateTimeTests {
		output := dateTimeToTime(tt.date, tt.time)
		expected, err := time.Parse(time.RFC3339, tt.rfc)
		if err != nil {
			t.Fatalf("error parsing expected date: %v", err)
		}
		// handle odd error case
		if expected.Second()%2 != 0 {
			expected = expected.Add(-1 * time.Second)
		}
		if expected != output {
			t.Errorf("dateTimeToTime(%d, %d) expected output %v, actual %v", tt.date, tt.time, expected, output)
		}
	}
}

func TestTimeToDateTime(t *testing.T) {
	for _, tt := range timeDateTimeTests {
		input, err := time.Parse(time.RFC3339, tt.rfc)
		if err != nil {
			t.Fatalf("error parsing input date: %v", err)
		}
		outDate, outTime := timeToDateTime(input)
		if outDate != tt.date || outTime != tt.time {
			t.Errorf("timeToDateTime(%v) expected output %d %d, actual %d %d", tt.rfc, tt.date, tt.time, outDate, outTime)
		}
	}
}

func TestDirectoryEntryLfnChecksum(t *testing.T) {
	/*
		the values for the hashes are taken from testdata/calcsfn_checksum.c, which is based on the
		formula given at https://en.wikipedia.org/wiki/Design_of_the_FAT_file_system#VFAT_long_file_names
	*/
	tests := []struct {
		name      string
		extension string
		output    byte
		err       error
	}{
		// first all of the error cases
		{"abc\u2378", "F", 0x00, fmt.Errorf("invalid shortname character in filename")},
		{"abc", "F", 0x00, fmt.Errorf("invalid shortname character in filename")},
		{"ABC", "F\u2378", 0x00, fmt.Errorf("invalid shortname character in extension")},
		{"ABC", "f", 0x00, fmt.Errorf("invalid shortname character in extension")},
		{"ABCDEFGHIJ", "F", 0x00, fmt.Errorf("short name for file is longer than")},
		{"ABCD", "FUUYY", 0x00, fmt.Errorf("extension for file is longer than")},
		// valid exact length of each
		{"ABCDEFGH", "TXT", 0xf6, nil},
		// shortened each
		{"ABCDEFG", "TXT", 0x51, nil},
		{"ABCDEFGH", "TX", 0xc2, nil},
		{"ABCDEF", "T", 0xcf, nil},
	}
	for _, tt := range tests {
		output, err := lfnChecksum(tt.name, tt.extension)
		if output != tt.output {
			t.Errorf("lfnChecksum(%s,%s) expected output %v, actual %v", tt.name, tt.extension, tt.output, output)
		}
		if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())) {
			t.Errorf("mismatched err expected, actual: %v, %v", tt.err, err)
		}
	}
}

func TestDirectoryEntryStringToASCIIBytes(t *testing.T) {
	tests := []struct {
		input  string
		output []byte
		err    error
	}{
		{"abc", []byte{0x61, 0x62, 0x63}, nil},
		{"abcdefg", []byte{0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67}, nil},
		{"abcdef\u2318", nil, fmt.Errorf("non-ASCII character in name: %s", "abcdef\u2318")},
	}
	for _, tt := range tests {
		output, err := stringToASCIIBytes(tt.input)
		if !bytes.Equal(output, tt.output) {
			t.Errorf("stringToASCIIBytes(%s) expected output %v, actual %v", tt.input, tt.output, output)
		}
		if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())) {
			t.Errorf("mismatched err expected, actual: %v, %v", tt.err, err)
		}
	}
}

func TestDirectoryEntryCalculateSlots(t *testing.T) {
	// holds 13 chars per slot, so test x<13, x==13, 13<x<26, x==26, 26< x
	tests := []struct {
		input string
		slots int
	}{
		{"abc", 1},
		{"abcdefghijklm", 1},
		{"abcdefghijklmn", 2},
		{"abcdefghijklmnopqrstuvwxyz", 2},
		{"abcdefghijklmnopqrstuvwxyz1", 3},
	}
	for _, tt := range tests {
		slots := calculateSlots(tt.input)
		if slots != tt.slots {
			t.Errorf("calculateSlots(%s) expected %d , actual %d", tt.input, tt.slots, slots)
		}
	}
}

func TestDirectoryEntryConvertLfnSfn(t *testing.T) {
	tests := []struct {
		input       string
		sfn         string
		extension   string
		isLfn       bool
		isTruncated bool
	}{
		{"ABC", "ABC", "", false, false},
		{"ABC.TXT", "ABC", "TXT", false, false},
		{"abc", "ABC", "", true, false},
		{"ABC.TXTTT", "ABC", "TXT", true, false},
		{"ABC.txt", "ABC", "TXT", true, false},
		{"aBC.q", "ABC", "Q", true, false},
		{"ABC.q.rt", "ABCQ", "RT", true, false},
		{"VeryLongName.ft", "VERYLO~1", "FT", true, true},
	}
	for _, tt := range tests {
		sfn, extension, isLfn, isTruncated := convertLfnSfn(tt.input)
		if sfn != tt.sfn || extension != tt.extension || isLfn != tt.isLfn || isTruncated != tt.isTruncated {
			t.Errorf("convertLfnSfn(%s) expected %s / %s / %t / %t ; actual %s / %s / %t / %t", tt.input, tt.sfn, tt.extension, tt.isLfn, tt.isTruncated, sfn, extension, isLfn, isTruncated)
		}
	}
}

func TestDirectoryEntryUCaseValid(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{"abc", "ABC"},
		{"ABC", "ABC"},
		{"aBC", "ABC"},
		{"a15D", "A15D"},
		{"A BC", "ABC"},
		{"A..-a*)82y12112bb", "A-A_)82Y12112BB"},
	}
	for _, tt := range tests {
		output := uCaseValid(tt.input)
		if output != tt.output {
			t.Errorf("uCaseValid(%s) expected %s actual %s", tt.input, tt.output, output)
		}
	}
}

func TestDirectoryEntryParseDirEntries(t *testing.T) {
	validDe, b, err := GetValidDirectoryEntries()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		de  []*directoryEntry
		b   []byte
		err error
	}{
		{validDe, b, nil},
	}

	for _, tt := range tests {
		output, err := parseDirEntries(tt.b)
		switch {
		case (err != nil && tt.err == nil) || (err == nil && tt.err != nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Log(err)
			t.Log(tt.err)
			t.Errorf("mismatched err expected, actual: %v, %v", tt.err, err)
		case (output == nil && tt.de != nil) || (tt.de == nil && output != nil):
			t.Errorf("parseDirEntries() DirectoryEntry mismatched nil actual, expected %v %v", output, tt.de)
		case len(output) != len(tt.de):
			t.Errorf("parseDirEntries() DirectoryEntry mismatched length actual, expected %d %d", len(output), len(tt.de))
		default:
			for i, de := range output {
				if *de != *tt.de[i] {
					t.Errorf("%d: parseDirEntries() DirectoryEntry mismatch, actual then valid:", i)
					t.Log(de)
					t.Log(tt.de[i])
				}
			}
		}
	}
}

func TestDirectoryEntryToBytes(t *testing.T) {
	validDe, validBytes, err := GetValidDirectoryEntries()
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for _, de := range validDe {
		b, err := de.toBytes()
		expected := validBytes[i*32 : (i+1+de.longFilenameSlots)*32]
		if err != nil {
			t.Errorf("error converting directory entry to bytes: %v", err)
			t.Logf("%v", de)
		} else {
			diff, diffString := util.DumpByteSlicesWithDiffs(b, expected, 32, false, true, true)
			if diff {
				t.Errorf("directory.toBytes() %s mismatched, actual then expected\n%s", de.filenameShort, diffString)
			}
		}
		i += de.longFilenameSlots + 1
	}
}
