package iso9660

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-test/deep"
)

const (
	volRecordsFile = "./testdata/volrecords.iso"
)

var (
	timeDecBytesTests = []struct {
		b   []byte
		rfc string
	}{
		// see reference at https://wiki.osdev.org/ISO_9660#Volume_Descriptors
		{append([]byte("1980010214353600"), 0), "1980-01-02T14:35:36Z"},
		{append([]byte("1995112500160700"), 8), "1995-11-25T00:16:07+02:00"},
		{append([]byte("2101063012000000"), 0xe6), "2101-06-30T12:00:00-06:30"},
	}
)

func comparePrimaryVolumeDescriptorsIgnoreDates(a, b *primaryVolumeDescriptor) []string {
	now := time.Now()
	// copy values so we do not mess up the originals
	c := &primaryVolumeDescriptor{}
	d := &primaryVolumeDescriptor{}
	*c = *a
	*d = *b

	// unify fields we let be equal
	c.creation = now
	d.creation = now
	c.effective = now
	d.effective = now
	c.modification = now
	d.modification = now
	c.expiration = now
	d.expiration = now
	cRootDirectoryEntry := &directoryEntry{}
	dRootDirectoryEntry := &directoryEntry{}
	*cRootDirectoryEntry = *c.rootDirectoryEntry
	*dRootDirectoryEntry = *d.rootDirectoryEntry
	cRootDirectoryEntry.creation = now
	dRootDirectoryEntry.creation = now
	c.rootDirectoryEntry = cRootDirectoryEntry
	d.rootDirectoryEntry = dRootDirectoryEntry

	// cannot actually compare root directory entry since can be pointers to different things
	// so we compare them separately, and then compare the rest
	if diff := deep.Equal(*c.rootDirectoryEntry, *d.rootDirectoryEntry); diff != nil {
		return diff
	}
	c.rootDirectoryEntry = nil
	d.rootDirectoryEntry = nil
	return deep.Equal(c, d)
}
func comparePrimaryVolumeDescriptorsBytesIgnoreDates(a, b []byte) bool {
	aNull := primaryVolumeDescriptorsBytesNullDate(a)
	bNull := primaryVolumeDescriptorsBytesNullDate(b)

	// we ignore the reserved areas that are unused
	return bytes.Equal(aNull[:883], bNull[:883])
}
func primaryVolumeDescriptorsBytesNullDate(a []byte) []byte {
	// null the volume dates
	dateLocations := []int{813, 830, 847, 864}
	length := 17
	now := make([]byte, length)
	a1 := make([]byte, len(a))
	copy(a1, a)
	for _, i := range dateLocations {
		copy(a1[i:i+length], now)
	}
	// null the root directory entry dates
	rootEntry := a1[156 : 156+34]
	r1 := make([]byte, len(rootEntry))
	copy(r1, rootEntry)
	copy(a1[156:156+34], directoryEntryBytesNullDate(r1))
	return a1
}

//nolint:deadcode,unused // this is unused, but is useful for understanding structures
func getValidVolumeDescriptors() ([]volumeDescriptor, []byte, error) {
	blocksize := uint16(2048)
	// read correct bytes off of disk
	b, err := os.ReadFile(volRecordsFile)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading data from volrecords test fixture %s: %v", volRecordsFile, err)
	}

	// sector 0 - Primary Volume Descriptor
	// sector 1 - Boot Volume Descriptor
	// sector 2 - Supplemental Volume Descriptor
	// sector 3 - Volume Descriptor Set Terminator

	t1 := time.Now()
	entries := []volumeDescriptor{
		&primaryVolumeDescriptor{
			systemIdentifier:           fmt.Sprintf("%32v", ""),
			volumeIdentifier:           "Ubuntu-Server 18.04.1 LTS amd64 ",
			volumeSize:                 415744, // in bytes
			setSize:                    1,
			sequenceNumber:             1,
			blocksize:                  blocksize,
			pathTableSize:              972,
			pathTableLLocation:         114,
			pathTableLOptionalLocation: 0,
			pathTableMLocation:         115,
			pathTableMOptionalLocation: 0,
			rootDirectoryEntry:         &directoryEntry{},
			volumeSetIdentifier:        fmt.Sprintf("%128v", ""),
			publisherIdentifier:        fmt.Sprintf("%128v", ""),
			preparerIdentifier:         fmt.Sprintf("%-128v", "XORRISO-1.2.4 2012.07.20.130001, LIBISOBURN-1.2.4, LIBISOFS-1.2.4, LIBBURN-1.2.4"),
			applicationIdentifier:      fmt.Sprintf("%128v", ""),
			copyrightFile:              fmt.Sprintf("%37v", ""),
			abstractFile:               fmt.Sprintf("%37v", ""),
			bibliographicFile:          fmt.Sprintf("%37v", ""),
			creation:                   t1,
			modification:               t1,
			expiration:                 t1,
			effective:                  t1,
		},
		&bootVolumeDescriptor{
			location: 0x71,
		},
		&supplementaryVolumeDescriptor{
			systemIdentifier:           fmt.Sprintf("%32v", ""),
			volumeIdentifier:           "Ubuntu-Server 18",
			volumeSize:                 415744, // in bytes
			escapeSequences:            []byte{0x25, 0x2F, 0x45},
			setSize:                    1,
			sequenceNumber:             1,
			blocksize:                  blocksize,
			pathTableSize:              1386,
			pathTableLLocation:         190,
			pathTableLOptionalLocation: 0,
			pathTableMLocation:         191,
			pathTableMOptionalLocation: 0,
			rootDirectoryEntry:         &directoryEntry{},
			volumeSetIdentifier:        fmt.Sprintf("%128v", ""),
			publisherIdentifier:        fmt.Sprintf("%128v", ""),
			preparerIdentifier:         fmt.Sprintf("%-128v", "XORRISO-1.2.4 2012.07.20.130001, LIBISOBURN-1.2.4, LIBISOFS-1.2."),
			applicationIdentifier:      fmt.Sprintf("%128v", ""),
			copyrightFile:              fmt.Sprintf("%37v", ""),
			abstractFile:               fmt.Sprintf("%37v", ""),
			bibliographicFile:          fmt.Sprintf("%37v", ""),
			creation:                   t1,
			modification:               t1,
			expiration:                 t1,
			effective:                  t1,
		},
		&terminatorVolumeDescriptor{},
	}

	return entries, b, nil
}

func get9660PrimaryVolumeDescriptor() (*primaryVolumeDescriptor, []byte, error) {
	// these are taken from the file ./testdata/fat32.img, see ./testdata/README.md
	blocksize := 2048
	pvdSector := 16
	t1, _ := time.Parse(time.RFC3339, "2017-11-26T07:53:16Z")

	// read correct bytes off of disk
	input, err := os.ReadFile(ISO9660File)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading data from iso9660 test fixture %s: %v", ISO9660File, err)
	}

	start := pvdSector * blocksize // PVD sector

	// five blocks, since we know it is five blocks
	allBytes := input[start : start+blocksize]

	pvd := &primaryVolumeDescriptor{
		systemIdentifier:           fmt.Sprintf("%32v", ""),
		volumeIdentifier:           "ISOIMAGE                        ",
		volumeSize:                 5386, // in bytes
		setSize:                    1,
		sequenceNumber:             1,
		blocksize:                  2048,
		pathTableSize:              168,
		pathTableLLocation:         35,
		pathTableLOptionalLocation: 0,
		pathTableMLocation:         36,
		pathTableMOptionalLocation: 0,
		rootDirectoryEntry:         &directoryEntry{},
		volumeSetIdentifier:        fmt.Sprintf("%128v", ""),
		publisherIdentifier:        fmt.Sprintf("%128v", ""),
		preparerIdentifier:         fmt.Sprintf("%-128v", "XORRISO-1.4.8 2017.09.12.143001, LIBISOBURN-1.4.8, LIBISOFS-1.4.8, LIBBURN-1.4.8"),
		applicationIdentifier:      fmt.Sprintf("%128v", ""),
		copyrightFile:              fmt.Sprintf("%37v", ""),
		abstractFile:               fmt.Sprintf("%37v", ""),
		bibliographicFile:          fmt.Sprintf("%37v", ""),
		creation:                   t1,
		modification:               t1,
		expiration:                 t1,
		effective:                  t1,
	}
	// we need the root directoryEntry
	rootDirEntry := &directoryEntry{
		extAttrSize:              0,
		location:                 0x12,
		size:                     0x800,
		creation:                 t1,
		isHidden:                 false,
		isSubdirectory:           true,
		isAssociated:             false,
		hasExtendedAttrs:         false,
		hasOwnerGroupPermissions: false,
		hasMoreEntries:           false,
		volumeSequence:           1,
		filename:                 "",
		isSelf:                   true,
		filesystem:               nil,
	}
	pvd.rootDirectoryEntry = rootDirEntry
	return pvd, allBytes, nil
}

func TestDecBytesToTime(t *testing.T) {
	for _, tt := range timeDecBytesTests {
		output, err := decBytesToTime(tt.b)
		if err != nil {
			t.Fatalf("error parsing actual date: %v", err)
		}
		expected, err := time.Parse(time.RFC3339, tt.rfc)
		if err != nil {
			t.Fatalf("error parsing expected date: %v", err)
		}
		if !expected.Equal(output) {
			t.Errorf("decBytesToTime(%d) expected output %v, actual %v", tt.b, expected, output)
		}
	}
}

func TestTimeToDecBytes(t *testing.T) {
	for _, tt := range timeDecBytesTests {
		input, err := time.Parse(time.RFC3339, tt.rfc)
		if err != nil {
			t.Fatalf("error parsing input date: %v", err)
		}
		b := timeToDecBytes(input)
		if !bytes.Equal(b, tt.b) {
			t.Errorf("timeToBytes(%v) expected then actual \n% x\n% x", tt.rfc, tt.b, b)
		}
	}
}

func TestPrimaryVolumeDescriptorToBytes(t *testing.T) {
	validPvd, validBytes, err := get9660PrimaryVolumeDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	b := validPvd.toBytes()
	if !comparePrimaryVolumeDescriptorsBytesIgnoreDates(b, validBytes) {
		t.Errorf("Mismatched bytes, actual vs expected")
		t.Log(b)
		t.Log(validBytes)
	}
}
func TestParsePrimaryVolumeDescriptor(t *testing.T) {
	validPvd, validBytes, err := get9660PrimaryVolumeDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	pvd, err := parsePrimaryVolumeDescriptor(validBytes)
	if err != nil {
		t.Fatalf("error parsing primary volume descriptor: %v", err)
	}
	if diff := comparePrimaryVolumeDescriptorsIgnoreDates(pvd, validPvd); diff != nil {
		t.Errorf("Mismatched primary volume descriptor, actual vs expected")
		t.Log(diff)
	}
}
func TestPrimaryVolumeDescriptorType(t *testing.T) {
	pvd := &primaryVolumeDescriptor{}
	if pvd.Type() != volumeDescriptorPrimary {
		t.Errorf("Primary Volume Descriptor type was %v instead of expected %v", pvd.Type(), volumeDescriptorPrimary)
	}
}
