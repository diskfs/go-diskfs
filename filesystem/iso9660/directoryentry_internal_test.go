package iso9660

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	timeBytesTests = []struct {
		b   []byte
		rfc string
	}{
		// see reference at https://wiki.osdev.org/ISO_9660#Directories
		{[]byte{80, 1, 2, 14, 35, 36, 0}, "1980-01-02T14:35:36+00:00"},
		{[]byte{95, 11, 25, 0, 16, 7, 8}, "1995-11-25T00:16:07+02:00"},
		{[]byte{101, 6, 30, 12, 0, 0, 0xe6}, "2001-06-30T12:00:00-06:30"},
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
	c.creation = now
	d.creation = now
	return *c == *d
}
func directoryEntryBytesNullDate(a []byte) []byte {
	now := make([]byte, 7, 7)
	a1 := make([]byte, len(a))
	copy(a1[18:18+7], now)
	return a1
}

func getValidDirectoryEntries(f *FileSystem) ([]*directoryEntry, []int, [][]byte, []byte, error) {
	blocksize := 2048
	rootSector := 18
	// read correct bytes off of disk
	input, err := ioutil.ReadFile(ISO9660File)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Error reading data from iso9660 test fixture %s: %v", ISO9660File, err)
	}

	// start of root directory in file.iso - sector 18
	// sector 0-15 - system area
	// sector 16 - Primary Volume Descriptor
	// sector 17 - Volume Descriptor Set Terimnator
	// sector 18 - / (root) directory
	// sector 19 -
	// sector 20 - /abc directory
	// sector 21 - /bar directory
	// sector 22 - /foo directory
	// sector 23 - /foo directory
	// sector 24 - /foo directory
	// sector 25 - /foo directory
	// sector 26 - /foo directory
	// sector 27 - L path table
	// sector 28 - M path table
	// sector 33-2592 - /ABC/LARGEFILE
	// sector 2593-5152 - /BAR/LARGEFILE
	// sector 5153 - /FOO/FILENA01
	//  ..
	// sector 5228 - /FOO/FILENA75
	// sector 5229 - /README.MD
	start := rootSector * blocksize // start of root directory in file.iso

	// one block, since we know it is just one block
	allBytes := input[start : start+blocksize]

	b := make([][]byte, 0, 8)

	t1 := time.Now()
	sizes := []int{0x84, 0x60, 0x6a, 0x6a, 0x6a, 0x78}
	entries := []*directoryEntry{
		&directoryEntry{
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
			filesystem:               f,
		},
		&directoryEntry{
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
			isParent:                 true,
			filesystem:               f,
		},
		&directoryEntry{
			extAttrSize:              0,
			location:                 0x14,
			size:                     0x800,
			creation:                 t1,
			isHidden:                 false,
			isSubdirectory:           true,
			isAssociated:             false,
			hasExtendedAttrs:         false,
			hasOwnerGroupPermissions: false,
			hasMoreEntries:           false,
			volumeSequence:           1,
			filename:                 "ABC",
			filesystem:               f,
		},
		&directoryEntry{
			extAttrSize:              0,
			location:                 0x15,
			size:                     0x800,
			creation:                 t1,
			isHidden:                 false,
			isSubdirectory:           true,
			isAssociated:             false,
			hasExtendedAttrs:         false,
			hasOwnerGroupPermissions: false,
			hasMoreEntries:           false,
			volumeSequence:           1,
			filename:                 "BAR",
			filesystem:               f,
		},
		&directoryEntry{
			extAttrSize:              0,
			location:                 0x16,
			size:                     0x2800,
			creation:                 t1,
			isHidden:                 false,
			isSubdirectory:           true,
			isAssociated:             false,
			hasExtendedAttrs:         false,
			hasOwnerGroupPermissions: false,
			hasMoreEntries:           false,
			volumeSequence:           1,
			filename:                 "FOO",
			filesystem:               f,
		},
		&directoryEntry{
			extAttrSize:              0,
			location:                 0x146d,
			size:                     0x3ea,
			creation:                 t1,
			isHidden:                 false,
			isSubdirectory:           false,
			isAssociated:             false,
			hasExtendedAttrs:         false,
			hasOwnerGroupPermissions: false,
			hasMoreEntries:           false,
			volumeSequence:           1,
			filename:                 "README.MD;1",
			filesystem:               f,
		},
	}

	read := 0
	for _ = range entries {
		recordSize := int(allBytes[read])
		// do we have a 0 point? if so, move ahead until we pass it at the end of the block
		if recordSize == 0x00 {
			read += (blocksize - read%blocksize)
		}
		b = append(b, allBytes[read:read+recordSize])
		read += recordSize
	}

	return entries, sizes, b, allBytes, nil
}

func getValidDirectoryEntriesExtended(fs *FileSystem) ([]*directoryEntry, [][]byte, []byte, error) {
	// these are taken from the file ./testdata/fat32.img, see ./testdata/README.md
	blocksize := 2048
	fooSector := 22
	t1, _ := time.Parse(time.RFC3339, "2017-11-26T07:53:16Z")
	sizes := []int{0x60, 0x60, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a,
		0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a,
		0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a,
		0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a,
		0x7a, 0x7a, 0x7a, 0x7a, 0x7a}
	entries := []*directoryEntry{
		// recordSize, extAttrSize,location,size,creation,isHidden,isSubdirectory,isAssociated,hasExtendedAttrs,hasOwnerGroupPermissions,hasMoreEntries,volumeSequence,filename
		{extAttrSize: 0x0, location: 0x16, size: 0x2800, creation: t1, isHidden: false, isSubdirectory: true, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "", isSelf: true},
		{extAttrSize: 0x0, location: 0x12, size: 0x800, creation: t1, isHidden: false, isSubdirectory: true, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "", isParent: true},
		{extAttrSize: 0x0, location: 0x1421, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA00.;1"},
		{extAttrSize: 0x0, location: 0x1422, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA01.;1"},
		{extAttrSize: 0x0, location: 0x1423, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA02.;1"},
		{extAttrSize: 0x0, location: 0x1424, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA03.;1"},
		{extAttrSize: 0x0, location: 0x1425, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA04.;1"},
		{extAttrSize: 0x0, location: 0x1426, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA05.;1"},
		{extAttrSize: 0x0, location: 0x1427, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA06.;1"},
		{extAttrSize: 0x0, location: 0x1428, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA07.;1"},
		{extAttrSize: 0x0, location: 0x1429, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA08.;1"},
		{extAttrSize: 0x0, location: 0x142a, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA09.;1"},
		{extAttrSize: 0x0, location: 0x142b, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA10.;1"},
		{extAttrSize: 0x0, location: 0x142c, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA11.;1"},
		{extAttrSize: 0x0, location: 0x142d, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA12.;1"},
		{extAttrSize: 0x0, location: 0x142e, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA13.;1"},
		{extAttrSize: 0x0, location: 0x142f, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA14.;1"},
		{extAttrSize: 0x0, location: 0x1430, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA15.;1"},
		{extAttrSize: 0x0, location: 0x1431, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA16.;1"},
		{extAttrSize: 0x0, location: 0x1432, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA17.;1"},
		{extAttrSize: 0x0, location: 0x1433, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA18.;1"},
		{extAttrSize: 0x0, location: 0x1434, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA19.;1"},
		{extAttrSize: 0x0, location: 0x1435, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA20.;1"},
		{extAttrSize: 0x0, location: 0x1436, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA21.;1"},
		{extAttrSize: 0x0, location: 0x1437, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA22.;1"},
		{extAttrSize: 0x0, location: 0x1438, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA23.;1"},
		{extAttrSize: 0x0, location: 0x1439, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA24.;1"},
		{extAttrSize: 0x0, location: 0x143a, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA25.;1"},
		{extAttrSize: 0x0, location: 0x143b, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA26.;1"},
		{extAttrSize: 0x0, location: 0x143c, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA27.;1"},
		{extAttrSize: 0x0, location: 0x143d, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA28.;1"},
		{extAttrSize: 0x0, location: 0x143e, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA29.;1"},
		{extAttrSize: 0x0, location: 0x143f, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA30.;1"},
		{extAttrSize: 0x0, location: 0x1440, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA31.;1"},
		{extAttrSize: 0x0, location: 0x1441, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA32.;1"},
		{extAttrSize: 0x0, location: 0x1442, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA33.;1"},
		{extAttrSize: 0x0, location: 0x1443, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA34.;1"},
		{extAttrSize: 0x0, location: 0x1444, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA35.;1"},
		{extAttrSize: 0x0, location: 0x1445, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA36.;1"},
		{extAttrSize: 0x0, location: 0x1446, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA37.;1"},
		{extAttrSize: 0x0, location: 0x1447, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA38.;1"},
		{extAttrSize: 0x0, location: 0x1448, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA39.;1"},
		{extAttrSize: 0x0, location: 0x1449, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA40.;1"},
		{extAttrSize: 0x0, location: 0x144a, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA41.;1"},
		{extAttrSize: 0x0, location: 0x144b, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA42.;1"},
		{extAttrSize: 0x0, location: 0x144c, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA43.;1"},
		{extAttrSize: 0x0, location: 0x144d, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA44.;1"},
		{extAttrSize: 0x0, location: 0x144e, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA45.;1"},
		{extAttrSize: 0x0, location: 0x144f, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA46.;1"},
		{extAttrSize: 0x0, location: 0x1450, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA47.;1"},
		{extAttrSize: 0x0, location: 0x1451, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA48.;1"},
		{extAttrSize: 0x0, location: 0x1452, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA49.;1"},
		{extAttrSize: 0x0, location: 0x1453, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA50.;1"},
		{extAttrSize: 0x0, location: 0x1454, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA51.;1"},
		{extAttrSize: 0x0, location: 0x1455, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA52.;1"},
		{extAttrSize: 0x0, location: 0x1456, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA53.;1"},
		{extAttrSize: 0x0, location: 0x1457, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA54.;1"},
		{extAttrSize: 0x0, location: 0x1458, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA55.;1"},
		{extAttrSize: 0x0, location: 0x1459, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA56.;1"},
		{extAttrSize: 0x0, location: 0x145a, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA57.;1"},
		{extAttrSize: 0x0, location: 0x145b, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA58.;1"},
		{extAttrSize: 0x0, location: 0x145c, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA59.;1"},
		{extAttrSize: 0x0, location: 0x145d, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA60.;1"},
		{extAttrSize: 0x0, location: 0x145e, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA61.;1"},
		{extAttrSize: 0x0, location: 0x145f, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA62.;1"},
		{extAttrSize: 0x0, location: 0x1460, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA63.;1"},
		{extAttrSize: 0x0, location: 0x1461, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA64.;1"},
		{extAttrSize: 0x0, location: 0x1462, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA65.;1"},
		{extAttrSize: 0x0, location: 0x1463, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA66.;1"},
		{extAttrSize: 0x0, location: 0x1464, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA67.;1"},
		{extAttrSize: 0x0, location: 0x1465, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA68.;1"},
		{extAttrSize: 0x0, location: 0x1466, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA69.;1"},
		{extAttrSize: 0x0, location: 0x1467, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA70.;1"},
		{extAttrSize: 0x0, location: 0x1468, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA71.;1"},
		{extAttrSize: 0x0, location: 0x1469, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA72.;1"},
		{extAttrSize: 0x0, location: 0x146a, size: 0xc, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA73.;1"},
		{extAttrSize: 0x0, location: 0x146b, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA74.;1"},
		{extAttrSize: 0x0, location: 0x146c, size: 0xb, creation: t1, isHidden: false, isSubdirectory: false, isAssociated: false, hasExtendedAttrs: false, hasOwnerGroupPermissions: false, hasMoreEntries: false, volumeSequence: 0x1, filename: "FILENA75.;1"},
	}

	for _, e := range entries {
		e.filesystem = fs
	}
	// read correct bytes off of disk
	input, err := ioutil.ReadFile(ISO9660File)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Error reading data from iso9660 test fixture %s: %v", ISO9660File, err)
	}

	start := fooSector * blocksize // start of /foo directory in file.iso

	// five blocks, since we know it is five blocks
	allBytes := input[start : start+5*blocksize]

	b := make([][]byte, 0, len(entries))
	read := 0
	for i := range entries {
		recordSize := sizes[i]
		// do we have a 0 point? if so, move ahead until we pass it at the end of the block
		if allBytes[read] == 0x00 {
			read += (blocksize - read%blocksize)
		}
		b = append(b, allBytes[read:read+recordSize])
		read += recordSize
	}
	return entries, b, allBytes, nil
}

func TestBytesToTime(t *testing.T) {
	for _, tt := range timeBytesTests {
		output := bytesToTime(tt.b)
		expected, err := time.Parse(time.RFC3339, tt.rfc)
		if err != nil {
			t.Fatalf("Error parsing expected date: %v", err)
		}
		if !expected.Equal(output) {
			t.Errorf("bytesToTime(%d) expected output %v, actual %v", tt.b, expected, output)
		}
	}
}

func TestTimeToBytes(t *testing.T) {
	for _, tt := range timeBytesTests {
		input, err := time.Parse(time.RFC3339, tt.rfc)
		if err != nil {
			t.Fatalf("Error parsing input date: %v", err)
		}
		b := timeToBytes(input)
		if bytes.Compare(b, tt.b) != 0 {
			t.Errorf("timeToBytes(%v) expected output %x, actual %x", tt.rfc, tt.b, b)
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
		{"abcdef\u2318", nil, fmt.Errorf("Non-ASCII character in name: %s", "abcdef\u2318")},
	}
	for _, tt := range tests {
		output, err := stringToASCIIBytes(tt.input)
		if bytes.Compare(output, tt.output) != 0 {
			t.Errorf("stringToASCIIBytes(%s) expected output %v, actual %v", tt.input, tt.output, output)
		}
		if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())) {
			t.Errorf("mismatched err expected, actual: %v, %v", tt.err, err)
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
		{"A..-a*)82y12112bb", "A_A__82Y12112BB"},
	}
	for _, tt := range tests {
		output := uCaseValid(tt.input)
		if output != tt.output {
			t.Errorf("uCaseValid(%s) expected %s actual %s", tt.input, tt.output, output)
		}
	}
}

func TestDirectoryEntryParseDirEntries(t *testing.T) {
	fs := &FileSystem{blocksize: 2048}
	validDe, _, _, b, err := getValidDirectoryEntries(fs)
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
		output, err := parseDirEntries(tt.b, fs)
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
				if !compareDirectoryEntriesIgnoreDates(de, tt.de[i]) {
					t.Errorf("%d: parseDirEntries() DirectoryEntry mismatch, actual then valid:", i)
					t.Logf("%#v\n", de)
					t.Logf("%#v\n", tt.de[i])
				}
			}
		}
	}

}

func TestDirectoryEntryToBytes(t *testing.T) {
	validDe, sizes, validBytes, _, err := getValidDirectoryEntries(nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, de := range validDe {
		b, err := de.toBytes()
		switch {
		case err != nil:
			t.Errorf("Error converting directory entry to bytes: %v", err)
			t.Logf("%v", de)
		case int(b[0]) != len(b):
			t.Errorf("Reported size as %d but had %d bytes", b[0], len(b))
		default:
			// set the byte sizes to what we expect from disk
			b[0] = uint8(sizes[i])
			if bytes.Compare(directoryEntryBytesNullDate(b), directoryEntryBytesNullDate(validBytes[i][:len(b)])) != 0 {
				t.Errorf("Mismatched bytes %s, actual vs expected", de.filename)
				t.Log(b)
				t.Log(validBytes[i])
			}
		}
	}
}

func TestDirectoryEntryGetLocation(t *testing.T) {
	// directoryEntryGetLocation(p string) (uint32, uint32, error) {
	tests := []struct {
		input  string
		output uint32
		err    error
	}{
		{"/", 18, nil},
		{"/ABC", 20, nil},
		{"/FOO", 22, nil},
		{"/NOTHERE", 0, nil},
	}

	f, err := os.Open(ISO9660File)
	if err != nil {
		t.Fatalf("Could not open iso testing file %s: %v", ISO9660File, err)
	}
	// the root directory entry
	root := &directoryEntry{
		extAttrSize:              0,
		location:                 0x12,
		size:                     0x800,
		creation:                 time.Now(),
		isHidden:                 false,
		isSubdirectory:           true,
		isAssociated:             false,
		hasExtendedAttrs:         false,
		hasOwnerGroupPermissions: false,
		hasMoreEntries:           false,
		volumeSequence:           1,
		filename:                 string(0x00),
		filesystem:               &FileSystem{blocksize: 2048, file: f},
	}

	for _, tt := range tests {
		// root directory entry needs a filesystem or this will error out
		output, _, err := root.getLocation(tt.input)
		if output != tt.output {
			t.Errorf("directoryEntry.getLocation(%s) expected output %d, actual %d", tt.input, tt.output, output)
		}
		if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())) {
			t.Errorf("mismatched err expected, actual: %v, %v", tt.err, err)
		}
	}
}
