package iso9660

import (
	"testing"
)

// TestDirectoryEntriesFromBytes largely a duplicate of TestdirectoryEntryParseDirEntries
// it just loads it into the Directory structure
func TestDirectoryEntriesFromBytes(t *testing.T) {
	fs := &FileSystem{blocksize: 2048}
	validDe, _, _, b, err := getValidDirectoryEntries(fs)
	if err != nil {
		t.Fatal(err)
	}

	d := &Directory{}
	err = d.entriesFromBytes(b, fs)
	switch {
	case err != nil:
		t.Errorf("Unexpected non-nil error: %v", err)
	case d.entries == nil:
		t.Errorf("unexpected nil entries")
	case len(d.entries) != len(validDe):
		t.Errorf("mismatched entries length actual %d vs expected %d", len(d.entries), len(validDe))
	default:
		// run through them and see that they match
		for i, de := range d.entries {
			if !compareDirectoryEntriesIgnoreDates(de, validDe[i]) {
				t.Errorf("%d: directoryEntry mismatch, actual then valid:", i)
				t.Logf("%#v\n", de)
				t.Logf("%#v\n", validDe[i])
			}
		}
	}

}

func TestDirectoryEntriesToBytes(t *testing.T) {
	blocksize := 2048
	validDe, _, _, b, err := getValidDirectoryEntries(nil)
	if err != nil {
		t.Fatal(err)
	}
	d := &Directory{
		entries: validDe,
		directoryEntry: directoryEntry{
			filesystem: &FileSystem{
				blocksize: int64(blocksize),
			},
		},
	}
	output, err := d.entriesToBytes()
	// null the date bytes out
	if err != nil {
		t.Fatalf("unexpected non-nil error: %v", err)
	}
	// cannot directly compare the bytes as of yet, since the original contains all sorts of system area stuff
	output = clearDatesDirectoryBytes(output, blocksize)
	output = clearSuspDirectoryBytes(output, blocksize)
	b = clearDatesDirectoryBytes(b, blocksize)
	b = clearSuspDirectoryBytes(b, blocksize)
	switch {
	case output == nil:
		t.Errorf("unexpected nil bytes")
	case len(output) == 0:
		t.Errorf("unexpected 0 length byte slice")
	case len(output) != len(b):
		t.Errorf("mismatched byte slice length actual %d, expected %d", len(output), len(b))
	case len(output)%blocksize != 0:
		t.Errorf("output size was %d which is not a perfect multiple of %d", len(output), blocksize)
	}
}

func clearDatesDirectoryBytes(b []byte, blocksize int) []byte {
	if b == nil {
		return b
	}
	nullBytes := make([]byte, 7, 7)
	for i := 0; i < len(b); {
		// get the length of the current record
		dirlen := int(b[i])
		if dirlen == 0 {
			i += blocksize - blocksize%i
			continue
		}
		copy(b[i+18:i+18+7], nullBytes)
		i += dirlen
	}
	return b
}
func clearSuspDirectoryBytes(b []byte, blocksize int) []byte {
	if b == nil {
		return b
	}
	for i := 0; i < len(b); {
		// get the length of the current record
		dirlen := int(b[i+0])
		namelen := int(b[i+32])
		if dirlen == 0 {
			i += blocksize - blocksize%i
			continue
		}
		if namelen%2 == 0 {
			namelen++
		}
		nullByteStart := 33 + namelen
		nullByteLen := dirlen - nullByteStart
		nullBytes := make([]byte, nullByteLen, nullByteLen)
		copy(b[i+nullByteStart:i+nullByteStart+nullByteLen], nullBytes)
		i += dirlen
	}
	return b
}
