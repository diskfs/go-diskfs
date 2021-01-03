package squashfs

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// the data for these tests is taken from an actual mksqaushfs; just do without compression to read with xxd
var testFragmentEntries = []struct {
	b     []byte
	entry *fragmentEntry
	err   error
}{
	{[]byte{0x1, 0x2}, nil, fmt.Errorf("Mismatched fragment entry size, received %d bytes, less than minimum %d", 2, 16)},
	{[]byte{0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}, &fragmentEntry{size: 7, start: 0x60, compressed: false}, nil},
	{[]byte{0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, &fragmentEntry{size: 7, start: 0x60, compressed: true}, nil},
}

func TestParseFragmentEntry(t *testing.T) {
	for i, tt := range testFragmentEntries {
		entry, err := parseFragmentEntry(tt.b)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case (entry == nil && tt.entry != nil) || (entry != nil && tt.entry == nil) || (entry != nil && tt.entry != nil && *entry != *tt.entry):
			t.Errorf("%d: mismatched header, actual then expected", i)
			t.Logf("%v", entry)
			t.Logf("%v", tt.entry)
		}
	}
}

func TestFragmentEntryToBytes(t *testing.T) {
	for i, tt := range testFragmentEntries {
		if tt.entry == nil {
			continue
		}
		b := tt.entry.toBytes()
		if bytes.Compare(b, tt.b) != 0 {
			t.Errorf("%d: mismatched bytes, actual then expected", i)
			t.Logf("%v", b)
			t.Logf("%v", tt.b)
		}
	}
}
