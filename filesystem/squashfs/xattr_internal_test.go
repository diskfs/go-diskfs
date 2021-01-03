package squashfs

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseXAttrIndex(t *testing.T) {
	tests := []struct {
		b   []byte
		x   *xAttrIndex
		err error
	}{
		{[]byte{0x0, 0x1}, nil, fmt.Errorf("Cannot parse xAttr Index of size %d less than minimum %d", 2, xAttrIDEntrySize)},
		{[]byte{
			0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7,
			0x8, 0x9, 0xa, 0xb,
			0xc, 0xd, 0xe, 0xf,
			0x10, 0x11, 0x12, 0x13, 0x14},
			&xAttrIndex{
				pos:   0x0706050403020100,
				count: 0x0b0a0908,
				size:  0x0f0e0d0c,
			}, nil},
	}
	for i, tt := range tests {
		x, err := parseXAttrIndex(tt.b)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case (x == nil && tt.x != nil) || (x != nil && tt.x == nil) || (x != nil && tt.x != nil && *x != *tt.x):
			t.Errorf("%d: mismatched xAttrIndex, actual then expected", i)
			t.Logf("%v", *x)
			t.Logf("%v", *tt.x)
		}

	}
}

func TestXAttrTableFind(t *testing.T) {
	x := &xAttrTable{
		list: []*xAttrIndex{
			{pos: 0, count: 2, size: 10},
			{pos: 32, count: 1, size: 8},
		},
		data: []byte{
			0, 0,
			3, 0,
			65, 66, 67,
			6, 0, 0, 0,
			68, 69, 70, 71, 72, 73,
			0, 0,
			3, 0,
			75, 76, 77,
			4, 0, 0, 0,
			78, 79, 80, 81,
			0, 0,
			4, 0,
			70, 71, 72, 73,
			2, 0, 0, 0,
			75, 76},
	}
	tests := []struct {
		pos    int
		xattrs map[string]string
		err    error
	}{
		{5, nil, fmt.Errorf("Position %d is greater than list size %d", 5, len(x.list))},
		{0, map[string]string{"ABC": "DEFGHI", "KLM": "NOPQ"}, nil},
		{1, map[string]string{"FGHI": "KL"}, nil},
	}
	for i, tt := range tests {
		xattrs, err := x.find(tt.pos)
		switch {
		case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
			t.Errorf("%d: mismatched error, actual then expected", i)
			t.Logf("%v", err)
			t.Logf("%v", tt.err)
		case !CompareEqualMapStringString(xattrs, tt.xattrs):
			t.Errorf("%d: mismatched data, actual then expected", i)
			t.Logf("%v", xattrs)
			t.Logf("%v", tt.xattrs)
		}
	}
}
