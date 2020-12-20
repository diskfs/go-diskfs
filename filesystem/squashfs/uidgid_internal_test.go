package squashfs

import "testing"

func TestParseIDTable(t *testing.T) {
	data := []byte{
		0x0, 0x0, 0x0, 0x0,
		0xa, 0x0, 0x0, 0x0,
		0x1, 0x0, 0x2, 0x2c,
		0xe6, 0x2a, 0x85, 0x7f,
	}
	expected := []uint32{0, 10, 0x2c020001, 0x7f852ae6}
	uidsgids := parseIDTable(data)
	if !testEqualUint32Slice(uidsgids, expected) {
		t.Errorf("Mismatched, actual then expected")
		t.Logf("%x", uidsgids)
		t.Logf("%x", expected)
	}
}
