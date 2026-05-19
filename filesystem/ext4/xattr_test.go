package ext4

import (
	"encoding/binary"
	"testing"
)

func TestParseXattrEntries(t *testing.T) {
	tests := []struct {
		name     string
		entries  []byte
		values   []byte
		expected map[string]string
		wantErr  bool
	}{
		{
			name:     "empty entries",
			entries:  make([]byte, 16),
			values:   make([]byte, 256),
			expected: map[string]string{},
		},
		{
			name:    "single trusted xattr",
			entries: buildXattrEntry(xattrIndexTrusted, "overlay.opaque", []byte("y")),
			expected: map[string]string{
				"trusted.overlay.opaque": "y",
			},
		},
		{
			name:    "single security xattr",
			entries: buildXattrEntry(xattrIndexSecurity, "capability", []byte("cap_data")),
			expected: map[string]string{
				"security.capability": "cap_data",
			},
		},
		{
			name:    "single user xattr",
			entries: buildXattrEntry(xattrIndexUser, "mykey", []byte("myvalue")),
			expected: map[string]string{
				"user.mykey": "myvalue",
			},
		},
		{
			name:    "posix acl access (name_len=0)",
			entries: buildXattrEntry(xattrIndexPosixACLAccess, "", []byte("acl_data")),
			expected: map[string]string{
				"system.posix_acl_access": "acl_data",
			},
		},
		{
			name:    "posix acl default (name_len=0)",
			entries: buildXattrEntry(xattrIndexPosixACLDefault, "", []byte("acl_default")),
			expected: map[string]string{
				"system.posix_acl_default": "acl_default",
			},
		},
		{
			name:    "multiple entries",
			entries: buildMultiXattrEntries(),
			expected: map[string]string{
				"user.key1":     "val1",
				"trusted.key2":  "val2",
				"security.key3": "val3",
			},
		},
		{
			name:    "name too long for buffer",
			entries: buildTruncatedEntry(xattrIndexTrusted, 200),
			wantErr: true,
		},
		{
			name:    "ea_inode value returns error",
			entries: buildEAInodeEntry(xattrIndexUser, "big", 42),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := tt.values
			if values == nil {
				values = tt.entries
			}
			result, err := parseXattrEntries(tt.entries, values)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d entries, got %d: %v", len(tt.expected), len(result), result)
			}
			for k, want := range tt.expected {
				got, ok := result[k]
				if !ok {
					t.Errorf("missing key %q", k)
					continue
				}
				if string(got) != want {
					t.Errorf("key %q: got %q, want %q", k, string(got), want)
				}
			}
		})
	}
}

func TestParseBlockXattrEntries(t *testing.T) {
	blockSize := 4096
	block := make([]byte, blockSize)

	// Write xattr block header with valid magic.
	binary.LittleEndian.PutUint32(block[0:4], xattrMagic)

	// Write a single entry after the header.
	name := "myattr"
	nameLen := len(name)
	entryStart := xattrHeaderSize
	block[entryStart] = uint8(nameLen)
	block[entryStart+1] = xattrIndexUser

	// Place value at the end of the block.
	value := []byte("hello")
	valueOffset := blockSize - len(value)
	binary.LittleEndian.PutUint16(block[entryStart+2:entryStart+4], uint16(valueOffset))
	binary.LittleEndian.PutUint32(block[entryStart+4:entryStart+8], 0)
	binary.LittleEndian.PutUint32(block[entryStart+8:entryStart+12], uint32(len(value)))
	binary.LittleEndian.PutUint32(block[entryStart+12:entryStart+16], 0)
	copy(block[entryStart+xattrEntrySize:], name)
	copy(block[valueOffset:], value)

	// Parse using the block layout: entries start after header, values relative to block start.
	entryData := block[xattrHeaderSize:]
	result, err := parseXattrEntries(entryData, block)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}
	got, ok := result["user.myattr"]
	if !ok {
		t.Fatal("missing key user.myattr")
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", string(got), "hello")
	}
}

// buildXattrEntry creates a byte slice containing a single xattr entry
// where entries and values share the same buffer (ibody-style).
func buildXattrEntry(nameIndex uint8, name string, value []byte) []byte {
	nameLen := len(name)
	entryLen := xattrEntrySize + nameLen
	entryLenAligned := (entryLen + 3) &^ 3

	// Ensure there's a zero terminator entry after the real entry.
	minLen := entryLenAligned + xattrEntrySize
	valueOffset := minLen
	totalLen := valueOffset + len(value)
	if totalLen < minLen {
		totalLen = minLen
	}

	buf := make([]byte, totalLen)

	buf[0] = uint8(nameLen)
	buf[1] = nameIndex
	binary.LittleEndian.PutUint16(buf[2:4], uint16(valueOffset))
	binary.LittleEndian.PutUint32(buf[4:8], 0)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(value)))
	binary.LittleEndian.PutUint32(buf[12:16], 0)
	copy(buf[xattrEntrySize:xattrEntrySize+nameLen], name)

	copy(buf[valueOffset:], value)

	return buf
}

// buildMultiXattrEntries creates a buffer with three xattr entries.
func buildMultiXattrEntries() []byte {
	type entry struct {
		index uint8
		name  string
		value []byte
	}
	entries := []entry{
		{xattrIndexUser, "key1", []byte("val1")},
		{xattrIndexTrusted, "key2", []byte("val2")},
		{xattrIndexSecurity, "key3", []byte("val3")},
	}

	// First pass: calculate entry area size.
	entryAreaSize := 0
	for _, e := range entries {
		entryAreaSize += (xattrEntrySize + len(e.name) + 3) &^ 3
	}
	entryAreaSize += xattrEntrySize // zero terminator

	// Values follow the entry area.
	valueAreaStart := entryAreaSize
	totalValueSize := 0
	for _, e := range entries {
		totalValueSize += len(e.value)
	}

	buf := make([]byte, valueAreaStart+totalValueSize)
	pos := 0
	valuePos := valueAreaStart

	for _, e := range entries {
		nameLen := len(e.name)
		buf[pos] = uint8(nameLen)
		buf[pos+1] = e.index
		binary.LittleEndian.PutUint16(buf[pos+2:pos+4], uint16(valuePos))
		binary.LittleEndian.PutUint32(buf[pos+4:pos+8], 0)
		binary.LittleEndian.PutUint32(buf[pos+8:pos+12], uint32(len(e.value)))
		binary.LittleEndian.PutUint32(buf[pos+12:pos+16], 0)
		copy(buf[pos+xattrEntrySize:], e.name)
		copy(buf[valuePos:], e.value)
		pos = (pos + xattrEntrySize + nameLen + 3) &^ 3
		valuePos += len(e.value)
	}

	return buf
}

// buildTruncatedEntry creates an entry that claims a name longer than the buffer.
func buildTruncatedEntry(nameIndex uint8, nameLen int) []byte {
	buf := make([]byte, xattrEntrySize+4)
	buf[0] = uint8(nameLen)
	buf[1] = nameIndex
	return buf
}

// buildEAInodeEntry creates an entry with a non-zero e_value_inum (EA inode).
func buildEAInodeEntry(nameIndex uint8, name string, valueInum uint32) []byte {
	nameLen := len(name)
	entryLen := xattrEntrySize + nameLen
	entryLenAligned := (entryLen + 3) &^ 3
	buf := make([]byte, entryLenAligned+xattrEntrySize)

	buf[0] = uint8(nameLen)
	buf[1] = nameIndex
	binary.LittleEndian.PutUint16(buf[2:4], 0)
	binary.LittleEndian.PutUint32(buf[4:8], valueInum)
	binary.LittleEndian.PutUint32(buf[8:12], 100) // some size
	binary.LittleEndian.PutUint32(buf[12:16], 0)
	copy(buf[xattrEntrySize:xattrEntrySize+nameLen], name)

	return buf
}

func TestReadIbodyXattrs(t *testing.T) {
	// buildInodeBytes constructs raw inode bytes with optional inline xattrs.
	// extraIsize is placed at offset 128 (i_extra_isize field).
	// xattrData (if non-nil) is placed right after the extra inode fields,
	// prefixed with the xattr magic.
	buildInodeBytes := func(inodeSize int, extraIsize uint16, xattrData []byte) []byte {
		buf := make([]byte, inodeSize)
		if inodeSize > int(ext2InodeSize)+2 {
			binary.LittleEndian.PutUint16(buf[ext2InodeSize:ext2InodeSize+2], extraIsize)
		}
		if xattrData != nil {
			xattrStart := int(ext2InodeSize) + int(extraIsize)
			binary.LittleEndian.PutUint32(buf[xattrStart:xattrStart+4], xattrMagic)
			copy(buf[xattrStart+4:], xattrData)
		}
		return buf
	}

	tests := []struct {
		name      string
		inodeSize uint16
		inodeData []byte
		expected  map[string]string
	}{
		{
			name:      "inode size 128 has no ibody xattrs",
			inodeSize: 128,
			inodeData: make([]byte, 128),
			expected:  nil,
		},
		{
			name:      "no xattr magic returns nil",
			inodeSize: 256,
			inodeData: make([]byte, 256),
			expected:  nil,
		},
		{
			name:      "valid ibody xattr",
			inodeSize: 256,
			inodeData: buildInodeBytes(256, 32, buildXattrEntry(xattrIndexUser, "foo", []byte("bar"))),
			expected: map[string]string{
				"user.foo": "bar",
			},
		},
		{
			name:      "extraIsize fills entire inode leaving no room",
			inodeSize: 256,
			inodeData: buildInodeBytes(256, 256-128, nil),
			expected:  nil,
		},
		{
			name:      "multiple ibody xattrs",
			inodeSize: 512,
			inodeData: buildInodeBytes(512, 32, buildMultiXattrEntries()),
			expected: map[string]string{
				"user.key1":     "val1",
				"trusted.key2":  "val2",
				"security.key3": "val3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &FileSystem{
				superblock: &superblock{
					inodeSize: tt.inodeSize,
				},
			}
			result, err := fs.readIbodyXattrs(tt.inodeData)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %v", result)
				}
				return
			}
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d entries, got %d: %v", len(tt.expected), len(result), result)
			}
			for k, want := range tt.expected {
				got, ok := result[k]
				if !ok {
					t.Errorf("missing key %q", k)
					continue
				}
				if string(got) != want {
					t.Errorf("key %q: got %q, want %q", k, string(got), want)
				}
			}
		})
	}
}
