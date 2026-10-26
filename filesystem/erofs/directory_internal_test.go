package erofs

import (
	"encoding/binary"
	"strings"
	"testing"
)

func validDirectoryBlock() []byte {
	b := make([]byte, 17)
	binary.LittleEndian.PutUint64(b[0:], 1)
	binary.LittleEndian.PutUint16(b[8:], 12)
	b[10] = fileTypeRegular
	copy(b[12:], "hello")
	return b
}

func TestParseDirectory(t *testing.T) {
	entries, err := parseDirectory(validDirectoryBlock())
	if err != nil {
		t.Fatalf("parseDirectory() error = %v", err)
	}
	if len(entries) != 1 || entries[0].name != "hello" || entries[0].nid != 1 {
		t.Fatalf("parseDirectory() entries = %#v", entries)
	}
}

func TestParseDirectorySymlink(t *testing.T) {
	b := validDirectoryBlock()
	b[10] = fileTypeSymlink
	entries, err := parseDirectory(b)
	if err != nil {
		t.Fatalf("parseDirectory() error = %v", err)
	}
	if entries[0].fileType != fileTypeSymlink {
		t.Fatalf("fileType = %d, want %d", entries[0].fileType, fileTypeSymlink)
	}
}

func TestParseDirectoryErrors(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"first name offset", func(b []byte) { binary.LittleEndian.PutUint16(b[8:], 11) }},
		{"file type", func(b []byte) { b[10] = 0 }},
		{"reserved", func(b []byte) { b[11] = 1 }},
		{"unaligned name offset", func(b []byte) { binary.LittleEndian.PutUint16(b[8:], 13) }},
		{"traversal name", func(b []byte) { copy(b[12:], "../hi") }},
		{"nested name", func(b []byte) { copy(b[12:], "a/b") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := validDirectoryBlock()
			tt.mutate(b)
			if _, err := parseDirectory(b); err == nil {
				t.Fatal("parseDirectory() error = nil, want error")
			}
		})
	}
}

func TestParseDirectoryNameOrder(t *testing.T) {
	b := make([]byte, 28)
	binary.LittleEndian.PutUint64(b[0:], 1)
	binary.LittleEndian.PutUint16(b[8:], 24)
	b[10] = fileTypeRegular
	binary.LittleEndian.PutUint64(b[12:], 2)
	binary.LittleEndian.PutUint16(b[20:], 26)
	b[22] = fileTypeRegular
	copy(b[24:], "b")
	copy(b[26:], "a")
	if _, err := parseDirectory(b); err == nil {
		t.Fatal("parseDirectory() error = nil for unsorted names")
	}
}

func TestParseDirectoryMaximumName(t *testing.T) {
	b := make([]byte, direntSize+maxNameLength)
	binary.LittleEndian.PutUint64(b[0:], 1)
	binary.LittleEndian.PutUint16(b[8:], direntSize)
	b[10] = fileTypeRegular
	copy(b[direntSize:], strings.Repeat("a", maxNameLength))
	entries, err := parseDirectory(b)
	if err != nil {
		t.Fatalf("parseDirectory() error = %v", err)
	}
	if len(entries) != 1 || len(entries[0].name) != maxNameLength {
		t.Fatalf("parseDirectory() entries = %#v", entries)
	}
}

func TestParseDirectoryPaddedFinalName(t *testing.T) {
	b := make([]byte, 27)
	binary.LittleEndian.PutUint64(b[0:], 1)
	binary.LittleEndian.PutUint16(b[8:], 24)
	b[10] = fileTypeRegular
	binary.LittleEndian.PutUint64(b[12:], 2)
	binary.LittleEndian.PutUint16(b[20:], 25)
	b[22] = fileTypeRegular
	copy(b[24:], "ab")
	entries, err := parseDirectory(b)
	if err != nil {
		t.Fatalf("parseDirectory() error = %v", err)
	}
	if len(entries) != 2 || entries[0].name != "a" || entries[1].name != "b" {
		t.Fatalf("parseDirectory() entries = %#v", entries)
	}
}

func TestParseDirectoryNextNameOffsetOutsideBlock(t *testing.T) {
	b := make([]byte, 26)
	binary.LittleEndian.PutUint64(b[0:], 1)
	binary.LittleEndian.PutUint16(b[8:], 24)
	b[10] = fileTypeRegular
	binary.LittleEndian.PutUint64(b[12:], 2)
	binary.LittleEndian.PutUint16(b[20:], 65535)
	b[22] = fileTypeRegular
	copy(b[24:], "a")
	if _, err := parseDirectory(b); err == nil {
		t.Fatal("parseDirectory() error = nil for next name offset outside block")
	}
}
