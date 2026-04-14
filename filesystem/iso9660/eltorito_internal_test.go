package iso9660

import (
	"bytes"
	"testing"

	"github.com/diskfs/go-diskfs/partition/mbr"
	"github.com/diskfs/go-diskfs/version"
)

func TestElToritoGenerateCatalog(t *testing.T) {
	et := &ElTorito{
		BootCatalog:     "/boot.cat",
		HideBootCatalog: false,
		Entries: []*ElToritoEntry{
			{Platform: BIOS, Emulation: HardDiskEmulation, BootFile: "/abc.img", HideBootFile: false, LoadSegment: 23, SystemType: mbr.Linux, size: 10, location: 100},
			{Platform: BIOS, Emulation: NoEmulation, BootFile: "/def.img", HideBootFile: false, LoadSegment: 0, SystemType: mbr.Fat32LBA, size: 20, location: 200},
			{Platform: EFI, Emulation: NoEmulation, BootFile: "/qrs.img", HideBootFile: false, LoadSegment: 0, SystemType: mbr.Fat16, size: 30, location: 300},
		},
	}
	// the catalog should look like
	// - validation entry
	// - initial/default entry
	// - header+entry for each subsequent
	//
	// we are NOT testing the conversions here as we do them elsewhere

	e := make([]byte, 0)
	veBytes, err := et.validationEntry()
	if err != nil {
		t.Fatalf("unexpected error generating validation entry: %v", err)
	}
	e = append(e, veBytes...)
	e = append(e, et.Entries[0].entryBytes()...)
	e = append(e, et.Entries[1].headerBytes(false, 1)...)
	e = append(e, et.Entries[1].entryBytes()...)
	e = append(e, et.Entries[2].headerBytes(true, 1)...)
	e = append(e, et.Entries[2].entryBytes()...)

	b, err := et.generateCatalog()
	if err != nil {
		t.Fatalf("unexpected error generating catalog: %v", err)
	}
	if !bytes.Equal(b, e) {
		t.Errorf("Mismatched bytes, actual then expected\n% x\n% x\n", b, e)
	}
}

func TestElToritoValidationEntry(t *testing.T) {
	t.Run("with valid initial entry", func(t *testing.T) {
		et := &ElTorito{
			BootCatalog:     "/boot.cat",
			HideBootCatalog: false,
			Entries: []*ElToritoEntry{
				{Platform: EFI},
			},
		}
		b, err := et.validationEntry()
		if err != nil {
			t.Fatalf("unexpected error generating validation entry: %v", err)
		}
		e := make([]byte, 0x20)
		e[0] = 0x1
		e[1] = 0xef
		copy(e[4:0x1c], version.AppName)
		e[0x1e] = 0x55
		e[0x1f] = 0xaa

		// add the checksum - we calculated this manually
		e[0x1c] = 0x3c
		e[0x1d] = 0xd5
		if !bytes.Equal(b, e) {
			t.Errorf("Mismatched bytes, actual then expected\n% x\n% x\n", b, e)
		}
	})
	t.Run("missing initial entry", func(t *testing.T) {
		et := &ElTorito{
			BootCatalog:     "/boot.cat",
			HideBootCatalog: false,
		}
		_, err := et.validationEntry()
		if err == nil {
			t.Fatalf("expected error generating validation entry, got nil")
		}
	})
}

func TestElToritoHeaderBytes(t *testing.T) {
	var (
		boot = "/abc.img"
	)
	e := &ElToritoEntry{
		Platform:     BIOS,
		Emulation:    HardDiskEmulation,
		BootFile:     boot,
		HideBootFile: false,
		LoadSegment:  23,
		SystemType:   mbr.Linux,
	}
	tests := []struct {
		last     bool
		entries  uint16
		expected []byte
	}{
		{true, 1, []byte{0x91, byte(BIOS), 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
		{false, 1, []byte{0x90, byte(BIOS), 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
		{true, 25, []byte{0x91, byte(BIOS), 0x19, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
		{false, 36, []byte{0x90, byte(BIOS), 0x24, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
	}
	for _, tt := range tests {
		b := e.headerBytes(tt.last, tt.entries)
		if !bytes.Equal(b, tt.expected) {
			t.Errorf("last (%v), entries (%d): mismatched result, actual then expected\n% x\n% x\n", tt.last, tt.entries, b, tt.expected)
		}
	}
}

func TestElToritoEntryBytes(t *testing.T) {
	tests := []struct {
		name     string
		entry    *ElToritoEntry
		expected []byte
	}{
		{
			name: "bios emulation uses one sector",
			entry: &ElToritoEntry{
				Platform:     BIOS,
				Emulation:    HardDiskEmulation,
				BootFile:     "/abc.img",
				HideBootFile: false,
				LoadSegment:  23,
				SystemType:   mbr.Linux,
				size:         2450,
				location:     193,
			},
			expected: []byte{0x88, byte(HardDiskEmulation), 0x17, 0x0, byte(mbr.Linux), 0x0, 0x1, 0x0, 0xc1, 0x00},
		},
		{
			name: "bios no emulation defaults to four sectors",
			entry: &ElToritoEntry{
				Platform:     BIOS,
				Emulation:    NoEmulation,
				BootFile:     "/abc.img",
				HideBootFile: false,
				LoadSegment:  23,
				SystemType:   mbr.Linux,
				size:         32 * 1024 * 1024,
				location:     193,
			},
			expected: []byte{0x88, byte(NoEmulation), 0x17, 0x0, byte(mbr.Linux), 0x0, 0x4, 0x0, 0xc1, 0x00},
		},
		{
			name: "bios no emulation stays four sectors above 32 mib",
			entry: &ElToritoEntry{
				Platform:     BIOS,
				Emulation:    NoEmulation,
				BootFile:     "/abc.img",
				HideBootFile: false,
				LoadSegment:  23,
				SystemType:   mbr.Linux,
				size:         32*1024*1024 + 1,
				location:     193,
			},
			expected: []byte{0x88, byte(NoEmulation), 0x17, 0x0, byte(mbr.Linux), 0x0, 0x4, 0x0, 0xc1, 0x00},
		},
		{
			name: "efi uses actual size",
			entry: &ElToritoEntry{
				Platform:     EFI,
				Emulation:    NoEmulation,
				BootFile:     "/abc.img",
				HideBootFile: false,
				LoadSegment:  23,
				SystemType:   mbr.Linux,
				size:         2450,
				location:     193,
			},
			expected: []byte{0x88, byte(NoEmulation), 0x17, 0x0, byte(mbr.Linux), 0x0, 0x5, 0x0, 0xc1, 0x00},
		},
		{
			name: "efi over max size falls back to zero",
			entry: &ElToritoEntry{
				Platform:     EFI,
				Emulation:    NoEmulation,
				BootFile:     "/abc.img",
				HideBootFile: false,
				LoadSegment:  23,
				SystemType:   mbr.Linux,
				size:         uint32(elToritoMaxBlocks*512 + 1),
				location:     193,
			},
			expected: []byte{0x88, byte(NoEmulation), 0x17, 0x0, byte(mbr.Linux), 0x0, 0x0, 0x0, 0xc1, 0x00},
		},
		{
			name: "explicit load size wins",
			entry: &ElToritoEntry{
				Platform:     EFI,
				Emulation:    NoEmulation,
				BootFile:     "/abc.img",
				HideBootFile: false,
				LoadSegment:  23,
				SystemType:   mbr.Linux,
				size:         2450,
				location:     193,
			},
			expected: []byte{0x88, byte(NoEmulation), 0x17, 0x0, byte(mbr.Linux), 0x0, 0x9, 0x0, 0xc1, 0x00},
		},
		{
			name: "explicit zero load size wins",
			entry: &ElToritoEntry{
				Platform:     BIOS,
				Emulation:    NoEmulation,
				BootFile:     "/abc.img",
				HideBootFile: false,
				LoadSegment:  23,
				SystemType:   mbr.Linux,
				size:         2450,
				location:     193,
			},
			expected: []byte{0x88, byte(NoEmulation), 0x17, 0x0, byte(mbr.Linux), 0x0, 0x0, 0x0, 0xc1, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.name {
			case "explicit load size wins":
				tt.entry.SetLoadSize(9)
			case "explicit zero load size wins":
				tt.entry.SetLoadSize(0)
			}
			b := tt.entry.entryBytes()
			expected := make([]byte, 0x20)
			copy(expected, tt.expected)
			if !bytes.Equal(b, expected) {
				t.Errorf("Mismatched bytes, actual then expected\n% x\n% x\n", b, expected)
			}
		})
	}
}
