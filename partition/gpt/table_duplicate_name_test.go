package gpt_test

import (
	"os"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/partition/gpt"
)

// TestTableDuplicateNameRoundTrip verifies that two partitions sharing an
// identical Name but with distinct GUIDs survive a Write/Read round-trip with
// their per-entry GUIDs preserved and distinct. A GPT partition is keyed by its
// unique GUID; the name field carries no uniqueness constraint, so a consumer
// that must pick one of two identically-named partitions has to rely on UUID().
// This guards that the library keeps the two entries distinguishable.
func TestTableDuplicateNameRoundTrip(t *testing.T) {
	const (
		sectorSize = 512
		diskSize   = 10 * 1024 * 1024
		dupName    = "EFI System"
		guidA      = "aaaaaaaa-0000-0000-0000-000000000001"
		guidB      = "bbbbbbbb-0000-0000-0000-000000000002"
	)

	f, err := os.CreateTemp("", "gpt-dupname-*.img")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := f.Truncate(diskSize); err != nil {
		t.Fatal(err)
	}

	table := &gpt.Table{
		LogicalSectorSize: sectorSize,
		ProtectiveMBR:     true,
		Partitions: []*gpt.Partition{
			{Index: 1, Start: 2048, End: 2048 + 2048 - 1, Type: gpt.EFISystemPartition, Name: dupName, GUID: guidA},
			{Index: 2, Start: 4096, End: 4096 + 2048 - 1, Type: gpt.EFISystemPartition, Name: dupName, GUID: guidB},
		},
	}
	if err := table.Write(f, diskSize); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := gpt.Read(f, sectorSize, sectorSize)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	parts := got.GetPartitions()
	if len(parts) != 2 {
		t.Fatalf("GetPartitions len = %d, want 2", len(parts))
	}

	for i, want := range []struct{ name, guid string }{{dupName, guidA}, {dupName, guidB}} {
		if l := parts[i].Label(); l != want.name {
			t.Errorf("partition %d Label = %q, want %q", i+1, l, want.name)
		}
		// Read uppercases the GUID, so compare case-insensitively.
		if u := parts[i].UUID(); !strings.EqualFold(u, want.guid) {
			t.Errorf("partition %d UUID = %q, want %q (case-insensitive)", i+1, u, want.guid)
		}
	}

	// The decisive properties: labels identical, GUIDs distinct.
	if parts[0].Label() != parts[1].Label() {
		t.Fatalf("labels not identical: %q vs %q", parts[0].Label(), parts[1].Label())
	}
	if strings.EqualFold(parts[0].UUID(), parts[1].UUID()) {
		t.Errorf("GUIDs not distinct: both %q", parts[0].UUID())
	}
}
