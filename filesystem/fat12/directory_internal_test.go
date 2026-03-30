package fat12

// directory_internal_test.go exercises uniqueShortName, nameMatches,
// createEntry (collision avoidance), removeEntry, and renameEntry.

import (
	"fmt"
	"testing"
)

// ── nameMatches ──────────────────────────────────────────────────────────────

func TestMatchesFilename(t *testing.T) {
	e := &directoryEntry{
		filenameLong:  "LongFileName.txt",
		filenameShort: "LONGFI~1",
		fileExtension: "TXT",
	}

	tests := []struct {
		name string
		want bool
	}{
		{"LongFileName.txt", true}, // exact long name
		{"longfilename.txt", true}, // long name case-insensitive
		{"LONGFILENAME.TXT", true}, // long name all-caps
		{"LONGFI~1.TXT", true},     // short name exact
		{"longfi~1.txt", true},     // short name case-insensitive
		{"other.txt", false},       // no match
		{"LONGFI~2.TXT", false},    // wrong tail
	}

	for _, tt := range tests {
		if got := e.nameMatches(tt.name); got != tt.want {
			t.Errorf("e.nameMatches(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestMatchesFilenameNoLFN(t *testing.T) {
	// Entry with no long filename (pure 8.3 entry).
	e := &directoryEntry{
		filenameLong:  "",
		filenameShort: "README",
		fileExtension: "TXT",
	}

	tests := []struct {
		name string
		want bool
	}{
		{"README.TXT", true},
		{"readme.txt", true},
		{"readme", false}, // extension missing
		{"OTHER.TXT", false},
	}
	for _, tt := range tests {
		if got := e.nameMatches(tt.name); got != tt.want {
			t.Errorf("matchesFilename(noLFN, %q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// ── uniqueShortName ───────────────────────────────────────────────────────────

func TestUniqueShortName(t *testing.T) {
	t.Run("no conflicts → returns ~1", func(t *testing.T) {
		got := uniqueShortName("LONGFI", "TXT", nil)
		if got != "LONGFI~1" {
			t.Errorf("got %q, want %q", got, "LONGFI~1")
		}
	})

	t.Run("~1 taken → returns ~2", func(t *testing.T) {
		existing := []*directoryEntry{
			{filenameShort: "LONGFI~1", fileExtension: "TXT"},
		}
		got := uniqueShortName("LONGFI", "TXT", existing)
		if got != "LONGFI~2" {
			t.Errorf("got %q, want %q", got, "LONGFI~2")
		}
	})

	t.Run("~1 through ~9 taken → returns ~10 with shorter stem", func(t *testing.T) {
		existing := make([]*directoryEntry, 9)
		for i := range 9 {
			existing[i] = &directoryEntry{
				filenameShort: fmt.Sprintf("LONGFI~%d", i+1),
				fileExtension: "TXT",
			}
		}
		got := uniqueShortName("LONGFI", "TXT", existing)
		if got != "LONGF~10" {
			t.Errorf("got %q, want %q", got, "LONGF~10")
		}
	})

	t.Run("~1 through ~99 taken → returns ~100 with even shorter stem", func(t *testing.T) {
		existing := make([]*directoryEntry, 99)
		for i := range 99 {
			suffix := fmt.Sprintf("~%d", i+1)
			maxStem := 8 - len(suffix)
			stem := "LONGFI"
			if len(stem) > maxStem {
				stem = stem[:maxStem]
			}
			existing[i] = &directoryEntry{
				filenameShort: stem + suffix,
				fileExtension: "TXT",
			}
		}
		got := uniqueShortName("LONGFI", "TXT", existing)
		// ~100 has length 4, so maxStem = 8-4 = 4 → "LONG" + "~100" = "LONG~100"
		if got != "LONG~100" {
			t.Errorf("got %q, want %q", got, "LONG~100")
		}
	})

	t.Run("volume labels are excluded from conflict check", func(t *testing.T) {
		existing := []*directoryEntry{
			{filenameShort: "LONGFI~1", fileExtension: "TXT", isVolumeLabel: true},
		}
		// The volume label should be ignored, so ~1 is available.
		got := uniqueShortName("LONGFI", "TXT", existing)
		if got != "LONGFI~1" {
			t.Errorf("got %q, want %q", got, "LONGFI~1")
		}
	})

	t.Run("short stem shorter than 6 chars", func(t *testing.T) {
		// stem "AB" → "AB~1", no conflict.
		got := uniqueShortName("AB", "TXT", nil)
		if got != "AB~1" {
			t.Errorf("got %q, want %q", got, "AB~1")
		}
	})
}

// ── createEntry collision avoidance ──────────────────────────────────────────

func TestCreateEntryShortNameCollision(t *testing.T) {
	// Two files whose names both truncate to the same 6-char stem.
	d := &Directory{}

	e1, err := d.createEntry("LongFileNameA.txt", 2, false)
	if err != nil {
		t.Fatalf("createEntry first: %v", err)
	}
	e2, err := d.createEntry("LongFileNameB.txt", 3, false)
	if err != nil {
		t.Fatalf("createEntry second: %v", err)
	}

	if e1.filenameShort == e2.filenameShort {
		t.Errorf("both entries got the same short name %q — collision not resolved",
			e1.filenameShort)
	}
	if e1.filenameShort != "LONGFI~1" {
		t.Errorf("first entry: short name = %q, want LONGFI~1", e1.filenameShort)
	}
	if e2.filenameShort != "LONGFI~2" {
		t.Errorf("second entry: short name = %q, want LONGFI~2", e2.filenameShort)
	}
}

func TestCreateEntryManyCollisions(t *testing.T) {
	d := &Directory{}
	prev := ""
	for i := range 12 {
		name := fmt.Sprintf("LongFileNameNumber%02d.txt", i)
		e, err := d.createEntry(name, uint32(i+2), false)
		if err != nil {
			t.Fatalf("createEntry %d: %v", i, err)
		}
		if e.filenameShort == prev {
			t.Errorf("entry %d has same short name as previous: %q", i, e.filenameShort)
		}
		prev = e.filenameShort
	}
}

func TestCreateEntryNoCollisionWhenStemFits(t *testing.T) {
	// Name fits in 8.3 with no truncation — no ~N suffix at all.
	d := &Directory{}
	e, err := d.createEntry("short.txt", 2, false)
	if err != nil {
		t.Fatalf("createEntry: %v", err)
	}
	if e.filenameShort != "SHORT" {
		t.Errorf("short name = %q, want SHORT", e.filenameShort)
	}
	if e.fileExtension != "TXT" {
		t.Errorf("extension = %q, want TXT", e.fileExtension)
	}
}

// ── removeEntry ───────────────────────────────────────────────────────────────

func TestRemoveEntryByLongName(t *testing.T) {
	d := &Directory{}
	if _, err := d.createEntry("LongFileNameA.txt", 2, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}
	if _, err := d.createEntry("LongFileNameB.txt", 3, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}

	if err := d.removeEntry("LongFileNameA.txt"); err != nil {
		t.Fatalf("removeEntry by long name: %v", err)
	}
	if len(d.entries) != 1 {
		t.Errorf("expected 1 entry after remove, got %d", len(d.entries))
	}
	if !d.entries[0].nameMatches("LongFileNameB.txt") {
		t.Errorf("wrong entry remains: %q", d.entries[0].filenameLong)
	}
}

func TestRemoveEntryByShortName(t *testing.T) {
	d := &Directory{}
	if _, err := d.createEntry("LongFileNameA.txt", 2, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}
	if _, err := d.createEntry("LongFileNameB.txt", 3, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}

	// Remove by the 8.3 short name of the first entry.
	if err := d.removeEntry("LONGFI~1.TXT"); err != nil {
		t.Fatalf("removeEntry by short name: %v", err)
	}
	if len(d.entries) != 1 {
		t.Errorf("expected 1 entry after remove, got %d", len(d.entries))
	}
}

func TestRemoveEntryNotFound(t *testing.T) {
	d := &Directory{}
	if _, err := d.createEntry("hello.txt", 2, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}

	if err := d.removeEntry("nonexistent.txt"); err == nil {
		t.Error("expected error removing non-existent entry, got nil")
	}
}

// ── renameEntry ───────────────────────────────────────────────────────────────

func TestRenameEntryByLongName(t *testing.T) {
	d := &Directory{}
	if _, err := d.createEntry("LongFileNameA.txt", 2, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}

	if err := d.renameEntry("LongFileNameA.txt", "newname.txt"); err != nil {
		t.Fatalf("renameEntry: %v", err)
	}
	if len(d.entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(d.entries))
	}
	if !d.entries[0].nameMatches("newname.txt") {
		t.Errorf("entry not renamed: %+v", d.entries[0])
	}
}

func TestRenameEntryByShortName(t *testing.T) {
	d := &Directory{}
	if _, err := d.createEntry("LongFileNameA.txt", 2, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}

	// Rename by short name.
	if err := d.renameEntry("LONGFI~1.TXT", "renamed.txt"); err != nil {
		t.Fatalf("renameEntry by short name: %v", err)
	}
	if len(d.entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(d.entries))
	}
	if !d.entries[0].nameMatches("renamed.txt") {
		t.Errorf("entry not renamed by short name: %+v", d.entries[0])
	}
}

func TestRenameEntryNewNameCollision(t *testing.T) {
	// When the new name is also long, renameEntry must assign a unique tail.
	d := &Directory{}
	if _, err := d.createEntry("LongFileNameA.txt", 2, false); err != nil { // gets LONGFI~1
		t.Fatalf("createEntry: %v", err)
	}
	if _, err := d.createEntry("OtherShortFile.txt", 3, false); err != nil { // gets OTHERS~1 or similar
		t.Fatalf("createEntry: %v", err)
	}

	// Rename the second entry to a name that would also produce LONGFI~1,
	// but that slot is taken. It must get LONGFI~2.
	if err := d.renameEntry("OtherShortFile.txt", "LongFileNameX.txt"); err != nil {
		t.Fatalf("renameEntry: %v", err)
	}

	var renamedEntry *directoryEntry
	for _, e := range d.entries {
		if e.nameMatches("LongFileNameX.txt") {
			renamedEntry = e
		}
	}
	if renamedEntry == nil {
		t.Fatal("renamed entry not found")
	}
	if renamedEntry.filenameShort == "LONGFI~1" {
		t.Errorf("renamed entry got short name LONGFI~1 which is already taken")
	}
	if renamedEntry.filenameShort != "LONGFI~2" {
		t.Errorf("renamed entry short name = %q, want LONGFI~2", renamedEntry.filenameShort)
	}
}

func TestRenameEntryNewNameFreesOldSlot(t *testing.T) {
	// Renaming should NOT count the old entry's short name as a conflict for
	// the new name when the same stem is reused.
	d := &Directory{}
	if _, err := d.createEntry("LongFileNameA.txt", 2, false); err != nil { // gets LONGFI~1
		t.Fatalf("createEntry: %v", err)
	}

	// Renaming to another long name with the same stem: LONGFI~1 is vacated,
	// so the new entry should be allowed to take LONGFI~1.
	if err := d.renameEntry("LongFileNameA.txt", "LongFileNameB.txt"); err != nil {
		t.Fatalf("renameEntry: %v", err)
	}
	if len(d.entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(d.entries))
	}
	// The old slot is free, so LONGFI~1 is available for the new name.
	if d.entries[0].filenameShort != "LONGFI~1" {
		t.Errorf("short name = %q, want LONGFI~1", d.entries[0].filenameShort)
	}
}

func TestRenameEntryOverwritesExistingDestination(t *testing.T) {
	// If an entry with the new name already exists, it must be removed
	// (mirroring the POSIX rename(2) behaviour).
	d := &Directory{}
	if _, err := d.createEntry("fileA.txt", 2, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}
	if _, err := d.createEntry("fileB.txt", 3, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}

	if err := d.renameEntry("fileA.txt", "fileB.txt"); err != nil {
		t.Fatalf("renameEntry overwrite: %v", err)
	}
	// Only one entry should remain.
	if len(d.entries) != 1 {
		t.Errorf("expected 1 entry after overwrite rename, got %d", len(d.entries))
	}
	if !d.entries[0].nameMatches("fileB.txt") {
		t.Errorf("wrong entry after overwrite: %q", d.entries[0].filenameLong)
	}
	// The surviving entry should point to cluster 2 (fileA's cluster),
	// because fileA was renamed to fileB.
	if d.entries[0].clusterLocation != 2 {
		t.Errorf("cluster = %d, want 2 (fileA's cluster)", d.entries[0].clusterLocation)
	}
}

func TestRenameEntryNotFound(t *testing.T) {
	d := &Directory{}
	if _, err := d.createEntry("hello.txt", 2, false); err != nil {
		t.Fatalf("createEntry: %v", err)
	}

	if err := d.renameEntry("nonexistent.txt", "other.txt"); err == nil {
		t.Error("expected error renaming non-existent entry, got nil")
	}
}
