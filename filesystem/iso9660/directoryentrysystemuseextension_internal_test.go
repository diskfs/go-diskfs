package iso9660

import "testing"

func TestParseDirectoryEntryExtensionsFourByteEntry(t *testing.T) {
	fourByteEntry := []byte{'S', 'T', 4, 1}
	entries, err := parseDirectoryEntryExtensions(fourByteEntry, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(entries))
	}
}
