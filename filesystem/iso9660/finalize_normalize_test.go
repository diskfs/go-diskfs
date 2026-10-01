package iso9660_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
)

// Normalize writes the same Rock Ridge owner and modes on every host.
func TestFinalizeNormalize(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "iso_normalize_test")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	defer f.Close()
	fs, err := iso9660.Create(file.New(f, false), 0, 0, testISOBlockSize, "")
	if err != nil {
		t.Fatalf("Failed to iso9660.Create: %v", err)
	}
	if err := os.Mkdir(filepath.Join(fs.Workspace(), "dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fs.Workspace(), "dir", "file"), []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fs.Finalize(iso9660.FinalizeOptions{RockRidge: true, Normalize: true}); err != nil {
		t.Fatalf("unexpected error fs.Finalize(): %v", err)
	}

	fs, err = iso9660.Read(file.New(f, true), 0, 0, testISOBlockSize)
	if err != nil {
		t.Fatalf("error reading the tmpfile as iso9660: %v", err)
	}
	for _, tt := range []struct {
		path  string
		mode  os.FileMode
		nlink uint32
	}{
		{"dir", os.ModeDir | 0o555, 2},
		{"dir/file", 0o444, 1},
	} {
		fi, err := fs.Stat(tt.path)
		if err != nil {
			t.Fatalf("error stat %s: %v", tt.path, err)
		}
		if fi.Mode() != tt.mode {
			t.Errorf("%s: mode %v, expected %v", tt.path, fi.Mode(), tt.mode)
		}
		sys, ok := fi.Sys().(*iso9660.StatT)
		if !ok {
			t.Fatalf("could not convert fi.Sys() to *StatT")
		}
		if sys.UID != 0 || sys.GID != 0 || sys.NLink != tt.nlink {
			t.Errorf("%s: owner %d:%d, links %d, expected 0:0, %d", tt.path, sys.UID, sys.GID, sys.NLink, tt.nlink)
		}
	}
}
