package iso9660_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
)

// With SOURCE_DATE_EPOCH set, the image does not depend on the build time
// or the workspace's file times.
func TestFinalizeSourceDateEpoch(t *testing.T) {
	t.Setenv("SOURCE_DATE_EPOCH", "1700000000")
	epoch := time.Unix(1700000000, 0)

	build := func(mtime time.Time) []byte {
		f, err := os.CreateTemp(t.TempDir(), "iso_epoch_test")
		if err != nil {
			t.Fatalf("Failed to create tmpfile: %v", err)
		}
		defer f.Close()
		fs, err := iso9660.Create(file.New(f, false), 0, 0, testISOBlockSize, "")
		if err != nil {
			t.Fatalf("Failed to iso9660.Create: %v", err)
		}
		p := filepath.Join(fs.Workspace(), "A")
		if err := os.WriteFile(p, []byte("a\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		if err := fs.Finalize(iso9660.FinalizeOptions{}); err != nil {
			t.Fatalf("unexpected error fs.Finalize(): %v", err)
		}

		fs, err = iso9660.Read(file.New(f, true), 0, 0, testISOBlockSize)
		if err != nil {
			t.Fatalf("error reading the tmpfile as iso9660: %v", err)
		}
		fi, err := fs.Stat("A")
		if err != nil {
			t.Fatalf("error stat A: %v", err)
		}
		if !fi.ModTime().Equal(epoch) {
			t.Errorf("modification time %v, expected %v", fi.ModTime(), epoch)
		}
		b, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	first := build(time.Unix(1, 0))
	time.Sleep(time.Second) // the volume descriptor times have seconds resolution
	second := build(time.Unix(2, 0))
	if !bytes.Equal(first, second) {
		t.Errorf("images differ")
	}
}
