package squashfs_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
)

// With SOURCE_DATE_EPOCH set, the image does not depend on the build time
// or the workspace's file times.
func TestFinalizeSourceDateEpoch(t *testing.T) {
	t.Setenv("SOURCE_DATE_EPOCH", "1700000000")
	epoch := time.Unix(1700000000, 0)

	build := func(mtime time.Time) []byte {
		f, err := os.CreateTemp(t.TempDir(), "squashfs_epoch_test")
		if err != nil {
			t.Fatalf("Failed to create tmpfile: %v", err)
		}
		defer f.Close()
		fs, err := squashfs.Create(file.New(f, false), 0, 0, 4096)
		if err != nil {
			t.Fatalf("Failed to squashfs.Create: %v", err)
		}
		p := filepath.Join(fs.Workspace(), "a")
		if err := os.WriteFile(p, []byte("a\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		if err := fs.Finalize(squashfs.FinalizeOptions{}); err != nil {
			t.Fatalf("unexpected error fs.Finalize(): %v", err)
		}

		fs, err = squashfs.Read(file.New(f, true), 0, 0, 4096)
		if err != nil {
			t.Fatalf("error reading the tmpfile as squashfs: %v", err)
		}
		fi, err := fs.Stat("a")
		if err != nil {
			t.Fatalf("error stat a: %v", err)
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
	second := build(time.Unix(2, 0))
	if !bytes.Equal(first, second) {
		t.Errorf("images differ")
	}
}
