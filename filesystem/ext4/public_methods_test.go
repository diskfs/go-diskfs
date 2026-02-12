package ext4

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem"
)

// TestType verifies that FileSystem.Type() returns TypeExt4.
func TestType(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	if fs.Type() != filesystem.TypeExt4 {
		t.Errorf("expected TypeExt4 (%d), got %d", filesystem.TypeExt4, fs.Type())
	}
}

// TestClose verifies that Close() returns nil (no-op for ext4).
func TestClose(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	if err := fs.Close(); err != nil {
		t.Errorf("expected nil error from Close(), got: %v", err)
	}
}

// TestEqual verifies the FileSystem.Equal() method.
func TestEqual(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs1, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem (first): %v", err)
	}

	t.Run("equal to self", func(t *testing.T) {
		if !fs1.Equal(fs1) {
			t.Errorf("expected filesystem to be equal to itself")
		}
	})

	t.Run("different filesystem", func(t *testing.T) {
		// Open a second handle to the same file — different backend means not Equal
		f2, err := os.Open(imgFile)
		if err != nil {
			t.Fatalf("Error opening second test image: %v", err)
		}
		defer f2.Close()

		b2 := file.New(f2, true)
		fs2, err := Read(b2, 100*MB, 0, 512)
		if err != nil {
			t.Fatalf("Error reading filesystem (second): %v", err)
		}

		if fs1.Equal(fs2) {
			t.Errorf("expected filesystems from different backends to not be Equal")
		}
	})
}

// TestMknodNotImplemented verifies that Mknod returns ErrNotImplemented.
func TestMknodNotImplemented(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	err = fs.Mknod("/testnode", 0, 0)
	if !errors.Is(err, filesystem.ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented, got: %v", err)
	}
}

// TestLinkNotImplemented verifies that Link returns ErrNotImplemented.
func TestLinkNotImplemented(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	err = fs.Link("random.dat", "hardlink2.dat")
	if !errors.Is(err, filesystem.ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented, got: %v", err)
	}
}

// TestRenameNotImplemented verifies that Rename returns ErrNotImplemented.
func TestRenameNotImplemented(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	err = fs.Rename("random.dat", "renamed.dat")
	if !errors.Is(err, filesystem.ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented, got: %v", err)
	}
}

// TestLabel verifies that Label() returns the volume label.
func TestLabel(t *testing.T) {
	t.Run("read existing label", func(t *testing.T) {
		f, err := os.Open(imgFile)
		if err != nil {
			t.Fatalf("Error opening test image: %v", err)
		}
		defer f.Close()

		b := file.New(f, true)
		fs, err := Read(b, 100*MB, 0, 512)
		if err != nil {
			t.Fatalf("Error reading filesystem: %v", err)
		}

		// Label() should not panic; the test image may or may not have a label
		_ = fs.Label()
	})

	t.Run("nil superblock returns empty", func(t *testing.T) {
		fs := &FileSystem{superblock: nil}
		if fs.Label() != "" {
			t.Errorf("expected empty label for nil superblock, got %q", fs.Label())
		}
	})
}

// TestSetLabel verifies that SetLabel() changes the volume label and persists it.
func TestSetLabel(t *testing.T) {
	outfile := testCreateImgCopyFrom(t, imgFile)
	f, err := os.OpenFile(outfile, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, false)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	newLabel := "NEWLABEL"
	if err := fs.SetLabel(newLabel); err != nil {
		t.Fatalf("SetLabel failed: %v", err)
	}

	if fs.Label() != newLabel {
		t.Errorf("expected label %q, got %q", newLabel, fs.Label())
	}

	// re-read the filesystem to confirm persistence
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Error seeking: %v", err)
	}
	b2 := file.New(f, true)
	fs2, err := Read(b2, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error re-reading filesystem: %v", err)
	}
	if fs2.Label() != newLabel {
		t.Errorf("label not persisted: expected %q, got %q", newLabel, fs2.Label())
	}
}

// TestReadFile verifies the convenience ReadFile method.
func TestReadFileMethod(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	t.Run("existing file", func(t *testing.T) {
		expected, err := os.ReadFile(randomDataFile)
		if err != nil {
			t.Fatalf("Error reading reference data: %v", err)
		}
		data, err := fs.ReadFile("random.dat")
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}
		if !bytes.Equal(data, expected) {
			t.Errorf("ReadFile data mismatch: expected %d bytes, got %d bytes", len(expected), len(data))
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := fs.ReadFile("nonexistent.dat")
		if err == nil {
			t.Errorf("expected error for nonexistent file, got nil")
		}
	})

	t.Run("directory", func(t *testing.T) {
		_, err := fs.ReadFile("foo")
		if err == nil {
			t.Errorf("expected error when calling ReadFile on a directory, got nil")
		}
	})
}

// TestStat verifies the Stat method directly.
func TestStat(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	t.Run("regular file", func(t *testing.T) {
		fi, err := fs.Stat("random.dat")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if fi.IsDir() {
			t.Errorf("expected regular file, got directory")
		}
		if fi.Size() <= 0 {
			t.Errorf("expected positive size, got %d", fi.Size())
		}
		if fi.Name() != "random.dat" {
			t.Errorf("expected name 'random.dat', got %q", fi.Name())
		}
	})

	t.Run("directory", func(t *testing.T) {
		fi, err := fs.Stat("foo")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if !fi.IsDir() {
			t.Errorf("expected directory, got regular file")
		}
	})

	t.Run("nonexistent", func(t *testing.T) {
		_, err := fs.Stat("nonexistent.dat")
		if err == nil {
			t.Errorf("expected error for nonexistent file")
		}
	})

	t.Run("sys returns StatT", func(t *testing.T) {
		fi, err := fs.Stat("random.dat")
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		stat, ok := fi.Sys().(*StatT)
		if !ok {
			t.Fatalf("Sys() did not return *StatT, got %T", fi.Sys())
		}
		// uid and gid should be non-negative (they are uint32)
		_ = stat.UID
		_ = stat.GID
	})
}
