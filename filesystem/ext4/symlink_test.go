package ext4

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

// TestSymlinkCreation tests creating symlinks of various kinds.
func TestSymlinkCreation(t *testing.T) {
	imageTests := []struct {
		name      string
		imageFile string
		fsOffset  int64
	}{
		{"no offset", imgFile, 0},
		{"with offset", imgFileOffset, 1024},
	}

	for _, it := range imageTests {
		t.Run(it.name, func(t *testing.T) {
			t.Run("short symlink", func(t *testing.T) {
				outfile := testCreateImgCopyFrom(t, it.imageFile)
				f, err := os.OpenFile(outfile, os.O_RDWR, 0)
				if err != nil {
					t.Fatalf("Error opening: %v", err)
				}
				defer f.Close()

				b := file.New(f, false)
				fs, err := Read(b, 100*MB, it.fsOffset, 512)
				if err != nil {
					t.Fatalf("Error reading: %v", err)
				}

				// Create a short symlink (target < 60 bytes, stored inline in inode)
				target := "random.dat"
				linkName := "short_symlink"

				if err := fs.Symlink(target, linkName); err != nil {
					t.Fatalf("Symlink creation failed: %v", err)
				}

				// Verify via ReadLink
				readTarget, err := fs.ReadLink(linkName)
				if err != nil {
					t.Fatalf("ReadLink failed: %v", err)
				}
				if readTarget != target {
					t.Errorf("expected target %q, got %q", target, readTarget)
				}

				// Verify the symlink resolves — open the file via symlink
				fsFile, err := fs.OpenFile(linkName, os.O_RDONLY)
				if err != nil {
					t.Fatalf("OpenFile via symlink failed: %v", err)
				}
				defer fsFile.Close()

				// Read some data to confirm it's the correct file
				buf := make([]byte, 10)
				n, err := fsFile.Read(buf)
				if err != nil && err != io.EOF {
					t.Fatalf("Read via symlink failed: %v", err)
				}
				if n == 0 {
					t.Errorf("expected to read some bytes from symlinked file, got 0")
				}
			})

			t.Run("long symlink", func(t *testing.T) {
				outfile := testCreateImgCopyFrom(t, it.imageFile)
				f, err := os.OpenFile(outfile, os.O_RDWR, 0)
				if err != nil {
					t.Fatalf("Error opening: %v", err)
				}
				defer f.Close()

				b := file.New(f, false)
				fs, err := Read(b, 100*MB, it.fsOffset, 512)
				if err != nil {
					t.Fatalf("Error reading: %v", err)
				}

				// Create a long symlink (target >= 60 bytes, stored in extent blocks)
				target := strings.Repeat("a", 80) // 80 bytes, well over 60
				linkName := "long_symlink"

				if err := fs.Symlink(target, linkName); err != nil {
					t.Fatalf("Symlink creation failed for long target: %v", err)
				}

				// Verify via ReadLink
				readTarget, err := fs.ReadLink(linkName)
				if err != nil {
					t.Fatalf("ReadLink failed for long symlink: %v", err)
				}
				if readTarget != target {
					t.Errorf("expected long target %q, got %q", target, readTarget)
				}
			})

			t.Run("dead symlink", func(t *testing.T) {
				outfile := testCreateImgCopyFrom(t, it.imageFile)
				f, err := os.OpenFile(outfile, os.O_RDWR, 0)
				if err != nil {
					t.Fatalf("Error opening: %v", err)
				}
				defer f.Close()

				b := file.New(f, false)
				fs, err := Read(b, 100*MB, it.fsOffset, 512)
				if err != nil {
					t.Fatalf("Error reading: %v", err)
				}

				// Create a symlink whose target doesn't exist
				target := "does_not_exist.dat"
				linkName := "dead_link"

				if err := fs.Symlink(target, linkName); err != nil {
					t.Fatalf("Symlink creation for dead link failed: %v", err)
				}

				// ReadLink should succeed — symlinks can point to nonexistent targets
				readTarget, err := fs.ReadLink(linkName)
				if err != nil {
					t.Fatalf("ReadLink on dead link failed: %v", err)
				}
				if readTarget != target {
					t.Errorf("expected target %q, got %q", target, readTarget)
				}

				// Opening the symlinked file should fail
				_, err = fs.OpenFile(linkName, os.O_RDONLY)
				if err == nil {
					t.Errorf("expected error when opening dead symlink, got nil")
				}
			})

			t.Run("symlink already exists", func(t *testing.T) {
				outfile := testCreateImgCopyFrom(t, it.imageFile)
				f, err := os.OpenFile(outfile, os.O_RDWR, 0)
				if err != nil {
					t.Fatalf("Error opening: %v", err)
				}
				defer f.Close()

				b := file.New(f, false)
				fs, err := Read(b, 100*MB, it.fsOffset, 512)
				if err != nil {
					t.Fatalf("Error reading: %v", err)
				}

				// Try to create a symlink where a file already exists
				err = fs.Symlink("random.dat", "shortfile.txt")
				if err == nil {
					t.Errorf("expected error when creating symlink where file exists, got nil")
				}
				if !strings.Contains(err.Error(), "already exists") {
					t.Errorf("expected 'already exists' error, got: %v", err)
				}
			})

			t.Run("symlink invalid path", func(t *testing.T) {
				outfile := testCreateImgCopyFrom(t, it.imageFile)
				f, err := os.OpenFile(outfile, os.O_RDWR, 0)
				if err != nil {
					t.Fatalf("Error opening: %v", err)
				}
				defer f.Close()

				b := file.New(f, false)
				fs, err := Read(b, 100*MB, it.fsOffset, 512)
				if err != nil {
					t.Fatalf("Error reading: %v", err)
				}

				// path starting with / is invalid per validatePath
				err = fs.Symlink("random.dat", "/absolute_link")
				if err == nil {
					t.Errorf("expected error for absolute symlink path, got nil")
				}
			})
		})
	}
}

// TestSymlinkInSubdirectory tests creating a symlink inside a subdirectory.
func TestSymlinkInSubdirectory(t *testing.T) {
	// Create a fresh filesystem so directory state is clean
	size := 100 * MB
	outfile, f := testCreateEmptyFile(t, size)
	defer f.Close()

	b := file.New(f, false)
	fs, err := Create(b, size, 0, 512, &Params{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Create a subdirectory and a target file
	if err := fs.Mkdir("subdir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	targetFile, err := fs.OpenFile("target.txt", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := targetFile.Write([]byte("target content")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create symlink in subdirectory
	if err := fs.Symlink("../target.txt", "subdir/link_to_target"); err != nil {
		t.Fatalf("Symlink in subdirectory failed: %v", err)
	}

	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}

	// Re-read the filesystem
	f2, err := os.Open(outfile)
	if err != nil {
		t.Fatalf("Error reopening: %v", err)
	}
	defer f2.Close()

	b2 := file.New(f2, true)
	fs2, err := Read(b2, size, 0, 512)
	if err != nil {
		t.Fatalf("Error re-reading: %v", err)
	}

	readTarget, err := fs2.ReadLink("subdir/link_to_target")
	if err != nil {
		t.Fatalf("ReadLink failed: %v", err)
	}
	if readTarget != "../target.txt" {
		t.Errorf("expected target %q, got %q", "../target.txt", readTarget)
	}
}

// TestReadLinkNonSymlink tests that ReadLink on a regular file returns an error.
func TestReadLinkNonSymlink(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading: %v", err)
	}

	_, err = fs.ReadLink("random.dat")
	if err == nil {
		t.Errorf("expected error reading link on non-symlink, got nil")
	}
	if !strings.Contains(err.Error(), "not a symbolic link") {
		t.Errorf("expected 'not a symbolic link' error, got: %v", err)
	}
}

// TestReadLinkNonexistent tests that ReadLink on a nonexistent path returns an error.
func TestReadLinkNonexistent(t *testing.T) {
	f, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening: %v", err)
	}
	defer f.Close()

	b := file.New(f, true)
	fs, err := Read(b, 100*MB, 0, 512)
	if err != nil {
		t.Fatalf("Error reading: %v", err)
	}

	_, err = fs.ReadLink("nonexistent.dat")
	if err == nil {
		t.Errorf("expected error for nonexistent symlink, got nil")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("expected 'does not exist' error, got: %v", err)
	}
}

// TestSymlinkE2fsckValid verifies that a filesystem with created symlinks passes e2fsck.
func TestSymlinkE2fsckValid(t *testing.T) {
	size := 100 * MB
	outfile, f := testCreateEmptyFile(t, size)
	defer f.Close()

	b := file.New(f, false)
	fs, err := Create(b, size, 0, 512, &Params{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Create a regular file first
	ext4File, err := fs.OpenFile("target.txt", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile for write failed: %v", err)
	}
	if _, err := ext4File.Write([]byte("symlink target content")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create short symlink
	if err := fs.Symlink("target.txt", "short_link"); err != nil {
		t.Fatalf("Short Symlink creation failed: %v", err)
	}

	// Create long symlink
	longTarget := strings.Repeat("x", 100)
	if err := fs.Symlink(longTarget, "long_link"); err != nil {
		t.Fatalf("Long Symlink creation failed: %v", err)
	}

	// Create dead symlink
	if err := fs.Symlink("ghost.txt", "dead_link"); err != nil {
		t.Fatalf("Dead Symlink creation failed: %v", err)
	}

	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}

	cmd := exec.Command("e2fsck", "-f", "-n", outfile)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("e2fsck failed after symlink creation: %v\nstdout:\n%s\nstderr:\n%s",
			err, stdout.String(), stderr.String())
	}
}
