package fat12_test

import (
	"bytes"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat12"
)

// ── helpers ───────────────────────────────────────────────────────────────────

const (
	// floppy144 is a standard 1.44 MB floppy image size.
	floppy144 = int64(1474560)
	// small8MB is a small FAT12 image for testing cluster chains.
	small8MB = int64(8 * 1024 * 1024)
)

// tmpImage creates a zeroed temporary file of the given size inside t.TempDir().
func tmpImage(t *testing.T, name string, size int64) string {
	t.Helper()
	p := path.Join(t.TempDir(), name)
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create temp image %s: %v", p, err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatalf("truncate temp image %s to %d: %v", p, size, err)
	}
	return p
}

// createFAT12 creates a FAT12 filesystem on a fresh image and returns the path.
func createFAT12(t *testing.T, label string) (imgPath string, fs filesystem.FileSystem) {
	t.Helper()
	imgPath = tmpImage(t, "fat12.img", floppy144)
	b, err := file.OpenFromPath(imgPath, false)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	fs, err = fat12.Create(b, floppy144, 0, 512, label, false)
	if err != nil {
		t.Fatalf("fat12.Create: %v", err)
	}
	return imgPath, fs
}

// reopenFAT12 opens an existing image as a FAT12 filesystem.
func reopenFAT12(t *testing.T, imgPath string) filesystem.FileSystem {
	t.Helper()
	b, err := file.OpenFromPath(imgPath, false)
	if err != nil {
		t.Fatalf("open backend for re-read: %v", err)
	}
	fs, err := fat12.Read(b, floppy144, 0, 512)
	if err != nil {
		t.Fatalf("fat12.Read: %v", err)
	}
	return fs
}

func writeFile(t *testing.T, fs filesystem.FileSystem, name string, content []byte) {
	t.Helper()
	f, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile(%q): %v", name, err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatalf("Write(%q): %v", name, err)
	}
	f.Close()
}

// ── Create ────────────────────────────────────────────────────────────────────

func TestFat12Create(t *testing.T) {
	tests := []struct {
		name      string
		size      int64
		blocksize int64
		wantErr   string
	}{
		{"valid 1.44MB floppy", floppy144, 512, ""},
		{"valid 6MB", 6 * 1024 * 1024, 512, ""},
		{"valid blocksize 0 (default)", floppy144, 0, ""},
		{"bad blocksize", floppy144, 1024, "blocksize for FAT12 must be"},
		{"too large", fat12.Fat12MaxSize + 1, 512, "exceeds FAT12 maximum"},
		{"too small", 512, 512, "too small"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imgPath := tmpImage(t, "fat12.img", max(tt.size, floppy144))
			b, err := file.OpenFromPath(imgPath, false)
			if err != nil {
				t.Fatalf("open backend: %v", err)
			}
			_, err = fat12.Create(b, tt.size, 0, tt.blocksize, "TEST", false)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.wantErr)
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

// ── Type ─────────────────────────────────────────────────────────────────────

func TestFat12Type(t *testing.T) {
	_, fs := createFAT12(t, "TYPTEST")
	if got := fs.Type(); got != filesystem.TypeFat12 {
		t.Errorf("Type() = %v, want TypeFat12", got)
	}
}

// ── Read (self-validation) ────────────────────────────────────────────────────

func TestFat12Read(t *testing.T) {
	t.Run("reads back a created FAT12 image", func(t *testing.T) {
		imgPath, _ := createFAT12(t, "READTEST")
		fs := reopenFAT12(t, imgPath)
		if fs.Type() != filesystem.TypeFat12 {
			t.Errorf("reopened Type() = %v, want TypeFat12", fs.Type())
		}
	})

	t.Run("rejects a FAT16 image", func(t *testing.T) {
		// Build a FAT16 image and try to open it as FAT12.
		imgPath := tmpImage(t, "fat16.img", 32*1024*1024)
		b, err := file.OpenFromPath(imgPath, false)
		if err != nil {
			t.Fatalf("open backend: %v", err)
		}
		_, err = fat12.Read(b, 32*1024*1024, 0, 512)
		if err == nil {
			t.Error("fat12.Read should reject a FAT16 image, got nil error")
		}
	})
}

// ── Label ─────────────────────────────────────────────────────────────────────

func TestFat12Label(t *testing.T) {
	const label = "MYLABEL"
	imgPath, fs := createFAT12(t, label)

	got := fs.Label()
	if !strings.HasPrefix(strings.TrimSpace(got), label) {
		t.Errorf("Label() = %q, want prefix %q", got, label)
	}

	// Persist and re-read.
	fs2 := reopenFAT12(t, imgPath)
	got2 := strings.TrimSpace(fs2.Label())
	if got2 != label {
		t.Errorf("after reopen: Label() = %q, want %q", got2, label)
	}
}

func TestFat12SetLabel(t *testing.T) {
	imgPath, fs := createFAT12(t, "OLD")
	if err := fs.SetLabel("NEWLABEL"); err != nil {
		t.Fatalf("SetLabel: %v", err)
	}

	fs2 := reopenFAT12(t, imgPath)
	got := strings.TrimSpace(fs2.Label())
	if got != "NEWLABEL" {
		t.Errorf("after SetLabel+reopen: Label() = %q, want %q", got, "NEWLABEL")
	}
}

// ── Mkdir ─────────────────────────────────────────────────────────────────────

func TestFat12Mkdir(t *testing.T) {
	dirs := []string{"/foo", "/foo/bar", "/a/b/c"}
	imgPath, fs := createFAT12(t, "MKDIRTEST")

	for _, d := range dirs {
		if err := fs.Mkdir(d); err != nil {
			t.Errorf("Mkdir(%q): %v", d, err)
		}
	}

	// Persist and verify via ReadDir.
	fs2 := reopenFAT12(t, imgPath)
	entries, err := fs2.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}
	names := dirEntryNames(entries)
	for _, want := range []string{"foo", "a"} {
		if !contains(names, want) {
			t.Errorf("root dir missing %q, got %v", want, names)
		}
	}

	subEntries, err := fs2.ReadDir("foo")
	if err != nil {
		t.Fatalf("ReadDir foo: %v", err)
	}
	subNames := dirEntryNames(subEntries)
	if !contains(subNames, "bar") {
		t.Errorf("foo/ missing 'bar', got %v", subNames)
	}
}

// ── OpenFile / Write / Read ───────────────────────────────────────────────────

func TestFat12WriteReadFile(t *testing.T) {
	content := []byte("Hello, FAT12!")
	imgPath, fs := createFAT12(t, "WRTEST")

	// Write.
	f, err := fs.OpenFile("/hello.txt", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile for write: %v", err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f.Close()

	// Read back from same FS instance.
	f2, err := fs.OpenFile("/hello.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile for read: %v", err)
	}
	buf, err := io.ReadAll(f2)
	f2.Close()
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(buf, content) {
		t.Errorf("content mismatch: got %q, want %q", buf, content)
	}

	// Re-open image from disk and verify persistence.
	fs2 := reopenFAT12(t, imgPath)
	f3, err := fs2.OpenFile("/hello.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile after reopen: %v", err)
	}
	buf2, _ := io.ReadAll(f3)
	f3.Close()
	if !bytes.Equal(buf2, content) {
		t.Errorf("persisted content mismatch: got %q, want %q", buf2, content)
	}
}

func TestFat12WriteReadLargeFile(t *testing.T) {
	// Write a file that spans multiple clusters (cluster size = 512 B on a floppy).
	content := bytes.Repeat([]byte("ABCDEFGH"), 256) // 2 KB
	_, fs := createFAT12(t, "LARGE")

	writeFile(t, fs, "/big.bin", content)

	f2, err := fs.OpenFile("/big.bin", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile for read: %v", err)
	}
	got, _ := io.ReadAll(f2)
	f2.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("large file content mismatch (len got=%d want=%d)", len(got), len(content))
	}
}

func TestFat12WriteReadSubdirFile(t *testing.T) {
	content := []byte("in a subdir")
	imgPath, fs := createFAT12(t, "SUBDIR")

	if err := fs.Mkdir("/sub"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	writeFile(t, fs, "/sub/file.txt", content)

	// Verify after reopen.
	fs2 := reopenFAT12(t, imgPath)
	f2, err := fs2.OpenFile("/sub/file.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile after reopen: %v", err)
	}
	got, _ := io.ReadAll(f2)
	f2.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("subdir file content mismatch: got %q, want %q", got, content)
	}
}

// ── ReadDir ───────────────────────────────────────────────────────────────────

func TestFat12ReadDir(t *testing.T) {
	_, fs := createFAT12(t, "RDIR")
	if err := fs.Mkdir("/alpha"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := fs.Mkdir("/beta"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	writeFile(t, fs, "/root.txt", []byte("x"))

	entries, err := fs.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir(.): %v", err)
	}
	names := dirEntryNames(entries)
	for _, want := range []string{"alpha", "beta", "root.txt"} {
		if !contains(names, want) {
			t.Errorf("root missing %q, got %v", want, names)
		}
	}
}

// ── Remove ────────────────────────────────────────────────────────────────────

func TestFat12Remove(t *testing.T) {
	imgPath, fs := createFAT12(t, "RMTEST")

	writeFile(t, fs, "/del.txt", []byte("bye"))

	if err := fs.Remove("/del.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// Should be gone after reopen.
	fs2 := reopenFAT12(t, imgPath)
	entries, _ := fs2.ReadDir(".")
	for _, e := range entries {
		if e.Name() == "del.txt" {
			t.Error("del.txt still present after Remove")
		}
	}
}

func TestFat12RemoveNonExistent(t *testing.T) {
	_, fs := createFAT12(t, "RMNX")
	if err := fs.Remove("/ghost.txt"); err == nil {
		t.Error("expected error removing non-existent file, got nil")
	}
}

// ── Rename ────────────────────────────────────────────────────────────────────

func TestFat12Rename(t *testing.T) {
	imgPath, fs := createFAT12(t, "RNTEST")
	content := []byte("rename me")

	writeFile(t, fs, "/old.txt", content)

	if err := fs.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	fs2 := reopenFAT12(t, imgPath)
	f2, err := fs2.OpenFile("/new.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile new.txt after rename: %v", err)
	}
	got, _ := io.ReadAll(f2)
	f2.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("renamed file content mismatch: got %q, want %q", got, content)
	}

	// Old name should be gone.
	if _, err := fs2.OpenFile("/old.txt", os.O_RDONLY); err == nil {
		t.Error("old.txt still accessible after rename")
	}
}

// ── Stat ──────────────────────────────────────────────────────────────────────

func TestFat12Stat(t *testing.T) {
	_, fs := createFAT12(t, "STATTEST")
	content := []byte("stat me")

	writeFile(t, fs, "/stat.txt", content)

	info, err := fs.Stat("stat.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Name() != "STAT.TXT" && info.Name() != "stat.txt" {
		t.Errorf("Name() = %q, want %q", info.Name(), "stat.txt")
	}
	if info.Size() != int64(len(content)) {
		t.Errorf("Size() = %d, want %d", info.Size(), len(content))
	}
	if info.IsDir() {
		t.Error("IsDir() should be false for a file")
	}
}

// ── ReadFile (fs.ReadFileFS) ──────────────────────────────────────────────────

func TestFat12ReadFile(t *testing.T) {
	content := []byte("readfile test")
	_, fs := createFAT12(t, "RFTEST")

	writeFile(t, fs, "/rf.txt", content)

	got, err := fs.ReadFile("rf.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("ReadFile mismatch: got %q, want %q", got, content)
	}
}

// ── Truncate (O_TRUNC) ────────────────────────────────────────────────────────

func TestFat12Truncate(t *testing.T) {
	_, fs := createFAT12(t, "TRUNC")
	original := []byte("original content here")
	shorter := []byte("new")

	writeFile(t, fs, "/trunc.txt", original)

	f2, _ := fs.OpenFile("/trunc.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE)
	if _, err := f2.Write(shorter); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f2.Close()

	f3, _ := fs.OpenFile("/trunc.txt", os.O_RDONLY)
	got, _ := io.ReadAll(f3)
	f3.Close()
	if !bytes.Equal(got, shorter) {
		t.Errorf("after truncate: got %q, want %q", got, shorter)
	}
}

// ── Root directory overflow ───────────────────────────────────────────────────

// TestFat12RootDirOverflow verifies that we get a clear error when the fixed
// root directory region fills up (224 entries on a 1.44 MB floppy).
func TestFat12RootDirOverflow(t *testing.T) {
	_, fs := createFAT12(t, "OVERFLOW")

	var lastErr error
	for i := range 300 {
		name := fmt.Sprintf("/f%d.txt", i)
		f, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR)
		if err != nil {
			lastErr = err
			break
		}
		f.Close()
	}
	if lastErr == nil {
		t.Error("expected error when root directory overflows, got nil")
	}
}

// ── Reproducible build ────────────────────────────────────────────────────────

func TestFat12Reproducible(t *testing.T) {
	makeFS := func(imgPath string) {
		b, err := file.OpenFromPath(imgPath, false)
		if err != nil {
			t.Fatalf("open backend: %v", err)
		}
		_, err = fat12.Create(b, floppy144, 0, 512, "REPRO", true)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	p1 := tmpImage(t, "repro1.img", floppy144)
	p2 := tmpImage(t, "repro2.img", floppy144)
	makeFS(p1)
	makeFS(p2)

	b1, err := os.ReadFile(p1)
	if err != nil {
		t.Fatalf("read image 1: %v", err)
	}
	b2, err := os.ReadFile(p2)
	if err != nil {
		t.Fatalf("read image 2: %v", err)
	}
	if !bytes.Equal(b1, b2) {
		t.Error("reproducible FAT12 images differ")
	}
}

// ── fs.FS interface ───────────────────────────────────────────────────────────

func TestFat12FSFS(t *testing.T) {
	_, fs := createFAT12(t, "FSFS")
	content := []byte("fs.FS content")

	writeFile(t, fs, "/fsfile.txt", content)

	// fs.FS Open
	rf, err := fs.Open("fsfile.txt")
	if err != nil {
		t.Fatalf("fs.FS Open: %v", err)
	}
	got, _ := io.ReadAll(rf)
	rf.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("fs.FS content mismatch: got %q, want %q", got, content)
	}

	// iofs.WalkDir
	var walked []string
	if err := fs.Mkdir("/subwalk"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	writeFile(t, fs, "/subwalk/w.txt", []byte("w"))

	err = iofs.WalkDir(fs, ".", func(p string, _ iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		walked = append(walked, p)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	if !contains(walked, "fsfile.txt") {
		t.Errorf("WalkDir missing fsfile.txt, got %v", walked)
	}
	if !contains(walked, "subwalk/w.txt") {
		t.Errorf("WalkDir missing subwalk/w.txt, got %v", walked)
	}
}

// ── Name matching regression tests ────────────────────────────────────────────

func TestReadDirWithMkdirShortNameExtension(t *testing.T) {
	_, fs := createFAT12(t, "SNDIREX")

	// "A.B" is valid 8.3 — stored as filenameShort="A", fileExtension="B", no LFN.
	if err := fs.Mkdir("/A.B"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	content := []byte("inside short dir with ext")
	writeFile(t, fs, "/A.B/file.txt", content)

	f, err := fs.OpenFile("/A.B/file.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile through short-name dir with extension: %v", err)
	}
	got, _ := io.ReadAll(f)
	f.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("content = %q, want %q", got, content)
	}
}

func TestRemoveMixedCase(t *testing.T) {
	imgPath, fs := createFAT12(t, "RMCASE")

	writeFile(t, fs, "/TestFile.txt", []byte("data"))

	if err := fs.Remove("/testfile.txt"); err != nil {
		t.Fatalf("Remove with lowercase: %v", err)
	}

	fs2 := reopenFAT12(t, imgPath)
	entries, _ := fs2.ReadDir(".")
	for _, e := range entries {
		if strings.EqualFold(e.Name(), "testfile.txt") {
			t.Error("file still present after case-insensitive Remove")
		}
	}
}

func TestRenameMixedCase(t *testing.T) {
	imgPath, fs := createFAT12(t, "RNCASE")
	content := []byte("rename me")

	writeFile(t, fs, "/Original.txt", content)

	if err := fs.Rename("/original.txt", "/moved.txt"); err != nil {
		t.Fatalf("Rename with lowercase: %v", err)
	}

	fs2 := reopenFAT12(t, imgPath)
	f, err := fs2.OpenFile("/moved.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile after case-insensitive Rename: %v", err)
	}
	got, _ := io.ReadAll(f)
	f.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("content = %q, want %q", got, content)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func dirEntryNames(entries []iofs.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
