package fat16_test

import (
	"bytes"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat12"
	"github.com/diskfs/go-diskfs/filesystem/fat16"
)

// ── helpers ───────────────────────────────────────────────────────────────────

const (
	size32MB  = int64(32 * 1024 * 1024)
	size128MB = int64(128 * 1024 * 1024)
	size512MB = int64(512 * 1024 * 1024)
)

func tmpImage(t *testing.T, name string, size int64) string {
	t.Helper()
	p := path.Join(t.TempDir(), name)
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create temp image %s: %v", p, err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatalf("truncate %s to %d: %v", p, size, err)
	}
	return p
}

func createFAT16(t *testing.T, label string) (imgPath string, fs filesystem.FileSystem) {
	t.Helper()
	imgPath = tmpImage(t, "fat16.img", size32MB)
	b, err := file.OpenFromPath(imgPath, false)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	fs, err = fat16.Create(b, size32MB, 0, 512, label, false)
	if err != nil {
		t.Fatalf("fat16.Create: %v", err)
	}
	return imgPath, fs
}

func reopenFAT16(t *testing.T, imgPath string) filesystem.FileSystem {
	t.Helper()
	b, err := file.OpenFromPath(imgPath, false)
	if err != nil {
		t.Fatalf("open backend for re-read: %v", err)
	}
	fs, err := fat16.Read(b, size32MB, 0, 512)
	if err != nil {
		t.Fatalf("fat16.Read: %v", err)
	}
	return fs
}

func dirEntryNames(entries []iofs.DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
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

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// ── Create ────────────────────────────────────────────────────────────────────

func TestFat16Create(t *testing.T) {
	tests := []struct {
		name      string
		size      int64
		blocksize int64
		wantErr   string
	}{
		{"valid 32MB", size32MB, 512, ""},
		{"valid 128MB", size128MB, 512, ""},
		{"valid 512MB", size512MB, 512, ""},
		{"valid blocksize 0", size32MB, 0, ""},
		{"bad blocksize", size32MB, 1024, "blocksize for FAT16 must be"},
		{"too large", fat12.Fat16MaxSize + 1, 512, "exceeds FAT16 maximum"},
		{"too small", 512, 512, "too small"},
		// A volume small enough to only have FAT12 cluster counts should fail.
		{"too small for FAT16 cluster count", int64(1 * 1024 * 1024), 512, "too small for FAT16"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocSize := tt.size
			if allocSize < size32MB {
				allocSize = size32MB // ensure the file is large enough to hold zeroes
			}
			imgPath := tmpImage(t, "fat16.img", allocSize)
			b, err := file.OpenFromPath(imgPath, false)
			if err != nil {
				t.Fatalf("open backend: %v", err)
			}
			_, err = fat16.Create(b, tt.size, 0, tt.blocksize, "TEST", false)
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

func TestFat16Type(t *testing.T) {
	_, fs := createFAT16(t, "TYPTEST")
	if got := fs.Type(); got != filesystem.TypeFat16 {
		t.Errorf("Type() = %v, want TypeFat16 (%d)", got, filesystem.TypeFat16)
	}
}

// TestFat16TypeNotFat12 confirms that fat16.FileSystem does not claim to be FAT12.
func TestFat16TypeNotFat12(t *testing.T) {
	_, fs := createFAT16(t, "NOTF12")
	if fs.Type() == filesystem.TypeFat12 {
		t.Error("Type() should not be TypeFat12 for a FAT16 filesystem")
	}
}

// ── Read (self-validation) ────────────────────────────────────────────────────

func TestFat16Read(t *testing.T) {
	t.Run("reads back a created FAT16 image", func(t *testing.T) {
		imgPath, _ := createFAT16(t, "READTEST")
		fs := reopenFAT16(t, imgPath)
		if fs.Type() != filesystem.TypeFat16 {
			t.Errorf("reopened Type() = %v, want TypeFat16", fs.Type())
		}
	})

	t.Run("rejects a FAT12 image", func(t *testing.T) {
		// Create a FAT12 image and try to open it as FAT16.
		imgPath := tmpImage(t, "fat12.img", int64(1474560))
		b, err := file.OpenFromPath(imgPath, false)
		if err != nil {
			t.Fatalf("open backend: %v", err)
		}
		if _, err = fat12.Create(b, 1474560, 0, 512, "F12", false); err != nil {
			t.Fatalf("fat12.Create: %v", err)
		}
		b2, _ := file.OpenFromPath(imgPath, false)
		_, err = fat16.Read(b2, 1474560, 0, 512)
		if err == nil {
			t.Error("fat16.Read should reject a FAT12 image, got nil error")
		}
	})
}

// ── Label ─────────────────────────────────────────────────────────────────────

func TestFat16Label(t *testing.T) {
	const label = "VOLNAME"
	imgPath, fs := createFAT16(t, label)

	got := strings.TrimSpace(fs.Label())
	if got != label {
		t.Errorf("Label() = %q, want %q", got, label)
	}

	fs2 := reopenFAT16(t, imgPath)
	got2 := strings.TrimSpace(fs2.Label())
	if got2 != label {
		t.Errorf("after reopen: Label() = %q, want %q", got2, label)
	}
}

func TestFat16SetLabel(t *testing.T) {
	imgPath, fs := createFAT16(t, "OLD")
	if err := fs.SetLabel("NEWLABEL"); err != nil {
		t.Fatalf("SetLabel: %v", err)
	}
	fs2 := reopenFAT16(t, imgPath)
	if got := strings.TrimSpace(fs2.Label()); got != "NEWLABEL" {
		t.Errorf("after SetLabel+reopen: Label() = %q, want NEWLABEL", got)
	}
}

// ── Mkdir ─────────────────────────────────────────────────────────────────────

func TestFat16Mkdir(t *testing.T) {
	dirs := []string{"/foo", "/foo/bar", "/a/b/c"}
	imgPath, fs := createFAT16(t, "MKDIR")

	for _, d := range dirs {
		if err := fs.Mkdir(d); err != nil {
			t.Errorf("Mkdir(%q): %v", d, err)
		}
	}

	fs2 := reopenFAT16(t, imgPath)
	entries, err := fs2.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}
	names := dirEntryNames(entries)
	for _, want := range []string{"foo", "a"} {
		if !contains(names, want) {
			t.Errorf("root missing %q, got %v", want, names)
		}
	}
}

// ── OpenFile / Write / Read ───────────────────────────────────────────────────

func TestFat16WriteReadFile(t *testing.T) {
	content := []byte("Hello, FAT16!")
	imgPath, fs := createFAT16(t, "WRTEST")

	f, err := fs.OpenFile("/hello.txt", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile for write: %v", err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f.Close()

	// Same instance.
	f2, _ := fs.OpenFile("/hello.txt", os.O_RDONLY)
	got, _ := io.ReadAll(f2)
	f2.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("same-instance read mismatch: got %q, want %q", got, content)
	}

	// Across reopen.
	fs2 := reopenFAT16(t, imgPath)
	f3, err := fs2.OpenFile("/hello.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile after reopen: %v", err)
	}
	got2, _ := io.ReadAll(f3)
	f3.Close()
	if !bytes.Equal(got2, content) {
		t.Errorf("reopen read mismatch: got %q, want %q", got2, content)
	}
}

func TestFat16WriteReadLargeFile(t *testing.T) {
	// 64 KB — spans many clusters.
	content := bytes.Repeat([]byte("FAT16DATA"), 7282) // ~64 KB
	content = content[:64*1024]
	_, fs := createFAT16(t, "LARGE")

	writeFile(t, fs, "/large.bin", content)

	f2, _ := fs.OpenFile("/large.bin", os.O_RDONLY)
	got, _ := io.ReadAll(f2)
	f2.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("large file mismatch (len: got %d, want %d)", len(got), len(content))
	}
}

func TestFat16WriteReadSubdirFile(t *testing.T) {
	content := []byte("deep file content")
	imgPath, fs := createFAT16(t, "DEEP")

	if err := fs.Mkdir("/a/b/c"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	writeFile(t, fs, "/a/b/c/deep.txt", content)

	fs2 := reopenFAT16(t, imgPath)
	f2, err := fs2.OpenFile("/a/b/c/deep.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile after reopen: %v", err)
	}
	got, _ := io.ReadAll(f2)
	f2.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("deep file mismatch: got %q, want %q", got, content)
	}
}

// ── Remove ────────────────────────────────────────────────────────────────────

func TestFat16Remove(t *testing.T) {
	imgPath, fs := createFAT16(t, "RMTEST")

	writeFile(t, fs, "/del.txt", []byte("delete me"))

	if err := fs.Remove("/del.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	fs2 := reopenFAT16(t, imgPath)
	entries, _ := fs2.ReadDir(".")
	for _, e := range entries {
		if e.Name() == "del.txt" {
			t.Error("del.txt still present after Remove")
		}
	}
}

func TestFat16RemoveNonExistent(t *testing.T) {
	_, fs := createFAT16(t, "RMNX")
	if err := fs.Remove("/ghost.txt"); err == nil {
		t.Error("expected error removing non-existent file, got nil")
	}
}

// ── Rename ────────────────────────────────────────────────────────────────────

func TestFat16Rename(t *testing.T) {
	content := []byte("rename me")
	imgPath, fs := createFAT16(t, "RNTEST")

	writeFile(t, fs, "/old.txt", content)

	if err := fs.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	fs2 := reopenFAT16(t, imgPath)
	f2, err := fs2.OpenFile("/new.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile new.txt after rename: %v", err)
	}
	got, _ := io.ReadAll(f2)
	f2.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("renamed content mismatch: got %q, want %q", got, content)
	}

	if _, err := fs2.OpenFile("/old.txt", os.O_RDONLY); err == nil {
		t.Error("old.txt still accessible after rename")
	}
}

// ── Stat ──────────────────────────────────────────────────────────────────────

func TestFat16Stat(t *testing.T) {
	_, fs := createFAT16(t, "STATTEST")
	content := []byte("stat this")

	writeFile(t, fs, "/stat.txt", content)

	info, err := fs.Stat("stat.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != int64(len(content)) {
		t.Errorf("Size() = %d, want %d", info.Size(), len(content))
	}
	if info.IsDir() {
		t.Error("IsDir() should be false for a file")
	}

	// Stat a directory.
	if err := fs.Mkdir("/statdir"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	dinfo, err := fs.Stat("statdir")
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if !dinfo.IsDir() {
		t.Error("IsDir() should be true for a directory")
	}
}

// ── Truncate ──────────────────────────────────────────────────────────────────

func TestFat16Truncate(t *testing.T) {
	_, fs := createFAT16(t, "TRUNC")
	original := bytes.Repeat([]byte("ABCD"), 1024) // 4 KB
	shorter := []byte("short")

	writeFile(t, fs, "/t.bin", original)

	f2, _ := fs.OpenFile("/t.bin", os.O_RDWR|os.O_TRUNC|os.O_CREATE)
	if _, err := f2.Write(shorter); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f2.Close()

	f3, _ := fs.OpenFile("/t.bin", os.O_RDONLY)
	got, _ := io.ReadAll(f3)
	f3.Close()
	if !bytes.Equal(got, shorter) {
		t.Errorf("after truncate: got %q, want %q", got, shorter)
	}
}

// ── ReadFile ──────────────────────────────────────────────────────────────────

func TestFat16ReadFile(t *testing.T) {
	content := []byte("readfile test fat16")
	_, fs := createFAT16(t, "RFTEST")

	writeFile(t, fs, "/rf.txt", content)

	got, err := fs.ReadFile("rf.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("ReadFile mismatch: got %q, want %q", got, content)
	}
}

// ── Reproducible build ────────────────────────────────────────────────────────

func TestFat16Reproducible(t *testing.T) {
	make16 := func(imgPath string) {
		b, _ := file.OpenFromPath(imgPath, false)
		if _, err := fat16.Create(b, size32MB, 0, 512, "REPRO", true); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}
	p1 := tmpImage(t, "repro1.img", size32MB)
	p2 := tmpImage(t, "repro2.img", size32MB)
	make16(p1)
	make16(p2)

	b1, _ := os.ReadFile(p1)
	b2, _ := os.ReadFile(p2)
	if !bytes.Equal(b1, b2) {
		t.Error("reproducible FAT16 images differ")
	}
}

// ── fs.FS interface ───────────────────────────────────────────────────────────

func TestFat16FSFS(t *testing.T) {
	_, fs := createFAT16(t, "FSFS")
	content := []byte("fs.FS via fat16")

	writeFile(t, fs, "/fsfile.txt", content)

	rf, err := fs.Open("fsfile.txt")
	if err != nil {
		t.Fatalf("fs.FS Open: %v", err)
	}
	got, _ := io.ReadAll(rf)
	rf.Close()
	if !bytes.Equal(got, content) {
		t.Errorf("fs.FS content mismatch: got %q, want %q", got, content)
	}

	// WalkDir should find files in subdirectories.
	if err := fs.Mkdir("/walk"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	writeFile(t, fs, "/walk/w.txt", []byte("walk"))

	var walked []string
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
	if !contains(walked, "walk/w.txt") {
		t.Errorf("WalkDir missing walk/w.txt, got %v", walked)
	}
}

// ── Multiple files and directories ───────────────────────────────────────────

func TestFat16ManyFiles(t *testing.T) {
	imgPath, fs := createFAT16(t, "MANY")

	const nDirs = 5
	const nFilesPerDir = 10

	for d := range nDirs {
		dir := strings.ToLower(string(rune('a' + d)))
		if err := fs.Mkdir("/" + dir); err != nil {
			t.Fatalf("Mkdir /%s: %v", dir, err)
		}
		for fi := range nFilesPerDir {
			name := strings.ToLower(string(rune('a'+d))) + strings.Repeat("x", fi) + ".txt"
			f, err := fs.OpenFile("/"+dir+"/"+name, os.O_CREATE|os.O_RDWR)
			if err != nil {
				t.Fatalf("OpenFile /%s/%s: %v", dir, name, err)
			}
			if _, err := f.Write([]byte(dir + "/" + name)); err != nil {
				t.Fatalf("Write: %v", err)
			}
			f.Close()
		}
	}

	fs2 := reopenFAT16(t, imgPath)
	for d := range nDirs {
		dir := strings.ToLower(string(rune('a' + d)))
		entries, err := fs2.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir %s after reopen: %v", dir, err)
		}
		if len(entries) != nFilesPerDir {
			t.Errorf("dir %s: got %d entries, want %d", dir, len(entries), nFilesPerDir)
		}
	}
}
