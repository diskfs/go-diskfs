package fat12_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat12"
	"github.com/diskfs/go-diskfs/filesystem/fat16"
	"github.com/diskfs/go-diskfs/filesystem/fat32"
	"github.com/diskfs/go-diskfs/testhelper"
)

// lazyKind is a type of FAT the lazy table is tested against. FAT32 has its
// own tests in the fat32 package, and joins these only for the tools check.
type lazyKind struct {
	name   string
	size   int64
	create func(b backend.Storage, size int64) (filesystem.FileSystem, error)
	read   func(b backend.Storage, size int64, opts ...fat12.ReadOption) (filesystem.FileSystem, error)
}

var lazyKinds = []lazyKind{
	{
		name: "FAT12",
		size: 8 * 1024 * 1024,
		create: func(b backend.Storage, size int64) (filesystem.FileSystem, error) {
			return fat12.Create(b, size, 0, 512, "LAZYFAT", false)
		},
		read: func(b backend.Storage, size int64, opts ...fat12.ReadOption) (filesystem.FileSystem, error) {
			return fat12.ReadWithOptions(b, size, 0, 512, opts...)
		},
	},
	{
		name: "FAT16",
		size: 32 * 1024 * 1024,
		create: func(b backend.Storage, size int64) (filesystem.FileSystem, error) {
			return fat16.Create(b, size, 0, 512, "LAZYFAT", false)
		},
		read: func(b backend.Storage, size int64, opts ...fat12.ReadOption) (filesystem.FileSystem, error) {
			return fat16.ReadWithOptions(b, size, 0, 512, opts...)
		},
	},
}

// lazyTestFiles are written into the image: long names, nested directories,
// and sizes on both sides of a 4 KiB boundary.
var lazyTestFiles = map[string]int{
	"/empty.bin":    0,
	"/one-byte.bin": 1,
	"/dir/sub/a-long-file-name-that-needs-lfn-entries-a.bin": 4095,
	"/dir/sub/a-long-file-name-that-needs-lfn-entries-b.bin": 4096,
	"/dir/sub/a-long-file-name-that-needs-lfn-entries-c.bin": 4097,
	"/dir/other/medium.bin":                                  22000,
	"/large.bin":                                             100000,
}

// lazyTestContent is the content written for a file: deterministic per path.
func lazyTestContent(p string, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(len(p) + i*7)
	}
	return b
}

// lazyImage creates an image of the kind holding lazyTestFiles and returns its path.
func lazyImage(t *testing.T, kind lazyKind) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "lazy-fat-*.img")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(kind.size); err != nil {
		t.Fatal(err)
	}
	fs, err := kind.create(file.New(f, false), kind.size)
	if err != nil {
		t.Fatalf("create filesystem: %v", err)
	}
	for p, size := range lazyTestFiles {
		writeLazyFile(t, fs, p, lazyTestContent(p, size))
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

// copyLazyImage copies the image file and returns the copy's path.
func copyLazyImage(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "lazy-fat-*.img")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

// openLazyImage opens the image file read-only or writable.
func openLazyImage(t *testing.T, name string, readOnly bool) backend.Storage {
	t.Helper()
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	f, err := os.OpenFile(name, flag, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return file.New(f, readOnly)
}

// writeLazyFile writes content to p through a writable filesystem.
func writeLazyFile(t *testing.T, fs filesystem.FileSystem, p string, content []byte) {
	t.Helper()
	if dir := path.Dir(p); dir != "/" {
		if err := fs.Mkdir(dir); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	openAndWriteLazy(t, fs, p, os.O_CREATE|os.O_RDWR, content)
}

// openAndWriteLazy opens p with flag and writes content to it.
func openAndWriteLazy(t *testing.T, fs filesystem.FileSystem, p string, flag int, content []byte) {
	t.Helper()
	f, err := fs.OpenFile(p, flag)
	if err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", p, err)
	}
}

// readLazyTree returns every file under dir with its content, keyed by its
// path from the root with a leading slash. ReadDir takes io/fs paths, where
// the root is "." and nothing starts with a slash.
func readLazyTree(t *testing.T, fs filesystem.FileSystem, dir string, into map[string][]byte) {
	t.Helper()
	entries, err := fs.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir %s: %v", dir, err)
	}
	for _, entry := range entries {
		p := path.Join(dir, entry.Name())
		if entry.IsDir() {
			readLazyTree(t, fs, p, into)
			continue
		}
		p = "/" + p
		f, err := fs.OpenFile(p, os.O_RDONLY)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		content, err := io.ReadAll(f)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		into[p] = content
	}
}

// changeLazyTree grows, truncates and removes files lazyImage wrote, then
// writes a new file into the clusters that freed. It returns every file the
// image then holds, with its content.
func changeLazyTree(t *testing.T, fs filesystem.FileSystem) map[string][]byte {
	t.Helper()
	const (
		grown     = "/large.bin"
		truncated = "/dir/other/medium.bin"
		removed   = "/dir/sub/a-long-file-name-that-needs-lfn-entries-b.bin"
		added     = "/dir/new/written-after-changes.bin"
	)
	files := map[string][]byte{}
	for p, size := range lazyTestFiles {
		files[p] = lazyTestContent(p, size)
	}

	appended := lazyTestContent(grown+"+", 50000)
	openAndWriteLazy(t, fs, grown, os.O_RDWR|os.O_APPEND, appended)
	files[grown] = append(files[grown], appended...)

	files[truncated] = lazyTestContent(truncated+"+", 3000)
	openAndWriteLazy(t, fs, truncated, os.O_RDWR|os.O_TRUNC, files[truncated])

	if err := fs.Remove(removed); err != nil {
		t.Fatalf("remove %s: %v", removed, err)
	}
	delete(files, removed)

	files[added] = lazyTestContent(added, 70000)
	writeLazyFile(t, fs, added, files[added])

	return files
}

// expectTree fails the test unless got holds exactly the files in want.
func expectTree(t *testing.T, label string, got, want map[string][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s lists %d files, want %d", label, len(got), len(want))
	}
	for p, content := range want {
		if !bytes.Equal(got[p], content) {
			t.Errorf("%s: %s is %d bytes, not the %d written", label, p, len(got[p]), len(content))
		}
	}
}

// corruptSecondaryFAT changes one byte of the secondary FAT of a FAT12 or FAT16 image.
func corruptSecondaryFAT(t *testing.T, name string) {
	t.Helper()
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	bs := make([]byte, 512)
	if _, err := f.ReadAt(bs, 0); err != nil {
		t.Fatal(err)
	}
	bytesPerSector := int64(binary.LittleEndian.Uint16(bs[11:]))
	reservedSectors := int64(binary.LittleEndian.Uint16(bs[14:]))
	sectorsPerFat := int64(binary.LittleEndian.Uint16(bs[22:]))
	at := (reservedSectors+sectorsPerFat)*bytesPerSector + 10
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, at); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, at); err != nil {
		t.Fatal(err)
	}
}

// TestLazyFATReadsWhatTheFullTableReads: the same image read with the whole
// table in memory and with entries read on demand lists the files written,
// with the content written.
func TestLazyFATReadsWhatTheFullTableReads(t *testing.T) {
	for _, kind := range lazyKinds {
		t.Run(kind.name, func(t *testing.T) {
			name := lazyImage(t, kind)
			full, err := kind.read(openLazyImage(t, name, true), kind.size)
			if err != nil {
				t.Fatalf("read with the full table: %v", err)
			}
			lazy, err := kind.read(openLazyImage(t, name, true), kind.size, fat12.WithLazyFAT())
			if err != nil {
				t.Fatalf("read with a lazy table: %v", err)
			}

			want := map[string][]byte{}
			for p, size := range lazyTestFiles {
				want[p] = lazyTestContent(p, size)
			}
			fullTree, lazyTree := map[string][]byte{}, map[string][]byte{}
			readLazyTree(t, full, ".", fullTree)
			readLazyTree(t, lazy, ".", lazyTree)
			expectTree(t, "the full table", fullTree, want)
			expectTree(t, "the lazy table", lazyTree, want)
		})
	}
}

// TestLazyFATFindsAFileWrittenAfterItWasRead: another writer adds a file after
// the filesystem was read, and the lazy table reads its clusters as they are now.
func TestLazyFATFindsAFileWrittenAfterItWasRead(t *testing.T) {
	for _, kind := range lazyKinds {
		t.Run(kind.name, func(t *testing.T) {
			name := lazyImage(t, kind)
			lazy, err := kind.read(openLazyImage(t, name, true), kind.size, fat12.WithLazyFAT())
			if err != nil {
				t.Fatalf("read with a lazy table: %v", err)
			}
			writer, err := kind.read(openLazyImage(t, name, false), kind.size)
			if err != nil {
				t.Fatalf("read for writing: %v", err)
			}
			const late = "/dir/later/written-after-read.bin"
			content := lazyTestContent(late, 9000)
			writeLazyFile(t, writer, late, content)

			f, err := lazy.OpenFile(late, os.O_RDONLY)
			if err != nil {
				t.Fatalf("open %s: %v", late, err)
			}
			if got, err := io.ReadAll(f); err != nil || !bytes.Equal(got, content) {
				t.Errorf("%s read %d bytes (%v), not the %d written", late, len(got), err, len(content))
			}
		})
	}
}

// TestLazyFATWritesWhatTheFullTableWrites: the same changes made through the
// full table and through the lazy table leave byte-identical images, and the
// files read back as written.
func TestLazyFATWritesWhatTheFullTableWrites(t *testing.T) {
	t.Setenv("SOURCE_DATE_EPOCH", "1700000000")
	for _, kind := range lazyKinds {
		t.Run(kind.name, func(t *testing.T) {
			fullName := lazyImage(t, kind)
			lazyName := copyLazyImage(t, fullName)

			full, err := kind.read(openLazyImage(t, fullName, false), kind.size)
			if err != nil {
				t.Fatalf("read with the full table: %v", err)
			}
			lazy, err := kind.read(openLazyImage(t, lazyName, false), kind.size, fat12.WithLazyFAT())
			if err != nil {
				t.Fatalf("read with a lazy table: %v", err)
			}
			changeLazyTree(t, full)
			want := changeLazyTree(t, lazy)

			fullBytes, err := os.ReadFile(fullName)
			if err != nil {
				t.Fatal(err)
			}
			lazyBytes, err := os.ReadFile(lazyName)
			if err != nil {
				t.Fatal(err)
			}
			for i := range fullBytes {
				if fullBytes[i] != lazyBytes[i] {
					t.Fatalf("images differ from byte %d", i)
				}
			}

			written, err := kind.read(openLazyImage(t, lazyName, true), kind.size, fat12.WithFATCheck())
			if err != nil {
				t.Fatalf("read the image the lazy table wrote: %v", err)
			}
			got := map[string][]byte{}
			readLazyTree(t, written, ".", got)
			expectTree(t, "the image the lazy table wrote", got, want)
		})
	}
}

// toolsImage is the docker image holding dosfstools and mtools, which make test
// builds and passes in TEST_IMAGE.
var toolsImage = os.Getenv("TEST_IMAGE")

// fsckComplaints returns what fsck.vfat -n found wrong, one line each: its
// output without the version banner, the closing summary and blank lines.
func fsckComplaints(output string) []string {
	lines := strings.Split(output, "\n")
	complaints := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		switch {
		case line == "", strings.HasPrefix(line, "fsck.fat "), strings.HasPrefix(line, "/file.img:"),
			line == "Leaving filesystem unchanged.":
			continue
		}
		complaints = append(complaints, line)
	}
	return complaints
}

// TestLazyFATWritesWhatTheToolsAccept: after the lazy table appends to,
// truncates, removes and adds files, fsck.vfat finds nothing to repair and
// mtools reads every file back as written, for every FAT width.
func TestLazyFATWritesWhatTheToolsAccept(t *testing.T) {
	if toolsImage == "" {
		t.Skip("TEST_IMAGE is not set to an image with dosfstools and mtools")
	}
	fat32Kind := lazyKind{
		name: "FAT32",
		size: 64 * 1024 * 1024,
		create: func(b backend.Storage, size int64) (filesystem.FileSystem, error) {
			return fat32.Create(b, size, 0, 512, "LAZYFAT", false)
		},
		read: func(b backend.Storage, size int64, opts ...fat12.ReadOption) (filesystem.FileSystem, error) {
			return fat32.ReadWithOptions(b, size, 0, 512, opts...)
		},
	}
	for _, kind := range append(slices.Clone(lazyKinds), fat32Kind) {
		t.Run(kind.name, func(t *testing.T) {
			name := lazyImage(t, kind)
			lazy, err := kind.read(openLazyImage(t, name, false), kind.size, fat12.WithLazyFAT())
			if err != nil {
				t.Fatalf("read with a lazy table: %v", err)
			}
			want := changeLazyTree(t, lazy)
			mounts := map[string]string{name: "/file.img"}

			output := new(bytes.Buffer)
			_ = testhelper.DockerRun(nil, output, false, true, mounts, toolsImage, "fsck.vfat", "-n", "/file.img")
			for _, complaint := range fsckComplaints(output.String()) {
				// Create writes the FAT32 free cluster count as unknown,
				// 0xFFFFFFFF, which the FAT specification allows.
				if kind.name == "FAT32" && strings.HasPrefix(complaint, "Free cluster summary uninitialized") {
					continue
				}
				t.Errorf("fsck.vfat: %s", complaint)
			}

			var script, wantSums strings.Builder
			for _, p := range slices.Sorted(maps.Keys(want)) {
				fmt.Fprintf(&script, "mtype -i /file.img '::%s' | sha256sum\n", p)
				fmt.Fprintf(&wantSums, "%x  -\n", sha256.Sum256(want[p]))
			}
			output.Reset()
			if err := testhelper.DockerRun(nil, output, false, true, mounts, toolsImage, "sh", "-c", script.String()); err != nil {
				t.Fatalf("mtype: %v\n%s", err, output)
			}
			if output.String() != wantSums.String() {
				t.Errorf("mtools read back\n%s\nwant\n%s", output, wantSums.String())
			}
		})
	}
}

// TestFATCheckRefusesDifferingCopies: WithFATCheck accepts an image whose two
// copies of the FAT match and refuses one where they differ, whichever table
// reads it.
func TestFATCheckRefusesDifferingCopies(t *testing.T) {
	checks := map[string][]fat12.ReadOption{
		"full table": {fat12.WithFATCheck()},
		"lazy table": {fat12.WithLazyFAT(), fat12.WithFATCheck()},
	}
	for _, kind := range lazyKinds {
		t.Run(kind.name, func(t *testing.T) {
			name := lazyImage(t, kind)
			for label, opts := range checks {
				if _, err := kind.read(openLazyImage(t, name, true), kind.size, opts...); err != nil {
					t.Errorf("%s refused matching copies: %v", label, err)
				}
			}

			corruptSecondaryFAT(t, name)
			for label, opts := range checks {
				if _, err := kind.read(openLazyImage(t, name, true), kind.size, opts...); err == nil {
					t.Errorf("%s accepted copies that differ", label)
				}
			}
			if _, err := kind.read(openLazyImage(t, name, true), kind.size, fat12.WithLazyFAT()); err != nil {
				t.Errorf("without the check, the lazy table refused the image: %v", err)
			}
		})
	}
}
