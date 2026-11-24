package squashfs

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

func testDirEntries(n int) []*directoryEntryRaw {
	entries := make([]*directoryEntryRaw, n)
	for i := range entries {
		entries[i] = &directoryEntryRaw{name: fmt.Sprintf("e%d", i)}
	}
	return entries
}

func TestDirCache(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var c *dirCache
		c.add("a", testDirEntries(1))
		if _, ok := c.get("a"); ok {
			t.Errorf("nil cache returned an entry")
		}
	})

	t.Run("evicts least recently used", func(t *testing.T) {
		c := newDirCache(10)
		c.add("a", testDirEntries(4))
		c.add("b", testDirEntries(4))
		// use a, so that b is the least recently used
		if _, ok := c.get("a"); !ok {
			t.Fatalf("a missing")
		}
		c.add("c", testDirEntries(4))
		if _, ok := c.get("b"); ok {
			t.Errorf("b should have been evicted")
		}
		for _, p := range []string{"a", "c"} {
			if _, ok := c.get(p); !ok {
				t.Errorf("%s missing", p)
			}
		}
		if c.entries != 8 {
			t.Errorf("entries %d, expected 8", c.entries)
		}
	})

	t.Run("bounds empty directories", func(t *testing.T) {
		c := newDirCache(2)
		for _, p := range []string{"a", "b", "c"} {
			c.add(p, nil)
		}
		if len(c.items) != 2 || c.entries != 2 {
			t.Errorf("%d directories and %d entries, expected 2 and 2", len(c.items), c.entries)
		}
	})

	t.Run("skips directories larger than the cache", func(t *testing.T) {
		c := newDirCache(2)
		c.add("a", testDirEntries(1))
		c.add("big", testDirEntries(3))
		if _, ok := c.get("big"); ok {
			t.Errorf("big should not be cached")
		}
		if _, ok := c.get("a"); !ok {
			t.Errorf("a should not have been evicted for a directory that is not cached")
		}
	})

	t.Run("adding twice counts once", func(t *testing.T) {
		c := newDirCache(10)
		c.add("a", testDirEntries(3))
		c.add("a", testDirEntries(3))
		if c.entries != 3 {
			t.Errorf("entries %d, expected 3", c.entries)
		}
	})
}

// testListAll lists every directory of fs, depth first, and returns the listings, one line per entry.
func testListAll(t *testing.T, fs *FileSystem) []string {
	t.Helper()
	var (
		lines []string
		list  func(dir string)
	)
	list = func(dir string) {
		entries, err := fs.readDirectory(dir)
		if err != nil {
			t.Fatalf("readDirectory(%q): %v", dir, err)
		}
		for _, e := range entries {
			p := path.Join(dir, e.name)
			lines = append(lines, fmt.Sprintf("%s %v %d %v %x", p, e.mode, e.size, e.modTime, e.inode.toBytes()))
			if e.IsDir() {
				list(p)
			}
		}
	}
	list(".")
	return lines
}

// TestDirCacheListing checks that listings are the same with the directory cache, cold or warm, as without it, and
// that a path that does not exist is still refused once its parent is cached.
func TestDirCacheListing(t *testing.T) {
	f, err := os.Open(Squashfsfile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fs, err := Read(file.New(f, true), 0, 0, 0)
	if err != nil {
		t.Fatalf("error reading test filesystem: %v", err)
	}
	if fs.dirCache == nil {
		t.Fatalf("Read did not set up a directory cache")
	}
	cached := fs.dirCache

	fs.dirCache = nil
	expected := testListAll(t, fs)

	fs.dirCache = cached
	for _, run := range []string{"cold", "warm"} {
		actual := testListAll(t, fs)
		if len(actual) != len(expected) {
			t.Fatalf("%s: %d entries, expected %d", run, len(actual), len(expected))
		}
		for i := range actual {
			if actual[i] != expected[i] {
				t.Errorf("%s: entry %d is %q, expected %q", run, i, actual[i], expected[i])
			}
		}
	}

	for _, p := range []string{"doesnotexist", "a/doesnotexist", "random/largefile"} {
		if _, err := fs.readDirectory(p); err == nil {
			t.Errorf("readDirectory(%q): expected an error", p)
		}
	}
}

// TestDirCacheConcurrent lists the image from several goroutines sharing a small cache, for the race detector.
func TestDirCacheConcurrent(t *testing.T) {
	fs, _, err := testGetFilesystem(nil)
	if err != nil {
		t.Fatalf("error getting valid test filesystem: %v", err)
	}
	fs.dirCache = nil
	expected := testListAll(t, fs)
	// small enough that goroutines evict each other's directories
	fs.dirCache = newDirCache(8)

	var wg sync.WaitGroup
	results := make([][]string, 8)
	for i := range results {
		wg.Go(func() {
			results[i] = testListAll(t, fs)
		})
	}
	wg.Wait()
	for i, actual := range results {
		if !bytes.Equal([]byte(fmt.Sprint(actual)), []byte(fmt.Sprint(expected))) {
			t.Errorf("goroutine %d: mismatched listing", i)
		}
	}
}
