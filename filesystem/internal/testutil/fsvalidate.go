package testutil

import (
	iofs "io/fs"
	"strings"
	"testing"
)

func TestFSTree(t *testing.T, fs iofs.ReadDirFS) {
	t.Helper()
	var seen map[string]struct{}
	var walk func(path string)
	walk = func(path string) {
		if _, ok := seen[path]; ok {
			t.Fatalf("cycle detected: revisiting path %q", path)
		}

		entries, err := fs.ReadDir(path)
		if err != nil {
			return // not a directory
		}
		seen[path] = struct{}{}

		for _, e := range entries {
			name := e.Name()

			if name == "." || name == ".." {
				t.Fatalf("illegal entry %q in %q", name, path)
			}

			if strings.Contains(name, "/") {
				t.Fatalf("entry name %q in %q is not a base name", name, path)
			}

			var child string
			if path == "." {
				child = name
			} else {
				child = path + "/" + name
			}

			if e.IsDir() {
				walk(child)
			}
		}
	}

	t.Run("dot", func(t *testing.T) {
		seen = map[string]struct{}{}
		walk(".")
		if len(seen) == 0 {
			t.Fatalf("no files seen during walk")
		}
	})
	t.Run("slash", func(t *testing.T) {
		seen = map[string]struct{}{}
		walk("/")
		if len(seen) != 0 {
			t.Fatalf("files seen during walk")
		}
	})
}
