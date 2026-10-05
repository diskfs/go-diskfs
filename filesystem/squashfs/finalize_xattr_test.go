package squashfs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
	"github.com/diskfs/go-diskfs/testhelper"
	"github.com/pkg/xattr"
)

// finalizeWithXattrs writes one file "a" with the given host xattrs and
// finalizes the filesystem with the given options.
func finalizeWithXattrs(t *testing.T, attrs map[string]string, options squashfs.FinalizeOptions) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "squashfs_xattr_test")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	fs, err := squashfs.Create(file.New(f, false), 0, 0, 4096)
	if err != nil {
		t.Fatalf("Failed to squashfs.Create: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fs.Workspace(), "a"), []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for k, v := range attrs {
		if err := xattr.Set(filepath.Join(fs.Workspace(), "a"), k, []byte(v)); err != nil {
			t.Skipf("workspace does not support xattrs: %v", err)
		}
	}
	if err := fs.Finalize(options); err != nil {
		t.Fatalf("unexpected error fs.Finalize(): %v", err)
	}
	return f
}

func readXattrs(t *testing.T, f *os.File) map[string]string {
	t.Helper()
	fs, err := squashfs.Read(file.New(f, true), 0, 0, 4096)
	if err != nil {
		t.Fatalf("error reading the tmpfile as squashfs: %v", err)
	}
	fi, err := fs.Stat("a")
	if err != nil {
		t.Fatalf("error stat a: %v", err)
	}
	sys, ok := fi.Sys().(*squashfs.StatT)
	if !ok {
		t.Fatalf("could not convert fi.Sys() to *StatT")
	}
	return sys.Xattrs
}

func TestFinalizeXattrs(t *testing.T) {
	attrs := map[string]string{"user.abc": "def", "user.myattr": "hello", "user.zzz": "three"}

	t.Run("written", func(t *testing.T) {
		f := finalizeWithXattrs(t, attrs, squashfs.FinalizeOptions{Xattrs: true})
		got := readXattrs(t, f)
		for k, v := range attrs {
			// the reader returns names without their namespace prefix
			k = strings.TrimPrefix(k, "user.")
			if got[k] != v {
				t.Errorf("xattr %s = %q, expected %q (all: %v)", k, got[k], v, got)
			}
		}
		if intImage == "" {
			return
		}
		// unsquashfs exits non-zero when it cannot read the xattr table
		mounts := map[string]string{f.Name(): "/file.sqs"}
		output := new(strings.Builder)
		if err := testhelper.DockerRun(nil, output, false, true, mounts, intImage, "unsquashfs", "-d", "/tmp/out", "/file.sqs"); err != nil {
			t.Errorf("unsquashfs: %v\n%s", err, output)
		}
	})

	t.Run("not asked for", func(t *testing.T) {
		f := finalizeWithXattrs(t, attrs, squashfs.FinalizeOptions{})
		if got := readXattrs(t, f); len(got) != 0 {
			t.Errorf("xattrs %v, expected none", got)
		}
	})
}

// FileXattrs are not written without Xattrs.
func TestFinalizeFileXattrsWithoutXattrs(t *testing.T) {
	f := finalizeWithXattrs(t, nil, squashfs.FinalizeOptions{
		FileXattrs: map[string]map[string]string{"a": {"user.abc": "def"}},
	})
	if got := readXattrs(t, f); len(got) != 0 {
		t.Errorf("xattrs %v, expected none", got)
	}
}

// Perms and FileXattrs set modes and xattrs per path, without the host
// storing them.
func TestFinalizePermsAndFileXattrs(t *testing.T) {
	label := "system_u:object_r:system_state_t:s0"
	f := finalizeWithXattrs(t, nil, squashfs.FinalizeOptions{
		Xattrs:     true,
		Perms:      map[string]os.FileMode{"a": 0o755},
		FileXattrs: map[string]map[string]string{"a": {"security.selinux": label}},
	})
	fs, err := squashfs.Read(file.New(f, true), 0, 0, 4096)
	if err != nil {
		t.Fatalf("error reading the tmpfile as squashfs: %v", err)
	}
	fi, err := fs.Stat("a")
	if err != nil {
		t.Fatalf("error stat a: %v", err)
	}
	if fi.Mode() != 0o755 {
		t.Errorf("mode %v, expected %v", fi.Mode(), os.FileMode(0o755))
	}
	// the reader returns names without their namespace prefix
	if got := readXattrs(t, f); !squashfs.CompareEqualMapStringString(got, map[string]string{"selinux": label}) {
		t.Errorf("xattrs %v, expected selinux=%s", got, label)
	}
}
