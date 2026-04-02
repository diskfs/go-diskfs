package ext4

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

func checkE2fsck(tb testing.TB) {
	tb.Helper()
	if _, err := exec.LookPath("e2fsck"); err != nil {
		tb.Skip("e2fsck not available")
	}
}

func checkMkfsExt4(tb testing.TB) {
	tb.Helper()
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		tb.Skip("mkfs.ext4 not available")
	}
}

func runE2fsck(tb testing.TB, path string) {
	tb.Helper()
	cmd := exec.Command("e2fsck", "-f", "-n", path)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		tb.Fatalf("e2fsck failed: %v\nstdout:\n%s\nstderr:\n%s",
			err, stdout.String(), stderr.String())
	}
}

func benchmarkCreate(b *testing.B, size int64, params *Params) {
	b.Helper()
	for b.Loop() {
		path := filepath.Join(b.TempDir(), "vol.img")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
		if err != nil {
			b.Fatal(err)
		}
		if err := f.Truncate(size); err != nil {
			f.Close()
			b.Fatal(err)
		}
		fs, err := Create(file.New(f, false), size, 0, 512, params)
		if err != nil {
			f.Close()
			b.Fatal(err)
		}
		fs.Close()
		f.Close()
	}
}

// BenchmarkCreate20GB benchmarks creating a 20 GB sparse ext4 filesystem
// with 4096-byte blocks and metadata checksums enabled.
//
// This is the default configuration used by containerd for writable
// container layers. Run with:
//
//	go test -bench BenchmarkCreate20GB -run '^$' -benchmem ./filesystem/ext4/
func BenchmarkCreate20GB(b *testing.B) {
	benchmarkCreate(b, 20*1024*1024*1024, &Params{
		SectorsPerBlock: 8, // 4096 byte blocks
		Features: []FeatureOpt{
			WithFeatureMetadataChecksums(true),
			WithFeatureReservedGDTBlocksForExpansion(false),
		},
	})
}

// BenchmarkCreate20GBNoJournal is the same as BenchmarkCreate20GB but
// without a journal. Run with:
//
//	go test -bench BenchmarkCreate20GBNoJournal -run '^$' -benchmem ./filesystem/ext4/
func BenchmarkCreate20GBNoJournal(b *testing.B) {
	benchmarkCreate(b, 20*1024*1024*1024, &Params{
		SectorsPerBlock: 8,
		Features: []FeatureOpt{
			WithFeatureMetadataChecksums(true),
			WithFeatureHasJournal(false),
			WithFeatureReservedGDTBlocksForExpansion(false),
		},
	})
}

// BenchmarkCreate20GBMkfsExec benchmarks the external mkfs.ext4 binary with
// lazy_itable_init and lazy_journal_init as a reference baseline. Skipped if
// mkfs.ext4 is not on PATH. Run with:
//
//	PATH="/opt/homebrew/opt/e2fsprogs/sbin:$PATH" go test -bench BenchmarkCreate20GBMkfsExec -run '^$' -benchmem ./filesystem/ext4/
func BenchmarkCreate20GBMkfsExec(b *testing.B) {
	checkMkfsExt4(b)
	for b.Loop() {
		path := filepath.Join(b.TempDir(), "vol.img")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
		if err != nil {
			b.Fatal(err)
		}
		if err := f.Truncate(20 * 1024 * 1024 * 1024); err != nil {
			f.Close()
			b.Fatal(err)
		}
		f.Close()

		cmd := exec.Command("mkfs.ext4", "-F", "-q",
			"-E", "lazy_itable_init=1,lazy_journal_init=1",
			path)
		if out, err := cmd.CombinedOutput(); err != nil {
			b.Fatalf("mkfs.ext4 failed: %v\n%s", err, out)
		}
	}
}

// TestCreateLazyInitE2fsck verifies that a filesystem created with metadata
// checksums passes e2fsck validation at various sizes. Skipped if e2fsck is
// not on PATH. Run with:
//
//	PATH="/opt/homebrew/opt/e2fsprogs/sbin:$PATH" go test -run TestCreateLazyInitE2fsck -v ./filesystem/ext4/
func TestCreateLazyInitE2fsck(t *testing.T) {
	checkE2fsck(t)

	testcases := []struct {
		name string
		size int64
		opts *Params
	}{
		{
			name: "20GB with journal",
			size: 20 * 1024 * 1024 * 1024,
			opts: &Params{
				SectorsPerBlock: 8,
				Features: []FeatureOpt{
					WithFeatureMetadataChecksums(true),
					WithFeatureReservedGDTBlocksForExpansion(false),
				},
			},
		},
		{
			name: "20GB no journal",
			size: 20 * 1024 * 1024 * 1024,
			opts: &Params{
				SectorsPerBlock: 8,
				Features: []FeatureOpt{
					WithFeatureMetadataChecksums(true),
					WithFeatureHasJournal(false),
					WithFeatureReservedGDTBlocksForExpansion(false),
				},
			},
		},
		{
			name: "512MB with journal",
			size: 512 * 1024 * 1024,
			opts: &Params{
				SectorsPerBlock: 8,
				Features: []FeatureOpt{
					WithFeatureMetadataChecksums(true),
					WithFeatureReservedGDTBlocksForExpansion(false),
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			outfile, f := testCreateEmptyFile(t, tc.size)
			defer f.Close()

			fs, err := Create(file.New(f, false), tc.size, 0, 512, tc.opts)
			if err != nil {
				t.Fatalf("Create failed: %v", err)
			}
			fs.Close()

			if err := f.Sync(); err != nil {
				t.Fatalf("Sync failed: %v", err)
			}

			runE2fsck(t, outfile)
		})
	}
}
