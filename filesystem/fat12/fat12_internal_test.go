package fat12

import (
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

// newTestFloppy creates a FAT12 filesystem on a 1.44 MB image.
func newTestFloppy(t *testing.T) *FileSystem {
	t.Helper()
	const size = 1474560
	f, err := os.CreateTemp(t.TempDir(), "fat12-*.img")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	fs, err := Create(file.New(f, false), size, 0, 512, "TEST", false)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return fs
}

// clusterChainOf returns the clusters held by the entry named name in the root
// directory.
func clusterChainOf(t *testing.T, fs *FileSystem, name string) []uint32 {
	t.Helper()
	_, entries, err := fs.readDirWithMkdir("/", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.nameMatches(name) {
			chain, err := fs.getClusterList(e.clusterLocation)
			if err != nil {
				t.Fatal(err)
			}
			return chain
		}
	}
	t.Fatalf("%s is not in the root directory", name)
	return nil
}

// TestCreateLeavesClusterTwoFree: FAT12 keeps its root directory in a fixed
// region, so cluster 2 holds data like any other and starts out free.
func TestCreateLeavesClusterTwoFree(t *testing.T) {
	if got := newTestFloppy(t).table.ClusterValue(2); got != 0 {
		t.Errorf("cluster 2 holds %#x after Create, want 0", got)
	}
}

// TestRemoveFreesTheClusters: removing a file or an empty directory frees
// every cluster it held.
func TestRemoveFreesTheClusters(t *testing.T) {
	fs := newTestFloppy(t)
	f, err := fs.OpenFile("/file.bin", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 5000)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Mkdir("/dir"); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"file.bin", "dir"} {
		chain := clusterChainOf(t, fs, name)
		if err := fs.Remove("/" + name); err != nil {
			t.Fatalf("Remove %s: %v", name, err)
		}
		for _, cluster := range chain {
			if got := fs.table.ClusterValue(cluster); got != 0 {
				t.Errorf("%s: cluster %d holds %#x after Remove, want 0", name, cluster, got)
			}
		}
	}
}
