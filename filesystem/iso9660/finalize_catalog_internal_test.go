package iso9660

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

// finalizeCatalogImage writes the files below workspace/isolinux, finalizes an
// image with the options and returns the image with a reader for it.
func finalizeCatalogImage(t *testing.T, files map[string]string, options FinalizeOptions) (*os.File, *FileSystem) {
	t.Helper()
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	isoDir := filepath.Join(workspace, "isolinux")
	if err := os.MkdirAll(isoDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(isoDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	image, err := os.Create(filepath.Join(root, "image.iso"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { image.Close() })
	backend := file.New(image, false)
	writer, err := Create(backend, 0, 0, 2048, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Finalize(options); err != nil {
		t.Fatal(err)
	}
	reader, err := Read(backend, 0, 0, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return image, reader
}

func catalogOptions(catalog string, hide bool) FinalizeOptions {
	return FinalizeOptions{
		RockRidge: true,
		Joliet:    true,
		ElTorito: &ElTorito{
			BootCatalog:     catalog,
			HideBootCatalog: hide,
			Entries: []*ElToritoEntry{{
				Platform: BIOS, Emulation: NoEmulation, BootFile: "/isolinux/boot.img",
			}},
		},
	}
}

// childNames returns the names of the entries, without self and parent.
func childNames(entries []*directoryEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.isSelf && !entry.isParent {
			names = append(names, entry.filename)
		}
	}
	return names
}

func findChild(entries []*directoryEntry, name string) *directoryEntry {
	for _, entry := range entries {
		if entry.filename == name {
			return entry
		}
	}
	return nil
}

// bootCatalogLocation returns the catalog sector from the boot volume descriptor.
func bootCatalogLocation(t *testing.T, image *os.File) uint32 {
	t.Helper()
	for sector := int64(16); sector < 20; sector++ {
		vd := make([]byte, 2048)
		if _, err := image.ReadAt(vd, sector*2048); err != nil {
			t.Fatal(err)
		}
		if vd[0] == byte(volumeDescriptorBoot) && string(vd[1:6]) == "CD001" {
			return binary.LittleEndian.Uint32(vd[0x47:0x4b])
		}
	}
	t.Fatal("no boot volume descriptor")
	return 0
}

func readExtent(t *testing.T, image *os.File, location, size uint32) []byte {
	t.Helper()
	content := make([]byte, size)
	if _, err := image.ReadAt(content, int64(location)*2048); err != nil {
		t.Fatal(err)
	}
	return content
}

func TestFinalizeBootCatalogCollisionAndDirectoryOrder(t *testing.T) {
	for _, tt := range []struct {
		name       string
		sourceFile bool
		hide       bool
	}{
		{name: "existing catalog", sourceFile: true},
		{name: "generated catalog"},
		{name: "hidden existing catalog", sourceFile: true, hide: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			files := map[string]string{
				"a":            "no extension",
				"a.1":          "number extension",
				"boot.img":     "boot image",
				"boot.msg":     "boot message",
				"vesamenu.c32": "menu",
			}
			if tt.sourceFile {
				files["boot.cat"] = "stale catalog"
			}
			options := catalogOptions("/isolinux/boot.cat", tt.hide)
			image, reader := finalizeCatalogImage(t, files, options)

			primary, err := reader.readDirectoryPVD("/isolinux")
			if err != nil {
				t.Fatal(err)
			}
			wantPrimary := []string{"A.;1", "A.1;1", "BOOT.CAT;1", "BOOT.IMG;1", "BOOT.MSG;1", "VESAMENU.C32;1"}
			wantJoliet := []string{"a", "a.1", "boot.cat", "boot.img", "boot.msg", "vesamenu.c32"}
			if tt.hide {
				wantPrimary = slices.Delete(wantPrimary, 2, 3)
				wantJoliet = slices.Delete(wantJoliet, 2, 3)
			}
			if names := childNames(primary); !slices.Equal(names, wantPrimary) {
				t.Errorf("primary directory entries = %q, want %q", names, wantPrimary)
			}
			joliet, err := reader.readDirectoryJoliet("/isolinux")
			if err != nil {
				t.Fatal(err)
			}
			if names := childNames(joliet); !slices.Equal(names, wantJoliet) {
				t.Errorf("Joliet directory entries = %q, want %q", names, wantJoliet)
			}

			bootCatalog, err := options.ElTorito.generateCatalog()
			if err != nil {
				t.Fatal(err)
			}
			location := bootCatalogLocation(t, image)
			if !bytes.Equal(readExtent(t, image, location, uint32(len(bootCatalog))), bootCatalog) {
				t.Error("boot volume descriptor does not point to the generated catalog")
			}
			catalog := findChild(primary, "BOOT.CAT;1")
			if catalog != nil && (catalog.location != location || catalog.size != uint32(len(bootCatalog))) {
				t.Errorf("visible catalog record has location %d size %d, want location %d size %d", catalog.location, catalog.size, location, len(bootCatalog))
			}
		})
	}
}

func TestFinalizeBootCatalogIdentifierCollisionPreservesSource(t *testing.T) {
	options := catalogOptions("/isolinux/boot.catalog", false)
	image, reader := finalizeCatalogImage(t, map[string]string{
		"boot.cat": "unrelated source content",
		"boot.img": "boot image",
	}, options)

	primary, err := reader.readDirectoryPVD("/isolinux")
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := findChild(primary, "BOOT.CAT;1"), findChild(primary, "BOOT0.CAT;1")
	if source == nil || catalog == nil {
		t.Fatalf("source and generated catalog records not both present: %q", childNames(primary))
	}
	if content := readExtent(t, image, source.location, source.size); string(content) != "unrelated source content" {
		t.Errorf("source content = %q", content)
	}
	bootCatalog, err := options.ElTorito.generateCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readExtent(t, image, catalog.location, catalog.size), bootCatalog) {
		t.Error("generated catalog record does not contain the catalog")
	}
	if location := bootCatalogLocation(t, image); location != catalog.location {
		t.Errorf("boot descriptor catalog location = %d, want %d", location, catalog.location)
	}
	joliet, err := reader.readDirectoryJoliet("/isolinux")
	if err != nil {
		t.Fatal(err)
	}
	names := childNames(joliet)
	if !slices.Contains(names, "boot.cat") || !slices.Contains(names, "boot.catalog") {
		t.Errorf("Joliet names = %q, want both source and requested catalog", names)
	}
}

func TestFinalizeHiddenBootCatalogIgnoresDirectoryOfSameName(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "isolinux", "boot.cat"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "isolinux", "boot.img"), []byte("boot image"), 0o600); err != nil {
		t.Fatal(err)
	}
	image, err := os.Create(filepath.Join(root, "image.iso"))
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	backend := file.New(image, false)
	writer, err := Create(backend, 0, 0, 2048, workspace)
	if err != nil {
		t.Fatal(err)
	}
	options := catalogOptions("/isolinux/boot.cat", true)
	options.Joliet = false
	if err := writer.Finalize(options); err != nil {
		t.Fatal(err)
	}
	reader, err := Read(backend, 0, 0, 2048)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := reader.readDirectoryPVD("/isolinux")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(e *directoryEntry) bool { return e.isSubdirectory && !e.isSelf && !e.isParent }) {
		t.Errorf("source directory boot.cat missing from /isolinux: %q", childNames(entries))
	}
}

func TestFinalizeRejectsBootCatalogThatIsABootImage(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "isolinux"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "isolinux", "boot.img"), []byte("boot image"), 0o600); err != nil {
		t.Fatal(err)
	}
	image, err := os.Create(filepath.Join(root, "image.iso"))
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	writer, err := Create(file.New(image, false), 0, 0, 2048, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Finalize(catalogOptions("/isolinux/boot.img", false)); err == nil {
		t.Fatal("Finalize accepted a boot catalog path that is also a boot image")
	}
}
