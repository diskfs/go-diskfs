package iso9660_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
	"github.com/diskfs/go-diskfs/partition/mbr"
	"github.com/diskfs/go-diskfs/testhelper"
)

var (
	intImage = os.Getenv("TEST_IMAGE")
)

// test creating an iso with el torito boot
func TestFinalizeElTorito(t *testing.T) {
	finalizeElTorito(t, "")
	dir, err := os.MkdirTemp("", "workspace")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	finalizeElTorito(t, dir)
}

func TestFinalizeElToritoWithInaccurateTmpDir(t *testing.T) {
	finalizeElTorito(t, "")
	dir, err := os.MkdirTemp("/tmp//", "workspace")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	finalizeElTorito(t, dir)
}

//nolint:thelper // this is not a helper function
func finalizeElTorito(t *testing.T, workspace string) {
	blocksize := int64(2048)
	f, err := os.CreateTemp("", "iso_finalize_test")
	defer os.Remove(f.Name())
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}

	b := file.New(f, false)
	fs, err := iso9660.Create(b, 0, 0, blocksize, workspace)
	if err != nil {
		t.Fatalf("Failed to iso9660.Create: %v", err)
	}
	var isofile filesystem.File
	for _, filename := range []string{"/BOOT1.IMG", "/BOOT2.IMG"} {
		isofile, err = fs.OpenFile(filename, os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("Failed to iso9660.OpenFile(%s): %v", filename, err)
		}
		// create some random data
		blen := 1024 * 1024
		for i := 0; i < 5; i++ {
			b := make([]byte, blen)
			_, err = rand.Read(b)
			if err != nil {
				t.Fatalf("%d: error getting random bytes for file %s: %v", i, filename, err)
			}
			if _, err = isofile.Write(b); err != nil {
				t.Fatalf("%d: error writing random bytes to tmpfile %s: %v", i, filename, err)
			}
		}
	}

	err = fs.Finalize(iso9660.FinalizeOptions{ElTorito: &iso9660.ElTorito{
		BootCatalog:     "/BOOT.CAT",
		HideBootCatalog: false,
		Platform:        iso9660.EFI,
		Entries: []*iso9660.ElToritoEntry{
			{Platform: iso9660.BIOS, Emulation: iso9660.NoEmulation, BootFile: "/BOOT1.IMG", HideBootFile: true, LoadSegment: 0, SystemType: mbr.Fat32LBA},
			{Platform: iso9660.EFI, Emulation: iso9660.NoEmulation, BootFile: "/BOOT2.IMG", HideBootFile: false, LoadSegment: 0, SystemType: mbr.Fat32LBA},
		},
	},
	})
	if err != nil {
		t.Fatal("unexpected error fs.Finalize()", err)
	}
	if err != nil {
		t.Fatalf("error trying to Stat() iso file: %v", err)
	}

	// now check the contents
	fs, err = iso9660.Read(b, 0, 0, 2048)
	if err != nil {
		t.Fatalf("error reading the tmpfile as iso: %v", err)
	}

	// we chose to hide the first one, so check the first one exists and not the second
	_, err = fs.OpenFile("/BOOT1.IMG", os.O_RDONLY)
	if err == nil {
		t.Errorf("Did not receive expected error opening file %s: %v", "/BOOT1.IMG", err)
	}
	_, err = fs.OpenFile("/BOOT2.IMG", os.O_RDONLY)
	if err != nil {
		t.Errorf("error opening file %s: %v", "/BOOT2.IMG", err)
	}

	validateIso(t, f)

	validateElTorito(t, f)

	// close the file
	err = f.Close()
	if err != nil {
		t.Fatalf("could not close iso file: %v", err)
	}
}

// full test - create some files, finalize, check the output
//
//nolint:gocyclo // we really do not care about the cyclomatic complexity of a test function. Maybe someday we will improve it.
func TestFinalize9660(t *testing.T) {
	blocksize := int64(2048)
	t.Run("deep dir", func(t *testing.T) {
		f, err := os.CreateTemp("", "iso_finalize_test")
		defer os.Remove(f.Name())
		if err != nil {
			t.Fatalf("Failed to create tmpfile: %v", err)
		}

		b := file.New(f, false)
		fs, err := iso9660.Create(b, 0, 0, blocksize, "")
		if err != nil {
			t.Fatalf("Failed to iso9660.Create: %v", err)
		}
		for _, dir := range []string{"/A/B/C/D/E/F/G/H/I/J/K"} {
			err = fs.Mkdir(dir)
			if err != nil {
				t.Fatalf("Failed to iso9660.Mkdir(%s): %v", dir, err)
			}
		}

		err = fs.Finalize(iso9660.FinalizeOptions{})
		if err == nil {
			t.Fatal("unexpected lack of error fs.Finalize()", err)
		}
	})
	t.Run("valid", func(t *testing.T) {
		f, err := os.CreateTemp("", "iso_finalize_test")
		defer os.Remove(f.Name())
		if err != nil {
			t.Fatalf("Failed to create tmpfile: %v", err)
		}

		b := file.New(f, false)
		fs, err := iso9660.Create(b, 0, 0, blocksize, "")
		if err != nil {
			t.Fatalf("Failed to iso9660.Create: %v", err)
		}
		for _, dir := range []string{"/", "/FOO", "/BAR", "/ABC"} {
			err = fs.Mkdir(dir)
			if err != nil {
				t.Fatalf("Failed to iso9660.Mkdir(%s): %v", dir, err)
			}
		}
		var isofile filesystem.File
		for _, filename := range []string{"/BAR/LARGEFILE", "/ABC/LARGEFILE"} {
			isofile, err = fs.OpenFile(filename, os.O_CREATE|os.O_RDWR)
			if err != nil {
				t.Fatalf("Failed to iso9660.OpenFile(%s): %v", filename, err)
			}
			// create some random data
			blen := 1024 * 1024
			for i := 0; i < 5; i++ {
				b := make([]byte, blen)
				_, err = rand.Read(b)
				if err != nil {
					t.Fatalf("%d: error getting random bytes for file %s: %v", i, filename, err)
				}
				if _, err = isofile.Write(b); err != nil {
					t.Fatalf("%d: error writing random bytes to tmpfile %s: %v", i, filename, err)
				}
			}
		}

		isofile, err = fs.OpenFile("README.MD", os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("Failed to iso9660.OpenFile(%s): %v", "README.MD", err)
		}
		dataBytes := []byte("readme\n")
		if _, err = isofile.Write(dataBytes); err != nil {
			t.Fatalf("error writing %s to tmpfile %s: %v", string(dataBytes), "README.MD", err)
		}

		fooCount := 75
		for i := 0; i <= fooCount; i++ {
			filename := fmt.Sprintf("/FOO/FILENAME_%d", i)
			contents := []byte(fmt.Sprintf("filename_%d\n", i))
			isofile, err = fs.OpenFile(filename, os.O_CREATE|os.O_RDWR)
			if err != nil {
				t.Fatalf("Failed to iso9660.OpenFile(%s): %v", filename, err)
			}
			if _, err = isofile.Write(contents); err != nil {
				t.Fatalf("%d: error writing bytes to tmpfile %s: %v", i, filename, err)
			}
		}

		err = fs.Finalize(iso9660.FinalizeOptions{})
		if err != nil {
			t.Fatal("unexpected error fs.Finalize()", err)
		}
		// now need to check contents
		fi, err := f.Stat()
		if err != nil {
			t.Fatalf("error trying to Stat() iso file: %v", err)
		}
		// we made two 5MB files, so should be at least 10MB
		if fi.Size() < 10*1024*1024 {
			t.Fatalf("resultant file too small after finalizing %d", fi.Size())
		}

		// now check the contents
		fs, err = iso9660.Read(b, 0, 0, 2048)
		if err != nil {
			t.Fatalf("error reading the tmpfile as iso: %v", err)
		}

		dirFi, err := fs.ReadDir("/")
		if err != nil {
			t.Errorf("error reading the root directory from iso: %v", err)
		}
		// we expect to have 3 entries: ABC BAR and FOO
		expected := map[string]bool{
			"ABC": false, "BAR": false, "FOO": false, "README.MD": false,
		}
		for _, e := range dirFi {
			delete(expected, e.Name())
		}
		if len(expected) > 0 {
			keys := make([]string, 0)
			for k := range expected {
				keys = append(keys, k)
			}
			t.Errorf("Some entries not found in root: %v", keys)
		}

		// get a few files I expect
		fileContents := map[string]string{
			"/README.MD":       "readme\n",
			"/FOO/FILENAME_50": "filename_50\n",
			"/FOO/FILENAME_2":  "filename_2\n",
		}

		for k, v := range fileContents {
			var (
				f    filesystem.File
				read int
			)

			f, err = fs.OpenFile(k, os.O_RDONLY)
			if err != nil {
				t.Errorf("error opening file %s: %v", k, err)
				continue
			}
			// check the contents
			b := make([]byte, 50)
			read, err = f.Read(b)
			if err != nil && err != io.EOF {
				t.Errorf("error reading from file %s: %v", k, err)
			}
			actual := string(b[:read])
			if actual != v {
				t.Errorf("Mismatched content, actual '%s' expected '%s'", actual, v)
			}
		}

		validateIso(t, f)

		// close the file
		err = f.Close()
		if err != nil {
			t.Fatalf("could not close iso file: %v", err)
		}
	})

	t.Run("existing workspace", func(t *testing.T) {
		// create a directory to bundle into an iso
		dir, err := os.MkdirTemp("", "iso_finalize_test")
		defer os.RemoveAll(dir)
		if err != nil {
			t.Fatalf("Failed to create tmpdir: %v", err)
		}
		err = os.MkdirAll(filepath.Join(dir, "a", "b", "c"), 0o775)
		if err != nil {
			t.Fatalf("Failed to create test dirs: %v", err)
		}
		err = os.WriteFile(filepath.Join(dir, "file"), []byte("somecontent"), 0o600)
		if err != nil {
			t.Fatalf("Failed to write test file: %v", err)
		}
		err = os.WriteFile(filepath.Join(dir, "a", "b", "c", "foo"), []byte("someothercontent"), 0o600)
		if err != nil {
			t.Fatalf("Failed to write test file: %v", err)
		}

		// create the iso directly from the existing directory
		f, err := os.CreateTemp("", "iso_finalize_test")
		defer os.Remove(f.Name())
		if err != nil {
			t.Fatalf("Failed to create tmpfile: %v", err)
		}

		b := file.New(f, false)
		fs, err := iso9660.Create(b, 0, 0, blocksize, dir)
		if err != nil {
			t.Fatalf("Failed to iso9660.Create: %v", err)
		}
		err = fs.Finalize(iso9660.FinalizeOptions{})
		if err != nil {
			t.Fatal("unexpected error fs.Finalize()", err)
		}

		// now check the contents
		fs, err = iso9660.Read(b, 0, 0, 2048)
		if err != nil {
			t.Fatalf("error reading the tmpfile as iso: %v", err)
		}
		isoFile, err := fs.OpenFile("/FILE", os.O_RDONLY)
		if err != nil {
			t.Fatalf("Failed to open top-level file from iso: %v", err)
		}
		content, err := io.ReadAll(isoFile)
		if err != nil {
			t.Fatalf("Failed to read top-level file from iso: %v", err)
		}
		actual := string(content)
		if actual != "somecontent" {
			t.Fatalf("Got unexpected content from '/file', got '%s', expected 'somecontent'", actual)
		}

		isoFile, err = fs.OpenFile("/A/B/C/FOO", os.O_RDONLY)
		if err != nil {
			t.Fatalf("Failed to open file from iso: %v", err)
		}
		content, err = io.ReadAll(isoFile)
		if err != nil {
			t.Fatalf("Failed to read file from iso: %v", err)
		}
		actual = string(content)
		if actual != "someothercontent" {
			t.Fatalf("Got unexpected content from '/a/b/c/foo', got '%s', expected 'someothercontent'", actual)
		}

		validateIso(t, f)

		// close the file
		err = f.Close()
		if err != nil {
			t.Fatalf("could not close iso file: %v", err)
		}
	})
}

//nolint:gocyclo // we really do not care about the cyclomatic complexity of a test function. Maybe someday we will improve it.
func TestFinalizeRockRidge(t *testing.T) {
	blocksize := int64(2048)
	t.Run("valid", func(t *testing.T) {
		f, err := os.CreateTemp("", "iso_finalize_test")
		defer os.Remove(f.Name())
		if err != nil {
			t.Fatalf("Failed to create tmpfile: %v", err)
		}

		b := file.New(f, false)
		fs, err := iso9660.Create(b, 0, 0, blocksize, "")
		if err != nil {
			t.Fatalf("Failed to iso9660.Create: %v", err)
		}
		for _, dir := range []string{"/", "/foo", "/bar", "/abc"} {
			err = fs.Mkdir(dir)
			if err != nil {
				t.Fatalf("Failed to iso9660.Mkdir(%s): %v", dir, err)
			}
		}
		// make a deep directory
		dir := "/deep/a/b/c/d/e/f/g/h/i/j"
		err = fs.Mkdir(dir)
		if err != nil {
			t.Fatalf("Failed to iso9660.Mkdir(%s): %v", dir, err)
		}
		var isofile filesystem.File
		for _, filename := range []string{"/bar/largefile", "/abc/largefile"} {
			isofile, err = fs.OpenFile(filename, os.O_CREATE|os.O_RDWR)
			if err != nil {
				t.Fatalf("Failed to iso9660.OpenFile(%s): %v", filename, err)
			}
			// create some random data
			blen := 1024 * 1024
			for i := 0; i < 5; i++ {
				b := make([]byte, blen)
				_, err = rand.Read(b)
				if err != nil {
					t.Fatalf("%d: error getting random bytes for file %s: %v", i, filename, err)
				}
				if _, err = isofile.Write(b); err != nil {
					t.Fatalf("%d: error writing random bytes to tmpfile %s: %v", i, filename, err)
				}
			}
		}

		isofile, err = fs.OpenFile("README.md", os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("Failed to iso9660.OpenFile(%s): %v", "README.md", err)
		}
		dataBytes := []byte("readme\n")
		if _, err = isofile.Write(dataBytes); err != nil {
			t.Fatalf("error writing %s to tmpfile %s: %v", string(dataBytes), "README.md", err)
		}

		fooCount := 75
		for i := 0; i <= fooCount; i++ {
			filename := fmt.Sprintf("/foo/filename_%d", i)
			contents := []byte(fmt.Sprintf("filename_%d\n", i))
			isofile, err = fs.OpenFile(filename, os.O_CREATE|os.O_RDWR)
			if err != nil {
				t.Fatalf("Failed to iso9660.OpenFile(%s): %v", filename, err)
			}
			if _, err = isofile.Write(contents); err != nil {
				t.Fatalf("%d: error writing bytes to tmpfile %s: %v", i, filename, err)
			}
		}

		workspace := fs.Workspace()

		err = fs.Finalize(iso9660.FinalizeOptions{RockRidge: true})
		if err != nil {
			t.Fatal("unexpected error fs.Finalize({RockRidge: true})", err)
		}

		if _, err := os.Stat(workspace); !os.IsNotExist(err) {
			t.Fatalf("Workspace dir %s should be removed after finalizing", workspace)
		}

		// now need to check contents
		fi, err := f.Stat()
		if err != nil {
			t.Fatalf("error trying to Stat() iso file: %v", err)
		}
		// we made two 5MB files, so should be at least 10MB
		if fi.Size() < 10*1024*1024 {
			t.Fatalf("resultant file too small after finalizing %d", fi.Size())
		}

		// now check the contents
		fs, err = iso9660.Read(b, 0, 0, 2048)
		if err != nil {
			t.Fatalf("error reading the tmpfile as iso: %v", err)
		}

		dirFi, err := fs.ReadDir("/")
		if err != nil {
			t.Errorf("error reading the root directory from iso: %v", err)
		}
		// we expect to have 3 entries: ABC BAR and FOO
		expected := map[string]bool{
			"abc": false, "bar": false, "foo": false, "README.md": false,
		}
		for _, e := range dirFi {
			delete(expected, e.Name())
		}
		if len(expected) > 0 {
			keys := make([]string, 0)
			for k := range expected {
				keys = append(keys, k)
			}
			t.Errorf("Some entries not found in root: %v", keys)
		}

		// get a few files I expect
		fileContents := map[string]string{
			"/README.md":       "readme\n",
			"/foo/filename_50": "filename_50\n",
			"/foo/filename_2":  "filename_2\n",
		}

		for k, v := range fileContents {
			var (
				f    filesystem.File
				read int
			)

			f, err = fs.OpenFile(k, os.O_RDONLY)
			if err != nil {
				t.Errorf("error opening file %s: %v", k, err)
				continue
			}
			// check the contents
			b := make([]byte, 50)
			read, err = f.Read(b)
			if err != nil && err != io.EOF {
				t.Errorf("error reading from file %s: %v", k, err)
			}
			actual := string(b[:read])
			if actual != v {
				t.Errorf("Mismatched content, actual '%s' expected '%s'", actual, v)
			}
		}

		validateIso(t, f)

		// close the file
		err = f.Close()
		if err != nil {
			t.Fatalf("could not close iso file: %v", err)
		}
	})
}

//nolint:thelper // this is not a helper function
func validateIso(t *testing.T, f *os.File) {
	// only do this test if os.Getenv("TEST_IMAGE") contains a real image for integration testing
	if intImage == "" {
		return
	}
	output := new(bytes.Buffer)
	/* to check file contents
	7z l -ba file.iso
	*/
	mpath := "/file.iso"
	mounts := map[string]string{
		f.Name(): mpath,
	}
	err := testhelper.DockerRun(nil, output, false, true, mounts, intImage, "7z", "l", "-ba", mpath)
	outString := output.String()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		t.Log(outString)
	}
}

//nolint:thelper // this is not a helper function
func validateElTorito(t *testing.T, f *os.File) {
	// only do this test if os.Getenv("TEST_IMAGE") contains a real image for integration testing
	if intImage == "" {
		return
	}
	output := new(bytes.Buffer)
	mpath := "/file.iso"
	mounts := map[string]string{
		f.Name(): mpath,
	}
	err := testhelper.DockerRun(nil, output, false, true, mounts, intImage, "isoinfo", "-d", "-i", mpath)
	outString := output.String()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		t.Log(outString)
	}
	// look for El Torito line
	re := regexp.MustCompile(`El Torito VD version 1 found, boot catalog is in sector (\d+)\n`)
	matches := re.FindStringSubmatch(outString)
	if matches == nil || len(matches) < 1 {
		t.Fatalf("unable to match El Torito information")
	}
	// what sector should it be in?
}
