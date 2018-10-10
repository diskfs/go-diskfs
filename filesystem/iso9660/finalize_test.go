package iso9660_test

import (
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/deitch/diskfs/filesystem"
	"github.com/deitch/diskfs/filesystem/iso9660"
)

// full test - create some files, finalize, check the output
func TestFinalize(t *testing.T) {
	blocksize := int64(2048)
	f, err := ioutil.TempFile("", "iso_finalize_test")
	defer os.Remove(f.Name())
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	fs, err := iso9660.Create(f, 0, 0, blocksize)
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
	for _, filename := range []string{"/BAR/LARGEFILE", "/ABC/LARGEFILE", "/README.MD"} {
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

	err = fs.Finalize()
	if err != nil {
		t.Fatal("Unexpected error fs.Finalize()", err)
	}
	// now need to check contents
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Error trying to Stat() iso file: %v", err)
	}
	// we made two 5MB files, so should be at least 10MB
	if fi.Size() < 10*1024*1024 {
		t.Fatalf("Resultant file too small after finalizing %d", fi.Size())
	}

	// now check the contents
	fs, err = iso9660.Read(f, 0, 0, 2048)
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
			t.Errorf("Error opening file %s: %v", k, err)
			continue
		}
		// check the contents
		b := make([]byte, 50, 50)
		read, err = f.Read(b)
		if err != nil && err != io.EOF {
			t.Errorf("Error reading from file %s: %v", k, err)
		}
		actual := string(b[:read])
		if actual != v {
			t.Errorf("Mismatched content, actual '%s' expected '%s'", actual, v)
		}
	}

	// close the file
	err = f.Close()
	if err != nil {
		t.Fatalf("Could not close iso file: %v", err)
	}

}
