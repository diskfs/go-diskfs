package squashfs_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
	"github.com/diskfs/go-diskfs/testhelper"
)

var (
	intImage = os.Getenv("TEST_IMAGE")
)

// full test - create some files, finalize, check the output
func TestFinalizeSquashfs(t *testing.T) {
	blocksize := int64(4096)
	t.Run("valid", func(t *testing.T) {
		f, err := os.CreateTemp("", "squashfs_finalize_test")
		fileName := f.Name()
		defer os.Remove(fileName)
		if err != nil {
			t.Fatalf("Failed to create tmpfile: %v", err)
		}
		fs, err := squashfs.Create(f, 0, 0, blocksize)
		if err != nil {
			t.Fatalf("Failed to squashfs.Create: %v", err)
		}
		for _, dir := range []string{"/", "/FOO", "/BAR", "/ABC"} {
			err = fs.Mkdir(dir)
			if err != nil {
				t.Fatalf("Failed to squashfs.Mkdir(%s): %v", dir, err)
			}
		}
		var sqsfile filesystem.File
		for _, filename := range []string{"/BAR/LARGEFILE", "/ABC/LARGEFILE"} {
			sqsfile, err = fs.OpenFile(filename, os.O_CREATE|os.O_RDWR)
			if err != nil {
				t.Fatalf("Failed to squashfs.OpenFile(%s): %v", filename, err)
			}
			// create some random data
			blen := 1024 * 1024
			for i := 0; i < 5; i++ {
				b := make([]byte, blen)
				_, err = rand.Read(b)
				if err != nil {
					t.Fatalf("%d: error getting random bytes for file %s: %v", i, filename, err)
				}
				if _, err = sqsfile.Write(b); err != nil {
					t.Fatalf("%d: error writing random bytes to tmpfile %s: %v", i, filename, err)
				}
			}
		}

		sqsfile, err = fs.OpenFile("README.MD", os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("Failed to squashfs.OpenFile(%s): %v", "README.MD", err)
		}
		b := []byte("readme\n")
		if _, err = sqsfile.Write(b); err != nil {
			t.Fatalf("error writing %s to tmpfile %s: %v", string(b), "README.MD", err)
		}

		fooCount := 75
		for i := 0; i <= fooCount; i++ {
			filename := fmt.Sprintf("/FOO/FILENAME_%d", i)
			contents := []byte(fmt.Sprintf("filename_%d\n", i))
			sqsfile, err = fs.OpenFile(filename, os.O_CREATE|os.O_RDWR)
			if err != nil {
				t.Fatalf("Failed to squashfs.OpenFile(%s): %v", filename, err)
			}
			if _, err = sqsfile.Write(contents); err != nil {
				t.Fatalf("%d: error writing bytes to tmpfile %s: %v", i, filename, err)
			}
		}

		err = fs.Finalize(squashfs.FinalizeOptions{})
		if err != nil {
			t.Fatal("unexpected error fs.Finalize()", err)
		}
		// now need to check contents
		fi, err := f.Stat()
		if err != nil {
			t.Fatalf("error trying to Stat() squashfs file: %v", err)
		}
		// we made two 5MB files, so should be at least 10MB
		if fi.Size() < 10*1024*1024 {
			t.Fatalf("resultant file too small after finalizing %d", fi.Size())
		}

		// now check the contents
		fs, err = squashfs.Read(f, 0, 0, blocksize)
		if err != nil {
			t.Fatalf("error reading the tmpfile as squashfs: %v", err)
		}

		dirFi, err := fs.ReadDir("/")
		if err != nil {
			t.Errorf("error reading the root directory from squashfs: %v", err)
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

		validateSquashfs(t, f)

		// close the file
		err = f.Close()
		if err != nil {
			t.Fatalf("could not close squashfs file: %v", err)
		}
	})
}

//nolint:thelper // this is not a helper function
func validateSquashfs(t *testing.T, f *os.File) {
	// only do this test if os.Getenv("TEST_IMAGE") contains a real image for integration testing
	if intImage == "" {
		return
	}
	output := new(bytes.Buffer)
	/* to check file contents
	unsquashfs -ll /file.sqs
	unsquashfs -s /file.sqs
	*/
	mpath := "/file.sqs"
	mounts := map[string]string{
		f.Name(): mpath,
	}
	err := testhelper.DockerRun(nil, output, false, true, mounts, intImage, "unsquashfs", "-ll", mpath)
	outString := output.String()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		t.Log(outString)
	}
	err = testhelper.DockerRun(nil, output, false, true, mounts, intImage, "unsquashfs", "-s", mpath)
	outString = output.String()
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		t.Log(outString)
	}
}
