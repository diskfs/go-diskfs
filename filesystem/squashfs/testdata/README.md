# squashfs Test Fixtures

This directory contains test fixtures for squashfs filesystems. Specifically, it contains the following files:

* `file.sqs`: A 10MB squashfs image
* `read_test.sqs`: A 3MB squashfs image for testing different reads sizes

To generate the `.sqs` files:


```
./buildtestsqs.sh
```

## `file.sqs`

We make the `/foo` directory with sufficient entries to exceed a single sector (>8192 bytes). This allows us to test reading directories past a sector boundary). Since each directory entry is at least 8 bytes + filesize name, we create 10 byte filenames, for a directory entry of 18 bytes. With a metadata sector size of 8192 bytes, we need 8192/18 = 455 entries to fill the cluster and one more to get to the next one, so we make 500 entries.

## `read_test.sqs`

This contains files of sizes around binary boundaries designed to exercise the corner cases in the block reading code.
