# squashfs Test Fixtures
This directory contains test fixtures for squashfs filesystems. Specifically, it contains the following files:

* `file.sqs`: A 10MB squashfs image

To generate the `file.sqs` :


```
./buildtestsqs.sh
```

We make the `/foo` directory with sufficient entries to exceed a single sector (>8192 bytes). This allows us to test reading directories past a sector boundary). Since each directory entry is at least 8 bytes + filesize name, we create 10 byte filenames, for a directory entry of 18 bytes. With a metadata sector size of 8192 bytes, we need 8192/18 = 455 entries to fill the cluster and one more to get to the next one, so we make 500 entries.
