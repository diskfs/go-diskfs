# FAT32 Test Fixtures
This directory contains test fixtures for FAT32 filesystems. Specifically, it contains the following files:

* `calcsfn_checksum.c`: A C program that generates correct short filename checksums.
* `fat32.go`: A Go program that creates a basic filesystem; see below.

All of the actual test artifacts are in `dist/`, which is excluded from version control
via `.gitignore` in this directory.

To generate the artifacts, run `mkfat32.sh`. This will generate a `fat32.img` file in the `dist/`
directory, as well as all sorts of information files about the filesystem and its contents,
generated using standard tooling.

The go tests for fat32 automatically generate those if `dist/fat32.img` is not there.
if it is and you need to regenerate it, you can run `mkfat32.sh` and then run the tests,
or just remove the `dist/` directory and it will regenerate everything the next time you run the
tests.

## Basic Builds
In addition to the usual test harnesses, this directory contains a file that generates a basic fat32 filesystem. To build it:

```
go build -o fat32 fat32.go
```

and run it as `./fat32`. The output will be a file called `test_file.img`. 

To build an `mtools` based image to compare, run `./mkfat32.sh`. 

