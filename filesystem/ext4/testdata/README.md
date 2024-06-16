# ext4 Test Fixtures

This directory contains test fixtures for ext4 filesystems. Specifically, it contains the following files:

* [buildimg.sh](buildimg.sh): A script to generate the `ext4.img` file and any other files needed for tests
* [README.md](README.md): This file
* [dist](dist): A directory containing the various created artifacts. These are under `.gitignore` and should not be committed to git.

Because of the size of the ext4 filesystem image `ext4.img`, it is excluded from git. Since all of the
artifacts are generated from it, there is not much point in committing those to git, so those are
ignored as well.

The artifacts need to be generated anew for each
installation on which you want to test. Of course, each generation can give slightly different
inode information, and certainly will give different timestamps, so you need to update the tests
appropriately; see below.

To generate the artifacts, including creating the `dist/` directory, run `./buildimg.sh` from within this directory.

This makes:

* an ext4 filesystem in an image file `ext4.img`, which contains:
  * the `/foo` directory with sufficient entries to require using hash tree directories
  * some small and large files in the root
* extracted blocks of the file, such as `superblock.bin` and `gdt.bin`
* the root directory in `root_directory.bin`
* information about the root directory, extracting using `debugfs` from the `ext4.img`, in `root_dir.txt`
* information about the `/foo` directory, extracting using `debugfs` from the `ext4.img`, in `foo_dir.txt`
* information about the superblock and block group table, extracted using `debugfs` from the `ext4.img`, in `stats.txt`

You **must** create artifacts before running the tests.
