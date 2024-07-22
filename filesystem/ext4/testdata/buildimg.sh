#!/bin/sh
set -e
mkdir -p dist
cat << "EOF" | docker run -i --rm -v $PWD/dist:/data -w /data --privileged alpine:3.20
set -e
set -x
apk --update add e2fsprogs e2fsprogs-extra
dd if=/dev/zero of=ext4.img bs=1M count=100
mkfs.ext4 ext4.img
mount ext4.img /mnt
cd /mnt
mkdir foo
mkdir foo/bar
echo "This is a short file" > shortfile.txt
dd if=/dev/zero of=two-k-file.dat bs=1024 count=2
dd if=/dev/zero of=six-k-file.dat bs=1024 count=6
dd if=/dev/zero of=seven-k-file.dat bs=1024 count=7
dd if=/dev/zero of=ten-meg-file.dat bs=1M count=10
set +x
i=0; until [ $i -gt 10000 ]; do mkdir foo/dir${i}; i=$(( $i+1 )); done
set -x
# create a file with known content
dd if=/dev/random of=/data/random.dat bs=1024 count=20
cp /data/random.dat random.dat
# symlink to a file and to a dead-end
ln -s random.dat symlink.dat
ln -s /random.dat absolutesymlink
ln -s nonexistent deadlink
ln -s /some/really/long/path/that/does/not/exist/and/does/not/fit/in/symlink deadlonglink # the target here is >60 chars and so will not fit within the inode
# hardlink
ln random.dat hardlink.dat
cd /data
umount /mnt

# now get the information we need to build the testdata
debugfs -R 'ls -l /' ext4.img > root_dir.txt
debugfs -R 'ls -l /foo' ext4.img > foo_dir.txt
debugfs -R "cat /" ext4.img > root_directory.bin
dumpe2fs ext4.img > stats.txt
dd if=ext4.img of=gdt.bin bs=1024 count=1 skip=2
dd if=ext4.img of=superblock.bin bs=1024 count=1 skip=1
dd if=superblock.bin bs=1 skip=208 count=16 2>/dev/null | hexdump -e '16/1 "%02x" "\n"' > journaluuid.txt
dd if=superblock.bin   bs=1 skip=$((0x10c)) count=$((15 * 4)) | hexdump -e '15/4 "0x%08x, " "\n"' > journalinodex.txt
dd if=superblock.bin count=2 skip=376 bs=1 2>/dev/null| hexdump -e '1/2 "%u"' > lifetime_kb.txt
EOF
