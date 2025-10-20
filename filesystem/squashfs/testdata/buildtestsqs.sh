#!/bin/sh
set -e
rm -f file.sqs file_uncompressed.sqs list.txt

cat << "EOF" | docker run -i --rm -v $PWD:/data alpine:3.22
set -e
apk --update add squashfs-tools coreutils attr
mkdir -p /build
cd /build
mkdir foo zero random
dd if=/dev/zero of=zero/largefile bs=1M count=5
dd if=/dev/urandom of=random/largefile bs=1M count=5
i=0
until [ $i -gt 500 ]; do echo "filename_"${i} > foo/filename_${i}; i=$(( $i+1 )); done
echo README > README.md
mkdir -p a/b/c/d
ln -s /a/b/c/d/ef/g/h emptylink
ln -s README.md goodlink
ln README.md hardlink
echo attr > attrfile
attr -s abc -V def attrfile
attr -s myattr -V hello attrfile
chown 1.2 README.md

# compressed version
mksquashfs . /data/file.sqs

# uncompressed version
mksquashfs . /data/file_uncompressed.sqs -noI -noD -noF

# create listing to check
find . > /data/list.txt
EOF

rm -f read_test.sqs read_test.md5sums

# We use a newer alpine here because mksquashfs has a bug which is
# triggered in 3.11!

cat << "EOF" | docker run -i --rm -v $PWD:/data alpine:3.22
set -e
apk --update add squashfs-tools coreutils attr
mkdir -p /build
cd /build

touch empty
truncate -s 1 small-zero
echo -n "x" > small-random
truncate -s 10 small-zero-10
echo -n "randomdata" > small-random-10

dd if=/dev/zero of=zeros bs=1M count=1
cp -av zeros zeros-minus
truncate -s 1048575 zeros-minus
cp -av zeros zeros-plus
truncate -s 1048577 zeros-plus

dd if=/dev/urandom of=random bs=1M count=1
cp -av random random-minus
truncate -s 1048575 random-minus
cp -av random random-zero-tail
truncate -s 1048586 random-zero-tail
cp -av random random-plus
echo -n "randomdata" >> random-plus

dd if=/dev/urandom bs=1k count=16 | hd > compressible-small
dd if=/dev/urandom bs=1k count=192 | hd > compressible-large

# compressed version
mksquashfs . /data/read_test.sqs

# create md5sums check file
find . -type f -exec sh -c 'md=$(md5sum "$0"); size=$(wc -c <"$0"); echo ${md} ${size}' {} \; > /data/read_test.md5sums
EOF

rm -f dir_read.sqs

# Build a test sqs file to test corner cases in directory reading
#
# This fills up a metadata page with 300 extended file inodes in order
# to test the corner cases in the directory reading code when reading
# inodes which are assumed to be standard sized but turn out to be
# extended sized. The inodes are forced to be extended by having xattr
# set on them.
#
# It also tests corner cases in the xattr parsing code!
#
# See: TestSquashfsReadDirCornerCases
cat << "EOF" | docker run -i --rm -v $PWD:/data alpine:3.22
set -e
apk --update add squashfs-tools coreutils attr
mkdir -p /build
cd /build

for i in $(seq -w 300); do
    touch file_$i
    setfattr -n user.test -v $i file_$i
done

mksquashfs . /data/dir_read.sqs  -comp zstd -Xcompression-level 3 -b 4k -all-root
EOF
