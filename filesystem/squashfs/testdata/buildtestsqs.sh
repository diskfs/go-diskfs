#!/bin/sh
set -e
cat << "EOF" | docker run -i --rm -v $PWD:/data alpine:3.11
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
EOF
