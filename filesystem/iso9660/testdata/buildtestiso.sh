#!/bin/sh

cat << "EOF" | docker run -i --rm -v $PWD:/data alpine:3.8
apk --update add xorriso
mkdir -p /build
cd /build
mkdir foo bar abc
dd if=/dev/zero of=bar/largefile bs=1M count=5
dd if=/dev/zero of=abc/largefile bs=1M count=5
i=0
until [ $i -gt 75 ]; do echo "filename_"${i} > foo/filename_${i}; i=$(( $i+1 )); done
xorriso -as mkisofs -o /data/file.iso .
EOF
