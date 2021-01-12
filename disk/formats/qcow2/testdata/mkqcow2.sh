#!/bin/sh

set -e
set +x

cat <<"EOF" | docker run -i --rm -v $(pwd):/data alpine:3.13
set -e
set +x
apk --update add dosfstools mtools qemu-img
IMGFILE=/tmp/file
dd if=/dev/zero of=$IMGFILE bs=1M count=10
mkfs.vfat -v -F 32 $IMGFILE
echo "mtools_skip_check=1" >> /etc/mtools.conf
mmd -i $IMGFILE ::/foo
mmd -i $IMGFILE ::/foo/bar
echo 'some random contents' > /FILE1.TXT
dd if=/dev/zero 'of=/A file with a large name.dat' bs=1024 count=2
dd if=/dev/zero 'of=/random_archive' bs=1024 count=6
dd if=/dev/zero 'of=/A file with a large number.dat' bs=1024 count=7
dd if=/dev/zero 'of=/some_long_embedded_nameא' bs=1024 count=7
mcopy -i $IMGFILE /FILE1.TXT ::/
mcopy -i $IMGFILE '/A file with a large name.dat' ::/
mcopy -i $IMGFILE '/random_archive' ::/
mcopy -i $IMGFILE '/A file with a large number.dat' ::/
mcopy -i $IMGFILE '/some_long_embedded_nameא' ::/foo/bar

i=0
until [ $i -gt 75 ]; do mmd -i $IMGFILE ::/foo/dir${i}; i=$(( $i+1 )); done

qemu-img convert -q -f raw -O qcow2 $IMGFILE /data/file.qcow2
EOF
