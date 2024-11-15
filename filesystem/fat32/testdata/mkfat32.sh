#!/bin/sh
set -e
set -x

INSTALLDIR="$1"

if [ -z "$INSTALLDIR" ]; then
  INSTALLDIR="$PWD"
fi

mkdir -p $INSTALLDIR/dist
cat <<"EOF" | docker run -i --rm -v $INSTALLDIR/dist:/data alpine:3.20
set -e
set -x

apk --update add dosfstools mtools sleuthkit
dd if=/dev/zero of=/data/fat32.img bs=1M count=10
mkfs.vfat -v -F 32 /data/fat32.img
echo "mtools_skip_check=1" >> /etc/mtools.conf
mmd -i /data/fat32.img ::/foo
mmd -i /data/fat32.img ::/foo/bar
echo 'Tenemos un archivo corto' > /CORTO1.TXT
dd if=/dev/zero 'of=/Un archivo con nombre largo.dat' bs=1024 count=2
dd if=/dev/zero 'of=/tercer_archivo' bs=1024 count=6
dd if=/dev/zero 'of=/Un archivo con nombre largo.dat' bs=1024 count=7
dd if=/dev/zero 'of=/some_long_embedded_nameא' bs=1024 count=7
mcopy -i /data/fat32.img /CORTO1.TXT ::/
mcopy -i /data/fat32.img '/Un archivo con nombre largo.dat' ::/
mcopy -i /data/fat32.img '/tercer_archivo' ::/
mcopy -i /data/fat32.img '/some_long_embedded_nameא' ::/foo/bar
mmd -i /data/fat32.img ::/lower83
mcopy -i /data/fat32.img /CORTO1.TXT ::/lower83/lower.low
mcopy -i /data/fat32.img /CORTO1.TXT ::/lower83/lower.UPP
mcopy -i /data/fat32.img /CORTO1.TXT ::/lower83/UPPER.low

i=0
until [ $i -gt 75 ]; do mmd -i /data/fat32.img ::/foo/dir${i}; i=$(( $i+1 )); done

fatlabel /data/fat32.img go-diskfs

# now get the information we need to build the testdata

# root dir info
mdir -a -i /data/fat32.img ::/ > /data/root_dir.txt
fls -f fat32 -p /data/fat32.img > /data/root_dir_fls.txt
i=0
cat /data/root_dir_fls.txt | while read line; do
    if [ -z "$line" ]; then
      continue
    fi
    type=$(echo $line | awk '{print $1}')
    if [ "$type" != "r/r" -a "$type" != "d/d" ]; then
      continue
    fi
    inode=$(echo $line | awk '{print $2}')
    # remove a trailing :
    inode=${inode%:}
    # save the istat info per the order in the directory, so we can find it later
    istat -f fat32 /data/fat32.img $inode > /data/root_dir_istat_${i}.txt
    i=$(( $i+1 ))
done

# foo dir info
# usual mdir info is helpful
mdir -a -i /data/fat32.img ::/foo/ > /data/foo_dir.txt
# to use fls to list the references, first, find the "inode" of /foo/
foo_inode=$(awk '$3 == "foo" {print $2}' /data/root_dir_fls.txt | tr -d ':')
fls -f fat32 -p /data/fat32.img $foo_inode > /data/foo_dir_fls.txt
i=0
cat /data/foo_dir_fls.txt | while read line; do
    if [ -z "$line" ]; then
      continue
    fi
    type=$(echo $line | awk '{print $1}')
    if [ "$type" != "r/r" -a "$type" != "d/d" ]; then
      continue
    fi
    inode=$(echo $line | awk '{print $2}')
    # remove a trailing :
    inode=${inode%:}
    # save the istat info per the order in the directory, so we can find it later
    istat -f fat32 /data/fat32.img $inode > /data/foo_dir_istat_${i}.txt
    i=$(( $i+1 ))
done

dosfsck -v -l /data/fat32.img > /data/fsck.txt

fsstat -f fat32 /data/fat32.img > /data/fsstat.txt

# get the serial number
dd if=/data/fat32.img bs=1 skip=67 count=4 2>/dev/null| hexdump -e '4/1 "%02x"' > /data/serial.txt

EOF
