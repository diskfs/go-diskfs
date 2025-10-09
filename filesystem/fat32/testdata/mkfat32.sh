#!/bin/sh
set -e
set -x

INSTALLDIR="$1"

if [ -z "$INSTALLDIR" ]; then
  INSTALLDIR="$PWD"
fi

docker build -t go-diskfs-testdata-fat32 .

mkdir -p $INSTALLDIR/dist
cat <<"EOF" | docker run -i --rm -v $INSTALLDIR/dist:/data go-diskfs-testdata-fat32
set -e
set -x

for n in $(echo "12 16 32"); do
  out="/data/fat${n}"
  mkdir ${out}

  dd if=/dev/zero of=${out}/disk.img bs=1M count=$(( n + 2 ))
  mkfs.vfat -v -F ${n} ${out}/disk.img
  echo "mtools_skip_check=1" >> /etc/mtools.conf
  mmd -i ${out}/disk.img ::/foo
  mmd -i ${out}/disk.img ::/foo/bar
  echo 'Tenemos un archivo corto' > /CORTO1.TXT
  dd if=/dev/zero 'of=/Un archivo con nombre largo.dat' bs=1024 count=2
  dd if=/dev/zero 'of=/tercer_archivo' bs=1024 count=6
  dd if=/dev/zero 'of=/Un archivo con nombre largo.dat' bs=1024 count=7
  dd if=/dev/zero 'of=/some_long_embedded_nameא' bs=1024 count=7
  mcopy -i ${out}/disk.img /CORTO1.TXT ::/
  mcopy -i ${out}/disk.img '/Un archivo con nombre largo.dat' ::/
  mcopy -i ${out}/disk.img '/tercer_archivo' ::/
  mcopy -i ${out}/disk.img '/some_long_embedded_nameא' ::/foo/bar
  mmd -i ${out}/disk.img ::/lower83
  mcopy -i ${out}/disk.img /CORTO1.TXT ::/lower83/lower.low
  mcopy -i ${out}/disk.img /CORTO1.TXT ::/lower83/lower.UPP
  mcopy -i ${out}/disk.img /CORTO1.TXT ::/lower83/UPPER.low

  i=0
  until [ $i -gt 75 ]; do mmd -i ${out}/disk.img ::/foo/dir${i}; i=$(( $i+1 )); done

  fatlabel ${out}/disk.img go-diskfs

  # now get the information we need to build the testdata

  # root dir info
  mdir -a -i ${out}/disk.img ::/ > ${out}/root_dir.txt
  fls -f fat${n} -p ${out}/disk.img > ${out}/root_dir_fls.txt
  i=0
  cat ${out}/root_dir_fls.txt | while read line; do
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
      istat -f fat${n} ${out}/disk.img $inode > ${out}/root_dir_istat_${i}.txt
      i=$(( $i+1 ))
  done

  # foo dir info
  # usual mdir info is helpful
  mdir -a -i ${out}/disk.img ::/foo/ > ${out}/foo_dir.txt
  # to use fls to list the references, first, find the "inode" of /foo/
  foo_inode=$(awk '$3 == "foo" {print $2}' ${out}/root_dir_fls.txt | tr -d ':')
  fls -f fat${n} -p ${out}/disk.img $foo_inode > ${out}/foo_dir_fls.txt
  i=0
  cat ${out}/foo_dir_fls.txt | while read line; do
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
      istat -f fat${n} ${out}/disk.img $inode > ${out}/foo_dir_istat_${i}.txt
      i=$(( $i+1 ))
  done

  dosfsck -v -l ${out}/disk.img > ${out}/fsck.txt

  fsstat -f fat${n} ${out}/disk.img > ${out}/fsstat.txt

  # get the serial number
  dd if=${out}/disk.img bs=1 skip=67 count=4 2>/dev/null| hexdump -e '4/1 "%02x"' > ${out}/serial.txt
done

EOF
