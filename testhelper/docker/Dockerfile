FROM alpine:3.11

# just install the tools we need
RUN apk --update add dosfstools mtools sgdisk sfdisk gptfdisk p7zip cdrkit squashfs-tools

RUN echo "mtools_skip_check=1" >> /etc/mtools.conf
