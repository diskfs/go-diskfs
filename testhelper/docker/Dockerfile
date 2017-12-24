FROM alpine:3.6

# just install the tools we need
RUN apk --update add dosfstools mtools sgdisk sfdisk gptfdisk

COPY entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
