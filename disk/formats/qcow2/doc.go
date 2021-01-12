// Package qcow2 provides support for creating, reading and writing qcow2 filesystems.
// It supports only qcow2 versions 2 and 3.
//
// Support is for simple reading and writing to qcow2 disks as regular reads and writes. Copy-on-Write support is not yet implemented.
//
// references:
//    https://github.com/qemu/qemu/blob/master/docs/interop/qcow2.txt
//    https://people.gnome.org/~markmc/qcow-image-format.html
//    https://juliofaracco.wordpress.com/2015/02/19/an-introduction-to-qcow2-image-format/
//    https://events.static.linuxfound.org/sites/events/files/slides/kvm-forum-2017-slides.pdf
//    https://github.com/zhangyoujia/qcow2-dump
package qcow2
