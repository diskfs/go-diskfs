package squashfs

func MakeTestFile(size uint64) *File {
	return &File{
		extendedFile: &extendedFile{
			fileSize: size,
		},
	}
}
