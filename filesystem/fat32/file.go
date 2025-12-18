package fat32

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/filesystem"
)

// ErrFileTooLarge is returned when attempting to write beyond FAT32's 4GB file size limit
var ErrFileTooLarge = errors.New("file size exceeds FAT32 limit of 4GB")

const maxFAT32FileSize = (1 << 32) - 1 // 4,294,967,295 bytes (4GB - 1)

// File represents a single file in a FAT32 filesystem
type File struct {
	*directoryEntry
	isReadWrite    bool
	isAppend       bool
	offset         int64
	parent         *Directory
	filesystem     *FileSystem
	needsFlush     bool                 // tracks if directory entries need to be written on Close()
	cachedClusters []uint32             // cached cluster chain to avoid repeated lookups
	allocatedSize  uint64               // size for which clusters have been allocated
	writableFile   backend.WritableFile // cached writable file handle
}

// Get the full cluster chain of the File.
// Getting this file system internal info can be beneficial for some low-level operations, such as:
// - Performing secure erase.
// - Detecting file fragmentation.
// - Passing Disk locations to a different tool that can work with it.
func (fl *File) GetClusterChain() ([]uint32, error) {
	if fl == nil || fl.filesystem == nil {
		return nil, os.ErrClosed
	}

	fs := fl.filesystem
	clusters, err := fs.getClusterList(fl.clusterLocation)
	if err != nil {
		return nil, fmt.Errorf("unable to get list of clusters for file: %v", err)
	}

	return clusters, nil
}

type DiskRange struct {
	Offset uint64
	Length uint64
}

// Get the disk ranges occupied by the File.
// Returns an array of disk ranges, where each entry is a contiguous area on disk.
// This information is similar to that returned by GetClusterChain, just in a different format,
// directly returning disk ranges instead of FAT clusters.
func (fl *File) GetDiskRanges() ([]DiskRange, error) {
	clusters, err := fl.GetClusterChain()
	if err != nil {
		return nil, err
	}

	fs := fl.filesystem
	bytesPerCluster := uint64(fs.bytesPerCluster)
	dataStart := uint64(fs.dataStart)

	var ranges []DiskRange
	var lastCluster uint32

	for _, cluster := range clusters {
		if lastCluster != 0 && cluster == lastCluster+1 {
			// Extend the current range
			ranges[len(ranges)-1].Length += bytesPerCluster
		} else {
			// Add a new range
			offset := dataStart + uint64(cluster-2)*bytesPerCluster
			ranges = append(ranges, DiskRange{
				Offset: offset,
				Length: bytesPerCluster,
			})
		}
		lastCluster = cluster
	}

	return ranges, nil
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF
// reads from the last known offset in the file from last read or write
// and increments the offset by the number of bytes read.
// Use Seek() to set at a particular point
func (fl *File) Read(b []byte) (int, error) {
	if fl == nil || fl.filesystem == nil {
		return 0, os.ErrClosed
	}
	// we have the DirectoryEntry, so we can get the starting cluster location
	// we then get a list of the clusters, and read the data from all of those clusters
	// write the content for the file
	totalRead := 0
	fs := fl.filesystem
	bytesPerCluster := fs.bytesPerCluster
	start := int(fs.dataStart)
	size := int(fl.fileSize) - int(fl.offset)
	maxRead := size
	file := fs.backend
	clusters, err := fs.getClusterList(fl.clusterLocation)
	if err != nil {
		return totalRead, fmt.Errorf("unable to get list of clusters for file: %v", err)
	}
	clusterIndex := 0

	// if there is nothing left to read, just return EOF
	if size <= 0 {
		return totalRead, io.EOF
	}

	// we stop when we hit the lesser of
	//   1- len(b)
	//   2- file end
	if len(b) < maxRead {
		maxRead = len(b)
	}

	// figure out which cluster we start with
	if fl.offset > 0 {
		clusterIndex = int(fl.offset / int64(bytesPerCluster))
		lastCluster := clusters[clusterIndex]
		// read any partials, if needed
		remainder := fl.offset % int64(bytesPerCluster)
		if remainder != 0 {
			offset := int64(start) + int64(lastCluster-2)*int64(bytesPerCluster) + remainder
			toRead := int64(bytesPerCluster) - remainder
			if toRead > int64(len(b)) {
				toRead = int64(len(b))
			}
			_, _ = file.ReadAt(b[0:toRead], offset+fs.start)
			totalRead += int(toRead)
			clusterIndex++
		}
	}

	for i := clusterIndex; i < len(clusters); i++ {
		left := maxRead - totalRead
		toRead := bytesPerCluster
		if toRead > left {
			toRead = left
		}
		offset := int64(start) + int64(clusters[i]-2)*int64(bytesPerCluster)
		_, _ = file.ReadAt(b[totalRead:totalRead+toRead], offset+fs.start)
		totalRead += toRead
		if totalRead >= maxRead {
			break
		}
	}

	fl.offset += int64(totalRead)
	var retErr error
	if fl.offset >= int64(fl.fileSize) {
		retErr = io.EOF
	}
	return totalRead, retErr
}

// ReadFrom reads data from r until EOF and writes it to the file.
// This is an optimized implementation that io.Copy will prefer over repeated Write calls.
// It allocates disk space in larger chunks to minimize FAT table scanning overhead.
func (fl *File) ReadFrom(r io.Reader) (int64, error) {
	if fl == nil || fl.filesystem == nil {
		return 0, os.ErrClosed
	}

	if !fl.isReadWrite {
		return 0, filesystem.ErrReadonlyFilesystem
	}

	// Cache the writable file handle
	if fl.writableFile == nil {
		wf, err := fl.filesystem.backend.Writable()
		if err != nil {
			return 0, err
		}

		fl.writableFile = wf
	}

	writableFile := fl.writableFile
	fs := fl.filesystem
	bytesPerCluster := fs.bytesPerCluster

	start := int64(fs.dataStart)

	// Try to determine source size for optimal allocation using Seeker interface
	var knownSize int64 = -1

	if seeker, ok := r.(io.Seeker); ok {
		currentPos, err := seeker.Seek(0, io.SeekCurrent)
		if err == nil {
			if endPos, err := seeker.Seek(0, io.SeekEnd); err == nil {
				knownSize = endPos - currentPos
				// Reset to original position
				if _, err = seeker.Seek(currentPos, io.SeekStart); err != nil {
					return 0, fmt.Errorf("unable to reset reader position after size determination: %v", err)
				}
			}
		}
	}

	// Allocation chunk size: allocate in 16MB chunks to reduce FAT scanning overhead
	// (only used when source size is unknown)
	const allocationChunkSize = 16 * 1024 * 1024
	chunkClusters := max(allocationChunkSize/bytesPerCluster, 1)

	// Pre-allocate all space if we know the size upfront
	if knownSize > 0 && fl.allocatedSize == 0 {
		targetSize := fl.offset + knownSize
		// Only pre-allocate if it won't exceed FAT32 limit
		if targetSize > 0 && targetSize <= maxFAT32FileSize {
			clusters, err := fs.allocateSpace(uint64(targetSize), fl.clusterLocation)
			if err == nil {
				fl.cachedClusters = clusters
				fl.allocatedSize = uint64(targetSize)
				if len(clusters) > 0 && fl.clusterLocation == 0 {
					fl.clusterLocation = clusters[0]
				}
			}
		}
	}

	totalWritten := int64(0)
	buffer := make([]byte, 32*1024) // 32KB read buffer (matches io.Copy default)

	for {
		// Read data from source
		n, readErr := r.Read(buffer)
		if n > 0 {
			// Calculate new size after this write
			oldSize := int64(fl.fileSize)
			newSize := max(fl.offset+int64(n), oldSize)

			// Check FAT32 file size limit
			if newSize > maxFAT32FileSize {
				return totalWritten, ErrFileTooLarge
			}

			// Allocate space in chunks if needed
			var clusters []uint32
			var err error

			if fl.cachedClusters != nil && uint64(newSize) <= fl.allocatedSize {
				// Use cached cluster list - no need to reallocate
				clusters = fl.cachedClusters
			} else {
				// Need more space - allocate ahead in chunks to minimize FAT scanning
				// Calculate how much extra to allocate beyond what we need right now
				allocateSize := newSize
				extraNeeded := allocateSize - int64(fl.allocatedSize)
				if extraNeeded > 0 {
					// Round up to next allocation chunk boundary
					extraClusters := (extraNeeded + int64(bytesPerCluster) - 1) / int64(bytesPerCluster)
					chunks := (extraClusters + int64(chunkClusters) - 1) / int64(chunkClusters)
					allocateSize = int64(fl.allocatedSize) + chunks*int64(chunkClusters)*int64(bytesPerCluster)
				}

				if fl.cachedClusters != nil {
					clusters, err = fs.allocateSpaceWithCache(uint64(allocateSize), fl.clusterLocation, fl.cachedClusters)
				} else {
					clusters, err = fs.allocateSpace(uint64(allocateSize), fl.clusterLocation)
				}
				if err != nil {
					return totalWritten, fmt.Errorf("unable to allocate clusters for file: %v", err)
				}

				fl.cachedClusters = clusters
				fl.allocatedSize = uint64(allocateSize)

				// Update cluster location if this is the first allocation
				if len(clusters) > 0 && fl.clusterLocation == 0 {
					fl.clusterLocation = clusters[0]
				}
			}

			// Update file size
			if oldSize != newSize {
				fl.fileSize = uint32(newSize)
			}

			// Write the data to clusters
			totalWritten += int64(n)
			clusterIndex := int(fl.offset) / bytesPerCluster
			remainder := fl.offset % int64(bytesPerCluster)
			writePos := 0

			// Handle partial first cluster if offset isn't cluster-aligned
			if remainder != 0 {
				lastCluster := clusters[clusterIndex]
				offset := start + int64(lastCluster-2)*int64(bytesPerCluster) + remainder
				toWrite := min(int64(bytesPerCluster)-remainder, int64(n))
				_, err := writableFile.WriteAt(buffer[writePos:writePos+int(toWrite)], offset+fs.start)
				if err != nil {
					return totalWritten, fmt.Errorf("unable to write to file: %v", err)
				}
				writePos += int(toWrite)
				clusterIndex++
			}

			// Write remaining full/partial clusters
			for writePos < n && clusterIndex < len(clusters) {
				left := n - writePos
				toWrite := min(bytesPerCluster, left)
				offset := start + int64(clusters[clusterIndex]-2)*int64(bytesPerCluster)
				_, err := writableFile.WriteAt(buffer[writePos:writePos+toWrite], offset+fs.start)
				if err != nil {
					return totalWritten, fmt.Errorf("unable to write to file: %v", err)
				}
				writePos += toWrite
				clusterIndex++
			}

			fl.offset += int64(n)
			fl.needsFlush = true
		}

		if readErr != nil {
			if readErr == io.EOF {
				return totalWritten, nil
			}
			return totalWritten, readErr
		}
	}
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// returns a non-nil error when n != len(b)
// writes to the last known offset in the file from last read or write
// and increments the offset by the number of bytes read.
// Use Seek() to set at a particular point
func (fl *File) Write(p []byte) (int, error) {
	if fl == nil || fl.filesystem == nil {
		return 0, os.ErrClosed
	}

	totalWritten := 0

	// Cache the writable file handle
	if fl.writableFile == nil {
		wf, err := fl.filesystem.backend.Writable()
		if err != nil {
			return totalWritten, err
		}
		fl.writableFile = wf
	}
	writableFile := fl.writableFile

	fs := fl.filesystem
	// if the file was not opened RDWR, nothing we can do
	if !fl.isReadWrite {
		return totalWritten, filesystem.ErrReadonlyFilesystem
	}
	// what is the new file size?
	writeSize := len(p)
	oldSize := int64(fl.fileSize)
	newSize := fl.offset + int64(writeSize)
	if newSize < oldSize {
		newSize = oldSize
	}

	// Check FAT32 file size limit
	if newSize > maxFAT32FileSize {
		return 0, ErrFileTooLarge
	}

	// Get or allocate clusters - use cached cluster list if available and sufficient
	var (
		clusters []uint32
		err      error
	)
	if fl.cachedClusters != nil && uint64(newSize) <= fl.allocatedSize {
		// Use cached cluster list - no need to reallocate or query FAT
		clusters = fl.cachedClusters
	} else {
		// Need to allocate more space
		// Pass the cached cluster list to avoid re-reading from FAT
		if fl.cachedClusters != nil {
			clusters, err = fs.allocateSpaceWithCache(uint64(newSize), fl.clusterLocation, fl.cachedClusters)
		} else {
			clusters, err = fs.allocateSpace(uint64(newSize), fl.clusterLocation)
		}
		if err != nil {
			return 0x00, fmt.Errorf("unable to allocate clusters for file: %v", err)
		}
		// Cache the cluster list and allocated size
		fl.cachedClusters = clusters
		fl.allocatedSize = uint64(newSize)
	}

	// update the directory entry size for the file
	if oldSize != newSize {
		fl.fileSize = uint32(newSize)
	}
	// write the content for the file
	bytesPerCluster := fl.filesystem.bytesPerCluster
	start := int(fl.filesystem.dataStart)
	clusterIndex := 0

	// figure out which cluster we start with
	if fl.offset > 0 {
		clusterIndex = int(fl.offset) / bytesPerCluster
		lastCluster := clusters[clusterIndex]
		// write any partials, if needed
		remainder := fl.offset % int64(bytesPerCluster)
		if remainder != 0 {
			offset := int64(start) + int64(lastCluster-2)*int64(bytesPerCluster) + remainder
			toWrite := int64(bytesPerCluster) - remainder
			// max we can write
			if toWrite > int64(len(p)) {
				toWrite = int64(len(p))
			}
			_, err := writableFile.WriteAt(p[0:toWrite], offset+fs.start)
			if err != nil {
				return totalWritten, fmt.Errorf("unable to write to file: %v", err)
			}
			totalWritten += int(toWrite)
			clusterIndex++
		}
	}

	for i := clusterIndex; i < len(clusters); i++ {
		left := len(p) - totalWritten
		toWrite := bytesPerCluster
		if toWrite > left {
			toWrite = left
		}
		offset := int64(start) + int64(clusters[i]-2)*int64(bytesPerCluster)
		_, err := writableFile.WriteAt(p[totalWritten:totalWritten+toWrite], offset+fs.start)
		if err != nil {
			return totalWritten, fmt.Errorf("unable to write to file: %v", err)
		}
		totalWritten += toWrite
	}

	fl.offset += int64(totalWritten)

	// mark that directory entries need to be written on Close()
	fl.needsFlush = true

	return totalWritten, nil
}

// Seek set the offset to a particular point in the file
func (fl *File) Seek(offset int64, whence int) (int64, error) {
	if fl == nil || fl.filesystem == nil {
		return 0, os.ErrClosed
	}
	newOffset := int64(0)
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekEnd:
		newOffset = int64(fl.fileSize) + offset
	case io.SeekCurrent:
		newOffset = fl.offset + offset
	}
	if newOffset < 0 {
		return fl.offset, fmt.Errorf("cannot set offset %d before start of file", offset)
	}
	fl.offset = newOffset
	return fl.offset, nil
}

// Close close the file
func (fl *File) Close() error {
	if fl == nil || fl.filesystem == nil {
		return nil
	}

	// if FAT was modified, flush it first
	if fl.filesystem.fatDirty {
		if err := fl.filesystem.writeFsis(); err != nil {
			return fmt.Errorf("error writing file system information sector: %v", err)
		}
		if err := fl.filesystem.writeFat(); err != nil {
			return fmt.Errorf("error writing file allocation table: %v", err)
		}
		fl.filesystem.fatDirty = false
	}

	// if file was modified, flush directory entries
	if fl.needsFlush {
		if err := fl.filesystem.writeDirectoryEntries(fl.parent); err != nil {
			return fmt.Errorf("error writing directory entries to disk: %v", err)
		}
		fl.needsFlush = false
	}

	fl.filesystem = nil
	return nil
}

func (fl *File) SetSystem(on bool) error {
	fl.isSystem = on
	fl.needsFlush = true
	return nil
}

func (fl *File) IsSystem() bool {
	return fl.isSystem
}

func (fl *File) SetHidden(on bool) error {
	fl.isHidden = on
	fl.needsFlush = true
	return nil
}

func (fl *File) IsHidden() bool {
	return fl.isHidden
}

func (fl *File) SetReadOnly(on bool) error {
	fl.isReadOnly = on
	fl.needsFlush = true
	return nil
}

func (fl *File) IsReadOnly() bool {
	return fl.isReadOnly
}
