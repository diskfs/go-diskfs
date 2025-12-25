package part

import "fmt"

type IncompletePartitionWriteError struct {
	writtenBytes uint64
	totalBytes   uint64
}

func (e *IncompletePartitionWriteError) Error() string {
	return fmt.Errorf("wrote %d bytes to partition of size %d", e.writtenBytes, e.totalBytes).Error()
}

func NewIncompletePartitionWriteError(written, total uint64) error {
	return &IncompletePartitionWriteError{
		writtenBytes: written,
		totalBytes:   total,
	}
}
