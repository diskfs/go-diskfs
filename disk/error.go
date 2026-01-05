package disk

import "fmt"

type UnknownFilesystemError struct {
	partition int
}

func (e *UnknownFilesystemError) Error() string {
	return fmt.Sprintf("unknown filesystem type on partition %d", e.partition)
}

func NewUnknownFilesystemError(partition int) *UnknownFilesystemError {
	return &UnknownFilesystemError{
		partition: partition,
	}
}

type NoPartitionTableError struct{}

func (e *NoPartitionTableError) Error() string {
	return "no partition table found on disk"
}

type MaxPartitionsExceededError struct {
	requested int
	max       int
}

func (e *MaxPartitionsExceededError) Error() string {
	return fmt.Sprintf("requested partition %d exceeds maximum partitions %d", e.requested, e.max)
}

func NewMaxPartitionsExceededError(requested, maxPart int) *MaxPartitionsExceededError {
	return &MaxPartitionsExceededError{
		requested: requested,
		max:       maxPart,
	}
}

type InvalidPartitionError struct {
	requested int
}

func (e *InvalidPartitionError) Error() string {
	return fmt.Sprintf("requested partition %d not found", e.requested)
}

func NewInvalidPartitionError(requested int) *InvalidPartitionError {
	return &InvalidPartitionError{
		requested: requested,
	}
}
