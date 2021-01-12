package formats

// Format represents the format of the disk
type Format int

const (
	// Raw disk format for basic raw disk
	Unknown Format = iota
	Raw
	Qcow2
)
