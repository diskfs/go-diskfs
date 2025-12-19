// Package timestamp provides utilities for handling timestamps
package timestamp

import (
	"os"
	"strconv"
	"time"
)

// GetTime returns the current time in UTC, honoring SOURCE_DATE_EPOCH if set.
// SOURCE_DATE_EPOCH is a Unix timestamp used for reproducible builds.
// If SOURCE_DATE_EPOCH is not set or invalid, it returns time.Now().UTC().
func GetTime() time.Time {
	if epoch := os.Getenv("SOURCE_DATE_EPOCH"); epoch != "" {
		if timestamp, err := strconv.ParseInt(epoch, 10, 64); err == nil {
			return time.Unix(timestamp, 0).UTC()
		}
	}

	return time.Now().UTC()
}
