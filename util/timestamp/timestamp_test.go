package timestamp_test

import (
	"testing"
	"time"

	"github.com/diskfs/go-diskfs/util/timestamp"
)

func TestTimeStamp(t *testing.T) {
	for _, tt := range []struct {
		name             string
		sourceDateEpoch  string
		expectedTimeFunc func() time.Time
	}{
		{
			name: "source date epoch not set",
			expectedTimeFunc: func() time.Time {
				return time.Now().UTC()
			},
		},
		{
			name:            "source date epoch set",
			sourceDateEpoch: "1609459200",
			expectedTimeFunc: func() time.Time {
				return time.Unix(1609459200, 0).UTC()
			},
		},
		{
			name:            "source date epoch invalid",
			sourceDateEpoch: "invalid",
			expectedTimeFunc: func() time.Time {
				return time.Now().UTC()
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// set SOURCE_DATE_EPOCH environment variable
			if tt.sourceDateEpoch != "" {
				t.Setenv("SOURCE_DATE_EPOCH", tt.sourceDateEpoch)
			}

			got := timestamp.GetTime()
			expected := tt.expectedTimeFunc()
			if !got.Truncate(time.Second).Equal(expected.Truncate(time.Second)) {
				t.Errorf("GetTime() = %v, want %v", got, expected)
			}
		})
	}
}
