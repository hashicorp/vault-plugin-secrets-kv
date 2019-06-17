package kv

import (
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/duration"
)

// deletionTime returns the time of creation plus the duration of the
// minimum non-zero value of mount, meta, or data. If mount, meta, and data
// are zero, false is returned.
func deletionTime(creation time.Time, mount, meta, data time.Duration) (time.Time, bool) {
	if mount == 0 && meta == 0 && data == 0 {
		return time.Time{}, false
	}
	var min time.Duration
	if data != 0 {
		min = data
	}
	if meta != 0 && meta < min || min == 0 {
		min = meta
	}
	if mount != 0 && mount < min || min == 0 {
		min = mount
	}
	return creation.Add(min), true
}

type deleteVersionAfterGetter interface {
	GetDeleteVersionAfter() *duration.Duration
}

func deleteVersionAfter(v deleteVersionAfterGetter) time.Duration {
	if v.GetDeleteVersionAfter() == nil {
		return time.Duration(0)
	}
	dva, err := ptypes.Duration(v.GetDeleteVersionAfter())
	if err != nil {
		return time.Duration(0)
	}
	return dva
}
