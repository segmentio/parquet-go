package format

import "sort"

// SortKeyValueMetadata sorts the slice of KeyValueMetadata entries.
func SortKeyValueMetadata(kv []KeyValue) {
	sort.Slice(kv, func(i, j int) bool {
		switch {
		case kv[i].Key < kv[j].Key:
			return true
		case kv[i].Key > kv[j].Key:
			return false
		default:
			return kv[i].Value < kv[j].Value
		}
	})
}
