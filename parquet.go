package parquet

import (
	"sort"

	"github.com/segmentio/parquet/format"
)

func atLeastOne(size int) int {
	return atLeast(size, 1)
}

func atLeast(size, least int) int {
	if size > least {
		return size
	}
	return least
}

func sortKeyValueMetadata(keyValueMetadata []format.KeyValue) {
	sort.Slice(keyValueMetadata, func(i, j int) bool {
		return keyValueMetadata[i].Key < keyValueMetadata[j].Key
	})
}

func lookupKeyValueMetadata(keyValueMetadata []format.KeyValue, key string) (value string, ok bool) {
	i := sort.Search(len(keyValueMetadata), func(i int) bool {
		return keyValueMetadata[i].Key >= key
	})
	if i == len(keyValueMetadata) || keyValueMetadata[i].Key != key {
		return "", false
	}
	return keyValueMetadata[i].Value, true
}
