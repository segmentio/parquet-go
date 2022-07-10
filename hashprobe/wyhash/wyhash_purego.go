//go:build purego || !amd64

package wyhash

import "github.com/segmentio/parquet-go/hashprobe/sparse"

func MultiHashArray32(hashes []uintptr, values sparse.Array32, seed uintptr) {
	for i := range hashes {
		hashes[i] = Hash32(values.Index(i))
	}
}

func MultiHashArray64(hashes []uintptr, values sparse.Array64, seed uintptr) {
	for i := range hashes {
		hashes[i] = Hash64(values.Index(i))
	}
}

func MultiHashArray128(hashes []uintptr, values sparse.Array128, seed uintptr) {
	for i := range hashes {
		hashes[i] = Hash128(values.Index(i))
	}
}
