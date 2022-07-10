//go:build !purego

package wyhash

import "github.com/segmentio/parquet-go/hashprobe/sparse"

//go:noescape
func MultiHashArray32(hashes []uintptr, values sparse.Array32, seed uintptr)

//go:noescape
func MultiHashArray64(hashes []uintptr, values sparse.Array64, seed uintptr)

//go:noescape
func MultiHashArray128(hashes []uintptr, values sparse.Array128, seed uintptr)
