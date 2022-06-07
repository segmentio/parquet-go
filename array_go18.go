//go:build go1.18

package parquet

import "unsafe"

func makeArray[T any](s []T) array {
	return array{
		ptr: *(*unsafe.Pointer)(unsafe.Pointer(&s)),
		len: len(s),
	}
}

func makeSlice[T any](a array) []T {
	return slice[T](a.ptr, a.len)
}

func slice[T any](p unsafe.Pointer, n int) []T {
	return unsafe.Slice((*T)(p), n)
}
