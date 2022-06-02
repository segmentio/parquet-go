//go:build go1.18 && (purego || !amd64)

package parquet

import (
	"unsafe"
)

func nullIndexInt(a array) int {
	for i, v := range unsafe.Slice((*int)(a.ptr), a.len) {
		if v == 0 {
			return i
		}
	}
	return a.len
}

func nullIndexInt32(a array) int {
	for i, v := range unsafe.Slice((*int32)(a.ptr), a.len) {
		if v == 0 {
			return i
		}
	}
	return a.len
}

func nullIndexInt64(a array) int {
	for i, v := range unsafe.Slice((*int64)(a.ptr), a.len) {
		if v == 0 {
			return i
		}
	}
	return a.len
}

func nullIndexUint(a array) int {
	for i, v := range unsafe.Slice((*uint)(a.ptr), a.len) {
		if v == 0 {
			return i
		}
	}
	return a.len
}

func nullIndexUint32(a array) int {
	for i, v := range unsafe.Slice((*uint32)(a.ptr), a.len) {
		if v == 0 {
			return i
		}
	}
	return a.len
}

func nullIndexUint64(a array) int {
	for i, v := range unsafe.Slice((*uint64)(a.ptr), a.len) {
		if v == 0 {
			return i
		}
	}
	return a.len
}

func nullIndexOfUint128(a array) int {
	for i, v := range unsafe.Slice((*[16]byte)(a.ptr), a.len) {
		if v == ([16]byte{}) {
			return i
		}
	}
	return a.len
}

func nullIndexFloat32(a array) int {
	for i, v := range unsafe.Slice((*float32)(a.ptr), a.len) {
		if v == 0 {
			return i
		}
	}
	return a.len
}

func nullIndexFloat64(a array) int {
	for i, v := range unsafe.Slice((*float64)(a.ptr), a.len) {
		if v == 0 {
			return i
		}
	}
	return a.len
}

func nullIndexPointer(a array) int {
	for i, v := range unsafe.Slice((*unsafe.Pointer)(a.ptr), a.len) {
		if v == nil {
			return i
		}
	}
	return a.len
}
