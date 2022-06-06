package parquet

import "unsafe"

type array struct {
	ptr unsafe.Pointer
	len int
}

func makeValueArray(values []Value) array {
	return *(*array)(unsafe.Pointer(&values))
}

func (a array) index(i int, size, offset uintptr) unsafe.Pointer {
	return unsafe.Add(a.ptr, uintptr(i)*size+offset)
}

func (a array) slice(i, j int, size, offset uintptr) array {
	if i < 0 || i > a.len || j < 0 || j > a.len {
		panic("slice index out of bounds")
	}
	if i > j {
		panic("negative slice length")
	}
	return array{
		ptr: a.index(i, size, offset),
		len: j - i,
	}
}
