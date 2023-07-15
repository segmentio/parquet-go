package parquet

import (
	"unsafe"

	"github.com/parquet-go/parquet-go/internal/unsafecast"
	"github.com/parquet-go/parquet-go/sparse"
)

func makeArrayValue(values []Value, offset uintptr) sparse.Array {
	ptr := *(*unsafe.Pointer)(unsafe.Pointer(&values))
	return sparse.UnsafeArray(unsafe.Add(ptr, offset), len(values), unsafe.Sizeof(Value{}))
}

func makeArrayString(values []string) sparse.Array {
	str := ""
	ptr := *(*unsafe.Pointer)(unsafe.Pointer(&values))
	return sparse.UnsafeArray(ptr, len(values), unsafe.Sizeof(str))
}

func makeArray(base unsafe.Pointer, length int, offset uintptr) sparse.Array {
	return sparse.UnsafeArray(base, length, offset)
}

func makeArrayOf[T any](s []T) sparse.Array {
	var model T
	return makeArray(unsafecast.PointerOf(s), len(s), unsafe.Sizeof(model))
}

type sliceHeader struct {
	base unsafe.Pointer
	len  int
	cap  int
}
