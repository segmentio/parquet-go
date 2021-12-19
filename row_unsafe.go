package parquet

import (
	"reflect"
	"unsafe"
)

// This function is an optimized implementation of the following code:
//
// 	func reflectAppend(value, elem reflect.Value) reflect.Value {
// 		value.Set(reflect.Append(value, elem))
// 		return value.Index(value.Len() - 1)
// 	}
//
// The use of reflect.Append requires the Go runtime to make heap allocations for
// the slice values boxed into the returned reflect.Value of Append. It measn that
// the number of malloc increases linearly with the number of values appended to
// a slice, which gets expensive very quickly.
func reflectAppend(value, elem reflect.Value, elemSize uintptr) reflect.Value {
	v := (*iface)(unsafe.Pointer(&value))
	e := (*iface)(unsafe.Pointer(&elem))
	s := (*slice)(v.ptr)

	if s.len == s.cap {
		n := 2 * s.cap
		if n == 0 {
			// Here is another optimization provided by the function;
			// the default append behavior is to start with a capacity of 1,
			// starting at 10 saves 2-3 alloc on average when reconstructing
			// small slices.
			n = 10
		}
		x := slice{
			ptr: unsafe_NewArray(e.typ, n),
			len: s.len,
			cap: n,
		}
		typedslicecopy(e.typ, x, *s)
		*s = x
	}

	dst := unsafe.Add(s.ptr, uintptr(s.len)*elemSize)
	typedmemmove(e.typ, dst, e.ptr)
	i := s.len
	s.len++
	return value.Index(i)
}

type iface struct {
	typ unsafe.Pointer
	ptr unsafe.Pointer
}

type slice struct {
	ptr unsafe.Pointer
	len int
	cap int
}

//go:linkname unsafe_NewArray reflect.unsafe_NewArray
func unsafe_NewArray(rtype unsafe.Pointer, length int) unsafe.Pointer

//go:linkname typedslicecopy reflect.typedslicecopy
func typedslicecopy(elemType unsafe.Pointer, dst, src slice) int

//go:linkname typedmemmove runtime.typedmemmove
func typedmemmove(typ, dst, src unsafe.Pointer)
