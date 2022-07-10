package sparse

import "unsafe"

type Array32 struct {
	ptr unsafe.Pointer
	len uintptr
	off uintptr
}

func MakeArray32(values []uint32) Array32 {
	return Array32{
		ptr: *(*unsafe.Pointer)(unsafe.Pointer(&values)),
		len: uintptr(len(values)),
		off: 4,
	}
}

func UnsafeArray32(base unsafe.Pointer, length, offset uintptr) Array32 {
	return Array32{
		ptr: base,
		len: length,
		off: offset,
	}
}

func (a *Array32) Len() int {
	return int(a.len)
}

func (a *Array32) Index(i int) uint32 {
	checkBounds(uintptr(i), a.len)
	return *(*uint32)(unsafe.Add(a.ptr, a.off*uintptr(i)))
}

type Array64 struct {
	ptr unsafe.Pointer
	len uintptr
	off uintptr
}

func MakeArray64(values []uint64) Array64 {
	return Array64{
		ptr: *(*unsafe.Pointer)(unsafe.Pointer(&values)),
		len: uintptr(len(values)),
		off: 8,
	}
}

func UnsafeArray64(base unsafe.Pointer, length, offset uintptr) Array64 {
	return Array64{
		ptr: base,
		len: length,
		off: offset,
	}
}

func (a *Array64) Len() int {
	return int(a.len)
}

func (a *Array64) Index(i int) uint64 {
	checkBounds(uintptr(i), a.len)
	return *(*uint64)(unsafe.Add(a.ptr, a.off*uintptr(i)))
}

type Array128 struct {
	ptr unsafe.Pointer
	len uintptr
	off uintptr
}

func MakeArray128(values [][16]byte) Array128 {
	return Array128{
		ptr: *(*unsafe.Pointer)(unsafe.Pointer(&values)),
		len: uintptr(len(values)),
		off: 16,
	}
}

func UnsafeArray128(base unsafe.Pointer, length, offset uintptr) Array128 {
	return Array128{
		ptr: base,
		len: length,
		off: offset,
	}
}

func (a *Array128) Len() int {
	return int(a.len)
}

func (a *Array128) Index(i int) [16]byte {
	checkBounds(uintptr(i), a.len)
	return *(*[16]byte)(unsafe.Add(a.ptr, a.off*uintptr(i)))
}

func checkBounds(i, n uintptr) {
	if i >= n {
		panic("index out of bounds")
	}
}
