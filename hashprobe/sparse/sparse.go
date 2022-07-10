package sparse

import "unsafe"

type Int32Array struct{ array }

func MakeInt32Array(values []int32) Int32Array {
	return Int32Array{makeArray(*(*unsafe.Pointer)(unsafe.Pointer(&values)), uintptr(len(values)), 4)}
}

func UnsafeInt32Array(base unsafe.Pointer, length int, offset uintptr) Int32Array {
	return Int32Array{makeArray(base, uintptr(length), offset)}
}

func (a *Int32Array) Len() int { return int(a.len) }

func (a *Int32Array) Index(i int) int32 { return *(*int32)(a.index(i)) }

func (a *Int32Array) Slice(i, j int) Int32Array { return Int32Array{a.slice(i, j)} }

func (a *Int32Array) Uint32Array() Uint32Array { return Uint32Array{a.array} }

type Float32Array struct{ array }

func MakeFloat32Array(values []float32) Float32Array {
	return Float32Array{makeArray(*(*unsafe.Pointer)(unsafe.Pointer(&values)), uintptr(len(values)), 4)}
}

func UnsafeFloat32Array(base unsafe.Pointer, length int, offset uintptr) Float32Array {
	return Float32Array{makeArray(base, uintptr(length), offset)}
}

func (a *Float32Array) Len() int { return int(a.len) }

func (a *Float32Array) Index(i int) float32 { return *(*float32)(a.index(i)) }

func (a *Float32Array) Slice(i, j int) Float32Array { return Float32Array{a.slice(i, j)} }

func (a *Float32Array) Uint32Array() Uint32Array { return Uint32Array{a.array} }

type Uint32Array struct{ array }

func MakeUint32Array(values []uint32) Uint32Array {
	return Uint32Array{makeArray(*(*unsafe.Pointer)(unsafe.Pointer(&values)), uintptr(len(values)), 4)}
}

func UnsafeUint32Array(base unsafe.Pointer, length int, offset uintptr) Uint32Array {
	return Uint32Array{makeArray(base, uintptr(length), offset)}
}

func (a *Uint32Array) Len() int { return int(a.len) }

func (a *Uint32Array) Index(i int) uint32 { return *(*uint32)(a.index(i)) }

func (a *Uint32Array) Slice(i, j int) Uint32Array { return Uint32Array{a.slice(i, j)} }

type Int64Array struct{ array }

func MakeInt64Array(values []int64) Int64Array {
	return Int64Array{makeArray(*(*unsafe.Pointer)(unsafe.Pointer(&values)), uintptr(len(values)), 8)}
}

func UnsafeInt64Array(base unsafe.Pointer, length int, offset uintptr) Int64Array {
	return Int64Array{makeArray(base, uintptr(length), offset)}
}

func (a *Int64Array) Len() int { return int(a.len) }

func (a *Int64Array) Index(i int) int64 { return *(*int64)(a.index(i)) }

func (a *Int64Array) Slice(i, j int) Int64Array { return Int64Array{a.slice(i, j)} }

func (a *Int64Array) Uint64Array() Uint64Array { return Uint64Array{a.array} }

type Float64Array struct{ array }

func MakeFloat64Array(values []float64) Float64Array {
	return Float64Array{makeArray(*(*unsafe.Pointer)(unsafe.Pointer(&values)), uintptr(len(values)), 8)}
}

func UnsafeFloat64Array(base unsafe.Pointer, length int, offset uintptr) Float64Array {
	return Float64Array{makeArray(base, uintptr(length), offset)}
}

func (a *Float64Array) Len() int { return int(a.len) }

func (a *Float64Array) Index(i int) float64 { return *(*float64)(a.index(i)) }

func (a *Float64Array) Slice(i, j int) Float64Array { return Float64Array{a.slice(i, j)} }

func (a *Float64Array) Uint64Array() Uint64Array { return Uint64Array{a.array} }

type Uint64Array struct{ array }

func MakeUint64Array(values []uint64) Uint64Array {
	return Uint64Array{makeArray(*(*unsafe.Pointer)(unsafe.Pointer(&values)), uintptr(len(values)), 8)}
}

func UnsafeUint64Array(base unsafe.Pointer, length int, offset uintptr) Uint64Array {
	return Uint64Array{makeArray(base, uintptr(length), offset)}
}

func (a *Uint64Array) Len() int { return int(a.len) }

func (a *Uint64Array) Index(i int) uint64 { return *(*uint64)(a.index(i)) }

func (a *Uint64Array) Slice(i, j int) Uint64Array { return Uint64Array{a.slice(i, j)} }

type Uint128Array struct{ array }

func MakeUint128Array(values [][16]byte) Uint128Array {
	return Uint128Array{makeArray(*(*unsafe.Pointer)(unsafe.Pointer(&values)), uintptr(len(values)), 16)}
}

func UnsafeUint128Array(base unsafe.Pointer, length int, offset uintptr) Uint128Array {
	return Uint128Array{makeArray(base, uintptr(length), offset)}
}

func (a *Uint128Array) Len() int { return int(a.len) }

func (a *Uint128Array) Index(i int) [16]byte { return *(*[16]byte)(a.index(i)) }

func (a *Uint128Array) Slice(i, j int) Uint128Array { return Uint128Array{a.slice(i, j)} }

type array struct {
	ptr unsafe.Pointer
	len uintptr
	off uintptr
}

func makeArray(base unsafe.Pointer, length, offset uintptr) array {
	return array{ptr: base, len: length, off: offset}
}

func (a *array) index(i int) unsafe.Pointer {
	if uintptr(i) >= a.len {
		panic("index out of bounds")
	}
	return unsafe.Add(a.ptr, a.off*uintptr(i))
}

func (a *array) slice(i, j int) array {
	if uintptr(i) > a.len || uintptr(j) > a.len || i > j {
		panic("slice index out of bounds")
	}
	return array{
		ptr: unsafe.Add(a.ptr, a.off*uintptr(i)),
		len: uintptr(j - i),
		off: a.off,
	}
}
