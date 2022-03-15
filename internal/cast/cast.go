//go:build go1.18

package cast

import "unsafe"

type Uint128 = [16]byte

type Type interface {
	~bool | ~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~Uint128
}

func Slice[To, From Type](data []From) []To {
	var zf From
	var zt To
	return unsafe.Slice(*(**To)(unsafe.Pointer(&data)), (uintptr(len(data))*unsafe.Sizeof(zf))/unsafe.Sizeof(zt))
}

func SliceToBytes[T Type](data []T) []byte {
	return Slice[byte](data)
}

func BytesToSlice[T Type](data []byte) []T {
	return Slice[T](data)
}

func BytesToString(data []byte) string {
	return *(*string)(unsafe.Pointer(&data))
}
