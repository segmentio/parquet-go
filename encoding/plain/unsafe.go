package plain

import "unsafe"

func unsafeInt32ToBytes(data []int32) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 4*len(data))
}

func unsafeInt64ToBytes(data []int64) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 8*len(data))
}

func unsafeInt96ToBytes(data [][12]byte) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 12*len(data))
}

func unsafeFloat32ToBytes(data []float32) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 4*len(data))
}

func unsafeFloat64ToBytes(data []float64) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 8*len(data))
}
