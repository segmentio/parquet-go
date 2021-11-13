package bits

import "unsafe"

func BoolToBytes(data []bool) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), len(data))
}

func Int32ToBytes(data []int32) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 4*len(data))
}

func Int64ToBytes(data []int64) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 8*len(data))
}

func Int96ToBytes(data [][12]byte) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 12*len(data))
}

func Float32ToBytes(data []float32) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 4*len(data))
}

func Float64ToBytes(data []float64) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 8*len(data))
}

func Int32ToUint32(data []int32) []uint32 {
	return unsafe.Slice(*(**uint32)(unsafe.Pointer(&data)), len(data))
}

func Int64ToUint64(data []int64) []uint64 {
	return unsafe.Slice(*(**uint64)(unsafe.Pointer(&data)), len(data))
}

func BytesToBE128(data []byte) [][16]byte {
	return unsafe.Slice(*(**[16]byte)(unsafe.Pointer(&data)), len(data)/16)
}
