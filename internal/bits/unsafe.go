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
