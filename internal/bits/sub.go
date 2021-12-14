package bits

func SubInt32(data []int32, value int32) {
	for i := range data {
		data[i] -= value
	}
}

func SubInt64(data []int64, value int64) {
	for i := range data {
		data[i] -= value
	}
}
