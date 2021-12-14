package bits

func AddInt32(data []int32, value int32) {
	for i := range data {
		data[i] += value
	}
}

func AddInt64(data []int64, value int64) {
	for i := range data {
		data[i] += value
	}
}
