package parquet

import "bytes"

func minFixedLenByteArray(size int, data []byte) (min []byte) {
	if len(data) > 0 {
		min = data[:size]

		for i, j := size, 2*size; j <= len(data); {
			item := data[i:j]

			if bytes.Compare(item, min) < 0 {
				min = item
			}

			i += size
			j += size
		}
	}
	return min
}
