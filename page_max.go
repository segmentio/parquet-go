package parquet

import (
	"bytes"
	"encoding/binary"
)

func maxBE128(data [][16]byte) (min []byte) {
	if len(data) > 0 {
		m := binary.BigEndian.Uint64(data[0][:8])
		j := 0
		for i := 1; i < len(data); i++ {
			x := binary.BigEndian.Uint64(data[i][:8])
			switch {
			case x > m:
				m, j = x, i
			case x == m:
				y := binary.BigEndian.Uint64(data[i][8:])
				n := binary.BigEndian.Uint64(data[j][8:])
				if y > n {
					m, j = x, i
				}
			}
		}
		min = data[j][:]
	}
	return min
}

func maxFixedLenByteArray(data []byte, size int) (max []byte) {
	if len(data) > 0 {
		max = data[:size]

		for i, j := size, 2*size; j <= len(data); {
			item := data[i:j]

			if bytes.Compare(item, max) > 0 {
				max = item
			}

			i += size
			j += size
		}
	}
	return max
}
