package parquet

import (
	"bytes"
	"encoding/binary"
)

func minBE128(data [][16]byte) (min []byte) {
	if len(data) > 0 {
		m := binary.BigEndian.Uint64(data[0][:8])
		j := 0
		for i := 1; i < len(data); i++ {
			x := binary.BigEndian.Uint64(data[i][:8])
			switch {
			case x < m:
				m, j = x, i
			case x == m:
				y := binary.BigEndian.Uint64(data[i][8:])
				n := binary.BigEndian.Uint64(data[j][8:])
				if y < n {
					m, j = x, i
				}
			}
		}
		min = data[j][:]
	}
	return min
}

func minFixedLenByteArray(data []byte, size int) (min []byte) {
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
