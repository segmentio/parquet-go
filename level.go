package parquet

import "github.com/segmentio/parquet-go/internal/bits"

func countLevelsEqual(levels []byte, value byte) int {
	return bits.CountByte(levels, value)
}

func countLevelsNotEqual(levels []byte, value byte) int {
	return len(levels) - countLevelsEqual(levels, value)
}

func appendLevel(levels []byte, value byte, count int) []byte {
	i := len(levels)
	n := len(levels) + count

	if cap(levels) < n {
		newLevels := make([]byte, n, 2*n)
		copy(newLevels, levels)
		levels = newLevels
	} else {
		levels = levels[:n]
	}

	memset(levels[i:], value)
	return levels
}
