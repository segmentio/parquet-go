package parquet

type int96 = [12]byte

func atLeastOne(size int) int {
	return atLeast(size, 1)
}

func atLeast(size, least int) int {
	if size > least {
		return size
	}
	return least
}
