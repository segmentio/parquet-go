package parquet

func atLeastOne(size int) int {
	return atLeast(size, 1)
}

func atLeast(size, least int) int {
	if size > least {
		return size
	}
	return least
}
