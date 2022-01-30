//go:build purego || !amd64

package bloom

func filterInsertBulk(f []Block, x []uint64) {
	filterInsertBulkGeneric(f, x)
}

func filterInsert(f []Block, x uint64) {
	filterInsertGeneric(f, x)
}

func filterCheck(f []Block, x uint64) bool {
	return filterCheckGeneric(f, x)
}
