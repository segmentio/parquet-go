//go:build go1.18 && (purego || !amd64)

package parquet

func nullIndexInt(a array) int { return nullIndex[int](a) }

func nullIndexInt32(a array) int { return nullIndex[int32](a) }

func nullIndexInt64(a array) int { return nullIndex[int64](a) }

func nullIndexUint(a array) int { return nullIndex[uint](a) }

func nullIndexUint32(a array) int { return nullIndex[uint32](a) }

func nullIndexUint64(a array) int { return nullIndex[uint64](a) }

func nullIndexOfUint128(a array) int { return nullIndex[[16]byte](a) }

func nullIndexFloat32(a array) int { return nullIndex[float32](a) }

func nullIndexFloat64(a array) int { return nullIndex[float64](a) }

func nullIndexPointer(a array) int { return nullIndex[*byte](a) }

func nonNullIndexBool(a array) int { return nonNullIndex[bool](a) }

func nonNullIndexInt(a array) int { return nonNullIndex[int](a) }

func nonNullIndexInt32(a array) int { return nonNullIndex[int32](a) }

func nonNullIndexInt64(a array) int { return nonNullIndex[int64](a) }

func nonNullIndexUint(a array) int { return nonNullIndex[uint](a) }

func nonNullIndexUint32(a array) int { return nonNullIndex[uint32](a) }

func nonNullIndexUint64(a array) int { return nonNullIndex[uint64](a) }

func nonNullIndexUint128(a array) int { return nonNullIndex[[16]byte](a) }

func nonNullIndexFloat32(a array) int { return nonNullIndex[float32](a) }

func nonNullIndexFloat64(a array) int { return nonNullIndex[float64](a) }

func nonNullIndexPointer(a array) int { return nonNullIndex[*byte](a) }
