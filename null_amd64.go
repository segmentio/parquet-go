//go:build go1.18 && !purego

package parquet

//go:noescape
func nullIndex32bits(array) int

//go:noescape
func nullIndex64bits(array) int

//go:noescape
func nullIndex128bits(array) int

func nullIndexInt(a array) int { return nullIndex64bits(a) }

func nullIndexInt32(a array) int { return nullIndex32bits(a) }

func nullIndexInt64(a array) int { return nullIndex64bits(a) }

func nullIndexUint(a array) int { return nullIndex64bits(a) }

func nullIndexUint32(a array) int { return nullIndex32bits(a) }

func nullIndexUint64(a array) int { return nullIndex64bits(a) }

func nullIndexOfUint128(a array) int { return nullIndex128bits(a) }

func nullIndexFloat32(a array) int { return nullIndex32bits(a) }

func nullIndexFloat64(a array) int { return nullIndex64bits(a) }

func nullIndexPointer(a array) int { return nullIndex64bits(a) }

//go:noescape
func nonNullIndex8bits(array) int

//go:noescape
func nonNullIndex32bits(array) int

//go:noescape
func nonNullIndex64bits(array) int

//go:noescape
func nonNullIndex128bits(array) int

func nonNullIndexBool(a array) int { return nonNullIndex8bits(a) }

func nonNullIndexInt(a array) int { return nonNullIndex64bits(a) }

func nonNullIndexInt32(a array) int { return nonNullIndex32bits(a) }

func nonNullIndexInt64(a array) int { return nonNullIndex64bits(a) }

func nonNullIndexUint(a array) int { return nonNullIndex64bits(a) }

func nonNullIndexUint32(a array) int { return nonNullIndex32bits(a) }

func nonNullIndexUint64(a array) int { return nonNullIndex64bits(a) }

func nonNullIndexUint128(a array) int { return nonNullIndex128bits(a) }

func nonNullIndexFloat32(a array) int { return nonNullIndex32bits(a) }

func nonNullIndexFloat64(a array) int { return nonNullIndex64bits(a) }

func nonNullIndexPointer(a array) int { return nonNullIndex64bits(a) }
