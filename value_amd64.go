//go:build !purego

package parquet

//go:noescape
func memsetValuesAVX2(values []Value, model Value)

func memsetValues(values []Value, model Value) {
	if hasAVX2 {
		memsetValuesAVX2(values, model)
	} else {
		for i := range values {
			values[i] = model
		}
	}
}
