package parquet

import "math"

const (
	// MaxColumnDepth is the maximum column depth supported by this package.
	MaxColumnDepth = math.MaxInt8

	// MaxColumnIndex is the maximum column index supported by this package.
	MaxColumnIndex = math.MaxInt8

	// MaxRepetitionLevel is the maximum repetition level supported by this package.
	MaxRepetitionLevel = math.MaxInt8

	// MaxDefinitionLevel is the maximum definition level supported by this package.
	MaxDefinitionLevel = math.MaxInt8
)
