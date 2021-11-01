module github.com/segmentio/parquet

go 1.17

require (
	github.com/andybalholm/brotli v1.0.3
	github.com/klauspost/compress v1.13.6
	github.com/pierrec/lz4/v4 v4.1.9
	github.com/segmentio/encoding v0.2.24-0.20211101013219-efdf202c8f02
)

replace github.com/segmentio/encoding => ../encoding
