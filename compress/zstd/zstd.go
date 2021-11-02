package zstd

import (
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/segmentio/parquet/compress"
)

type Codec struct {
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	z, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return reader{z}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	z, err := zstd.NewWriter(nonNilWriter(w),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithZeroFrames(true),
		zstd.WithEncoderCRC(false),
	)
	if err != nil {
		return nil, err
	}
	return writer{z}, nil
}

type reader struct{ *zstd.Decoder }

func (r reader) Close() error { r.Decoder.Close(); return nil }

type writer struct{ *zstd.Encoder }

func (w writer) Close() error             { w.Encoder.Close(); return nil }
func (w writer) Reset(ww io.Writer) error { w.Encoder.Reset(nonNilWriter(ww)); return nil }

func nonNilWriter(w io.Writer) io.Writer {
	if w == nil {
		w = io.Discard
	}
	return w
}
