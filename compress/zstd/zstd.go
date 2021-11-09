package zstd

import (
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/segmentio/parquet/compress"
)

type Level = zstd.EncoderLevel

const (
	// SpeedFastest will choose the fastest reasonable compression.
	// This is roughly equivalent to the fastest Zstandard mode.
	SpeedFastest = zstd.SpeedFastest

	// SpeedDefault is the default "pretty fast" compression option.
	// This is roughly equivalent to the default Zstandard mode (level 3).
	SpeedDefault = zstd.SpeedDefault

	// SpeedBetterCompression will yield better compression than the default.
	// Currently it is about zstd level 7-8 with ~ 2x-3x the default CPU usage.
	// By using this, notice that CPU usage may go up in the future.
	SpeedBetterCompression = zstd.SpeedBetterCompression

	// SpeedBestCompression will choose the best available compression option.
	// This will offer the best compression no matter the CPU cost.
	SpeedBestCompression = zstd.SpeedBestCompression
)

const (
	DefaultLevel       = SpeedDefault
	DefaultConcurrency = 1
)

type Codec struct {
	Level       Level
	Concurrency int
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	z, err := zstd.NewReader(r,
		zstd.WithDecoderConcurrency(c.concurrency()),
	)
	if err != nil {
		return nil, err
	}
	return reader{z}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	z, err := zstd.NewWriter(nonNilWriter(w),
		zstd.WithEncoderConcurrency(c.concurrency()),
		zstd.WithEncoderLevel(c.level()),
		zstd.WithZeroFrames(true),
		zstd.WithEncoderCRC(false),
	)
	if err != nil {
		return nil, err
	}
	return writer{z}, nil
}

func (c *Codec) concurrency() int {
	if c.Concurrency != 0 {
		return c.Concurrency
	}
	return DefaultConcurrency
}

func (c *Codec) level() Level {
	if c.Level != 0 {
		return c.Level
	}
	return DefaultLevel
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
