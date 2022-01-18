// Package lz4 implements the LZ4_RAW parquet compression codec.
package lz4

import (
	"io"

	"github.com/pierrec/lz4/v4"
	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/format"
)

type BlockSize = lz4.BlockSize

const (
	Block64Kb  = lz4.Block64Kb
	Block256Kb = lz4.Block256Kb
	Block1Mb   = lz4.Block1Mb
	Block4Mb   = lz4.Block4Mb
)

type Level = lz4.CompressionLevel

const (
	Fast   = lz4.Fast
	Level1 = lz4.Level1
	Level2 = lz4.Level2
	Level3 = lz4.Level3
	Level4 = lz4.Level4
	Level5 = lz4.Level5
	Level6 = lz4.Level6
	Level7 = lz4.Level7
	Level8 = lz4.Level8
	Level9 = lz4.Level9
)

const (
	DefaultBlockSize   = Block4Mb
	DefaultLevel       = Fast
	DefaultConcurrency = 1
)

type Codec struct {
	BlockSize   BlockSize
	Level       Level
	Concurrency int
}

func (c *Codec) String() string {
	return "LZ4_RAW"
}

func (c *Codec) CompressionCodec() format.CompressionCodec {
	return format.Lz4Raw
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	lzr := lz4.NewReader(r)
	err := lzr.Apply(
		lz4.ConcurrencyOption(c.concurrency()),
	)
	if err != nil {
		return nil, err
	}
	return reader{lzr}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	lzw := lz4.NewWriter(w)
	err := lzw.Apply(
		lz4.BlockChecksumOption(false),
		lz4.ChecksumOption(false),
		lz4.BlockSizeOption(c.blockSize()),
		lz4.CompressionLevelOption(c.level()),
		lz4.ConcurrencyOption(c.concurrency()),
	)
	if err != nil {
		return nil, err
	}
	return writer{lzw}, nil
}

func (c *Codec) concurrency() int {
	if c.Concurrency != 0 {
		return c.Concurrency
	}
	return DefaultConcurrency
}

func (c *Codec) blockSize() BlockSize {
	if c.BlockSize != 0 {
		return c.BlockSize
	}
	return DefaultBlockSize
}

func (c *Codec) level() Level {
	// zero == Fast
	return c.Level
}

type reader struct{ *lz4.Reader }

func (r reader) Close() error             { return nil }
func (r reader) Reset(rr io.Reader) error { r.Reader.Reset(rr); return nil }

type writer struct{ *lz4.Writer }

func (w writer) Reset(ww io.Writer) error { w.Writer.Reset(ww); return nil }
