package delta

import (
	"fmt"
	"io"
	"math"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/format"
)

type LengthByteArrayEncoder struct {
	encoding.NotSupportedEncoder
	binpack BinaryPackedEncoder
	lengths []int32
}

func NewLengthByteArrayEncoder(w io.Writer) *LengthByteArrayEncoder {
	e := &LengthByteArrayEncoder{lengths: make([]int32, 512)}
	e.Reset(w)
	return e
}

func (e *LengthByteArrayEncoder) Reset(w io.Writer) {
	e.binpack.Reset(w)
}

func (e *LengthByteArrayEncoder) Encoding() format.Encoding {
	return format.DeltaLengthByteArray
}

func (e *LengthByteArrayEncoder) EncodeByteArray(data []byte) error {
	e.lengths = e.lengths[:0]

	_, err := plain.ScanByteArrayList(data, plain.All, func(value []byte) error {
		if len(value) > math.MaxInt32 {
			return fmt.Errorf("DELTA_LENGTH_BYTE_ARRAY: byte array of length %d is too large to be encoded", len(value))
		}
		e.lengths = append(e.lengths, int32(len(value)))
		return nil
	})
	if err != nil {
		return err
	}

	if err := e.binpack.EncodeInt32(e.lengths); err != nil {
		return err
	}

	_, err = plain.ScanByteArrayList(data, plain.All, func(value []byte) error {
		_, werr := e.binpack.writer.Write(value)
		return werr
	})
	return err
}
