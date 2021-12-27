package delta

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type LengthByteArrayDecoder struct {
	encoding.NotSupportedDecoder
	binpack BinaryPackedDecoder
	lengths []int32
	index   int
}

func NewLengthByteArrayDecoder(r io.Reader) *LengthByteArrayDecoder {
	d := &LengthByteArrayDecoder{}
	d.Reset(r)
	return d
}

func (d *LengthByteArrayDecoder) Reset(r io.Reader) {
	d.binpack.Reset(r)
	d.lengths = d.lengths[:0]
	d.index = -1
}

func (d *LengthByteArrayDecoder) Encoding() format.Encoding {
	return format.DeltaLengthByteArray
}

func (d *LengthByteArrayDecoder) DecodeByteArray(data *encoding.ByteArrayList) (n int, err error) {
	if d.index < 0 {
		d.lengths, err = d.decodeLengths(d.lengths[:0])
		if err != nil {
			return 0, err
		}
		d.index = 0
	}

	n = data.Len()
	for data.Len() < data.Cap() && d.index < len(d.lengths) {
		value := data.PushSize(int(d.lengths[d.index]))
		_, err := io.ReadFull(d.binpack.reader, value)
		if err != nil {
			err = fmt.Errorf("DELTA_LENGTH_BYTE_ARRAY: decoding byte array at index %d/%d: %w", d.index, len(d.lengths), dontExpectEOF(err))
			break
		}
		d.index++
	}

	if d.index == len(d.lengths) {
		err = io.EOF
	}

	return data.Len() - n, err
}

func (d *LengthByteArrayDecoder) decodeLengths(lengths []int32) ([]int32, error) {
	for {
		if len(lengths) == cap(lengths) {
			if cap(lengths) == 0 {
				lengths = make([]int32, 0, blockSize32)
			} else {
				newLengths := make([]int32, len(lengths), 2*cap(lengths))
				copy(newLengths, lengths)
				lengths = newLengths
			}
		}

		n, err := d.binpack.DecodeInt32(lengths[len(lengths):cap(lengths)])
		lengths = lengths[:len(lengths)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return lengths, err
		}
	}
}
