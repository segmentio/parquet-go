package delta

import (
	"encoding/binary"
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
	d := &LengthByteArrayDecoder{lengths: make([]int32, defaultBufferSize/4)}
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

func (d *LengthByteArrayDecoder) DecodeByteArray(data []byte) (int, error) {
	if d.index < 0 {
		if err := d.decodeLengths(); err != nil {
			return 0, err
		}
	}
	if len(data) == 0 {
		return 0, nil
	}
	if len(data) < 4 {
		return 0, encoding.ErrBufferTooShort
	}
	if d.index == len(d.lengths) {
		return 0, io.EOF
	}

	decoded := 0
	for d.index < len(d.lengths) && len(data) >= 4 {
		n := int(d.lengths[d.index])
		binary.LittleEndian.PutUint32(data, uint32(n))
		data = data[4:]

		if len(data) < n {
			if decoded == 0 {
				return 0, encoding.ErrValueTooLarge
			}
			break
		}

		if err := d.readFull(data[:n]); err != nil {
			return decoded, fmt.Errorf("DELTA_LENGTH_BYTE_ARRAY: decoding byte array at index %d/%d: %w", d.index, len(d.lengths), err)
		}

		data = data[n:]
		decoded++
		d.index++
	}

	return decoded, nil
}

func (d *LengthByteArrayDecoder) decodeLengths() (err error) {
	d.lengths, err = appendDecodeInt32(&d.binpack, d.lengths[:0])
	if err != nil {
		return err
	}
	d.index = 0
	return nil
}

func (d *LengthByteArrayDecoder) readFull(b []byte) error {
	_, err := io.ReadFull(d.binpack.reader, b)
	return dontExpectEOF(err)
}
