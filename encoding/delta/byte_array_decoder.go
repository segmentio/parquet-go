package delta

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type ByteArrayDecoder struct {
	encoding.NotSupportedDecoder
	deltas   BinaryPackedDecoder
	arrays   LengthByteArrayDecoder
	previous []byte
	prefixes []int32
}

func NewByteArrayDecoder(r io.Reader) *ByteArrayDecoder {
	d := &ByteArrayDecoder{prefixes: make([]int32, defaultBufferSize/4)}
	d.Reset(r)
	return d
}

func (d *ByteArrayDecoder) Reset(r io.Reader) {
	if r != nil {
		br, _ := r.(*bufio.Reader)
		if br == nil {
			r = bufio.NewReaderSize(r, defaultBufferSize)
		}
	}
	d.deltas.Reset(r)
	d.arrays.Reset(r)
	d.previous = d.previous[:0]
	d.prefixes = d.prefixes[:0]
}

func (d *ByteArrayDecoder) Encoding() format.Encoding {
	return format.DeltaByteArray
}

func (d *ByteArrayDecoder) DecodeByteArray(data []byte) (int, error) {
	if d.arrays.index < 0 {
		if err := d.decodePrefixes(); err != nil {
			return 0, err
		}
		if err := d.arrays.decodeLengths(); err != nil {
			return 0, err
		}
	}
	if len(data) == 0 {
		return 0, nil
	}
	if len(data) < 4 {
		return 0, encoding.ErrBufferTooShort
	}
	if d.arrays.index == len(d.arrays.lengths) {
		return 0, io.EOF
	}

	decoded := 0
	for d.arrays.index < len(d.arrays.lengths) && len(data) >= 4 {
		prefixLength := uint(len(d.previous))
		suffixLength := uint(d.arrays.lengths[d.arrays.index])
		length := prefixLength + suffixLength
		binary.LittleEndian.PutUint32(data, uint32(length))
		data = data[4:]

		if uint(len(data)) < length {
			if decoded == 0 {
				return 0, encoding.ErrValueTooLarge
			}
			break
		}

		copy(data[:prefixLength], d.previous[:prefixLength])
		if err := d.arrays.readFull(data[prefixLength:length]); err != nil {
			return decoded, fmt.Errorf("DELTA_BYTE_ARRAY: decoding byte array at index %d/%d: %w", d.arrays.index, len(d.arrays.lengths), err)
		}

		if i := d.arrays.index + 1; i < len(d.prefixes) {
			j := uint(d.prefixes[i])
			k := uint(len(data))
			if j > k {
				return decoded, fmt.Errorf("DELTA_BYTE_ARRAY: next prefix is longer than the last decoded byte array (%d>%d)", j, k)
			}
			d.previous = append(d.previous[:0], data[:j]...)
		}

		data = data[length:]
		decoded++
		d.arrays.index++
	}

	return decoded, nil
}

func (d *ByteArrayDecoder) decodePrefixes() (err error) {
	d.prefixes, err = appendDecodeInt32(&d.deltas, d.prefixes[:0])
	return err
}
