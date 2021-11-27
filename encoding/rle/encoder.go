package rle

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type Encoder struct {
	encoding.NotImplementedEncoder
	writer    io.Writer
	bitWidth  uint
	buffer    [64]byte
	runLength runLengthRunEncoder
	bitPack   bitPackRunEncoder
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{writer: w}
}

func (e *Encoder) Encoding() format.Encoding {
	return format.RLE
}

func (e *Encoder) Write(b []byte) (int, error) {
	return e.writer.Write(b)
}

func (e *Encoder) WriteByte(b byte) error {
	e.buffer[0] = b
	_, err := e.Write(e.buffer[:1])
	return err
}

func (e *Encoder) WriteUvarint(u uint64) (int, error) {
	n := binary.PutUvarint(e.buffer[:], u)
	return e.Write(e.buffer[:n])
}

func (e *Encoder) BitWidth() int {
	return int(e.bitWidth)
}

func (e *Encoder) SetBitWidth(bitWidth int) {
	e.bitWidth = uint(bitWidth)
}

func (e *Encoder) Reset(w io.Writer) {
	e.writer = w
}

func (e *Encoder) EncodeBoolean(data []bool) error {
	return e.encode(bits.BoolToBytes(data), 1, 8)
}

func (e *Encoder) EncodeInt8(data []int8) error {
	return e.encode(bits.Int8ToBytes(data), uint(e.bitWidth), 8)
}

func (e *Encoder) EncodeInt16(data []int16) error {
	return e.encode(bits.Int16ToBytes(data), uint(e.bitWidth), 16)
}

func (e *Encoder) EncodeInt32(data []int32) error {
	return e.encode(bits.Int32ToBytes(data), uint(e.bitWidth), 32)
}

func (e *Encoder) EncodeInt64(data []int64) error {
	return e.encode(bits.Int64ToBytes(data), uint(e.bitWidth), 64)
}

func (e *Encoder) encode(data []byte, dstWidth, srcWidth uint) error {
	wordSize := uint(bits.ByteCount(srcWidth))
	if dstWidth == 0 {
		dstWidth = srcWidth
	}

	eightWordSize := 8 * wordSize
	i := uint(0)
	n := uint(len(data))
	pattern := e.buffer[:eightWordSize]

	for i < n {
		j := i
		k := i + eightWordSize
		fill(pattern, data[i:i+wordSize])

		for k <= n && !bytes.Equal(data[j:k], pattern) {
			j += eightWordSize
			k += eightWordSize
		}

		if i < j {
			if err := e.encodeBitPack(data[i:j], dstWidth, srcWidth); err != nil {
				return err
			}
		} else {
			if k <= n {
				j += eightWordSize
				k += eightWordSize
			}

			for k <= n && bytes.Equal(data[j:k], pattern) {
				j += eightWordSize
				k += eightWordSize
			}

			k = j + wordSize
			for k <= n && bytes.Equal(data[j:k], pattern[:wordSize]) {
				j += wordSize
				k += wordSize
			}

			if i < j {
				if err := e.encodeRunLength(data[i:j], dstWidth, srcWidth); err != nil {
					return err
				}
			}
		}

		i = j
	}

	return nil
}

func (e *Encoder) encodeBitPack(run []byte, dstWidth, srcWidth uint) error {
	if _, err := e.WriteUvarint((uint64(len(run)/(8*bits.ByteCount(srcWidth))) << 1) | 1); err != nil {
		return err
	}
	return e.bitPack.encode(e.writer, dstWidth, run, srcWidth)
}

func (e *Encoder) encodeRunLength(run []byte, dstWidth, srcWidth uint) error {
	if _, err := e.WriteUvarint(uint64(len(run)/bits.ByteCount(srcWidth)) << 1); err != nil {
		return err
	}
	return e.runLength.encode(e.writer, dstWidth, run, srcWidth)
}

func fill(b, v []byte) int {
	n := copy(b, v)

	for i := n; i < len(b); {
		n += copy(b[i:], b[:i])
		i *= 2
	}

	return n
}

var (
	_ io.ByteWriter = (*Encoder)(nil)
	_ io.Writer     = (*Encoder)(nil)
)
