package rle

import (
	"encoding/binary"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type Encoder struct {
	encoding.NotImplementedEncoder
	writer   io.Writer
	buffer   [binary.MaxVarintLen32]byte
	bitWidth uint
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
	return e.encode(bits.BoolToBytes(data), 1, 8, equalInt8)
}

func (e *Encoder) EncodeInt8(data []int8) error {
	return e.encode(bits.Int8ToBytes(data), uint(e.bitWidth), 8, equalInt8)
}

func (e *Encoder) EncodeInt16(data []int16) error {
	return e.encode(bits.Int16ToBytes(data), uint(e.bitWidth), 16, equalInt16)
}

func (e *Encoder) EncodeInt32(data []int32) error {
	return e.encode(bits.Int32ToBytes(data), uint(e.bitWidth), 32, equalInt32)
}

func (e *Encoder) EncodeInt64(data []int64) error {
	return e.encode(bits.Int64ToBytes(data), uint(e.bitWidth), 64, equalInt64)
}

func (e *Encoder) EncodeInt96(data [][12]byte) error {
	return e.encode(bits.Int96ToBytes(data), uint(e.bitWidth), 96, equalInt96)
}

func (e *Encoder) encode(data []byte, dstWidth, srcWidth uint, eq func(a, b []byte) bool) error {
	wordSize := bits.ByteCount(srcWidth)
	if dstWidth == 0 {
		dstWidth = srcWidth
	}

	// Bit-pack encoding is done in chunks of 8 values.
	if count := len(data) / wordSize; count >= 8 && preferBitPack(data, dstWidth, srcWidth, eq) {
		n := (count / 8) * 8
		i := n * wordSize
		e.encodeBitPack(n, data[:i], dstWidth, srcWidth)
		data = data[i:]
	}

	if len(data) > 0 {
		forEachRun(data, wordSize, eq, func(run []byte) {
			e.encodeRunLength(len(run)/wordSize, run[:wordSize])
		})
	}

	return nil
}

func (e *Encoder) encodeBitPack(count int, data []byte, dstWidth, srcWidth uint) (int, error) {
	if _, err := e.WriteUvarint((uint64(count/8) << 1) | 1); err != nil {
		return 0, err
	}

	/*
		wordSize := bits.ByteCount(srcWidth)
		offset := len(e.data)
		length := bits.ByteCount(uint(len(data)/wordSize) * dstWidth)

		if (cap(e.data) - offset) >= length {
			e.data = e.data[:offset+length]
		} else {
			newCap := 2 * cap(e.data)
			if newCap == 0 {
				newCap = encoding.DefaultBufferSize
			}
			for (newCap - offset) < length {
				newCap *= 2
			}
			newData := make([]byte, offset+length, newCap)
			copy(newData, e.data)
			e.data = newData
		}

		return bits.Pack(e.data[offset:], dstWidth, data, srcWidth)
	*/
	return 0, nil
}

func (e *Encoder) encodeRunLength(count int, data []byte) error {
	if _, err := e.WriteUvarint(uint64(count) << 1); err != nil {
		return err
	}
	_, err := e.Write(data)
	return err
}

func preferBitPack(data []byte, dstWidth, srcWidth uint, eq func(a, b []byte) bool) bool {
	if dstWidth == 1 {
		return true
	}

	wordSize := bits.ByteCount(srcWidth)
	sizeOfItems := int64(dstWidth)
	numberOfItems := int64(len(data) / wordSize)
	numberOfRuns := int64(0)
	numberOfItemsInRuns := int64(0)

	forEachRun(data, wordSize, eq, func(run []byte) {
		numberOfRuns++
		numberOfItemsInRuns += int64(len(run) / wordSize)
	})

	estimatedSizeOfBitPack := numberOfItems * sizeOfItems
	estimatedSizeOfRunLength := (numberOfRuns * (8 + sizeOfItems)) + ((numberOfItems - numberOfItemsInRuns) * sizeOfItems)
	return estimatedSizeOfBitPack < estimatedSizeOfRunLength
}

func forEachRun(data []byte, wordSize int, eq func(a, b []byte) bool, do func([]byte)) {
	for i := 0; i < len(data); {
		j := i + wordSize
		a := data[i:j]

		for j < len(data) && eq(a, data[j:j+wordSize]) {
			j += wordSize
		}

		do(data[i:j])
		i = j
	}
}

func equalInt8(a, b []byte) bool  { return a[0] == b[0] }
func equalInt16(a, b []byte) bool { return *(*[2]byte)(a) == *(*[2]byte)(b) }
func equalInt32(a, b []byte) bool { return *(*[4]byte)(a) == *(*[4]byte)(b) }
func equalInt64(a, b []byte) bool { return *(*[8]byte)(a) == *(*[8]byte)(b) }
func equalInt96(a, b []byte) bool { return *(*[12]byte)(a) == *(*[12]byte)(b) }

var (
	_ io.ByteWriter = (*Encoder)(nil)
	_ io.Writer     = (*Encoder)(nil)
)
