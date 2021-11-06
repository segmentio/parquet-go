package rle

import (
	"encoding/binary"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
)

const (
	defaultBufferSize = 1024
)

type Encoder struct {
	encoding.NotImplementedEncoder
	w        io.Writer
	buffer   [binary.MaxVarintLen32]byte
	data     []byte
	bitWidth uint
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w:    w,
		data: make([]byte, 4, defaultBufferSize),
	}
}

func (e *Encoder) SetBitWidth(bitWidth int) {
	e.bitWidth = uint(bitWidth)
}

func (e *Encoder) Close() error {
	if len(e.data) > 4 {
		defer e.Reset(e.w)
		binary.LittleEndian.PutUint32(e.data[:4], uint32(len(e.data)-4))
		_, err := e.w.Write(e.data)
		return err
	}
	return nil
}

func (e *Encoder) Reset(w io.Writer) {
	e.w = w

	if cap(e.data) == 0 {
		e.data = make([]byte, 4, defaultBufferSize)
	} else {
		e.data = e.data[:4]
		*(*[4]byte)(e.data) = [4]byte{}
	}
}

func (e *Encoder) EncodeBoolean(data []bool) error {
	return e.encode(bits.BoolToBytes(data), 1, 8, equalBool)
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

func (e *Encoder) encodeBitPack(count int, data []byte, dstWidth, srcWidth uint) int {
	n := binary.PutUvarint(e.buffer[:], (uint64(count/8)<<1)|1)
	e.data = append(e.data, e.buffer[:n]...)

	wordSize := bits.ByteCount(srcWidth)
	offset := len(e.data)
	length := bits.ByteCount(uint(len(data)/wordSize) * dstWidth)

	if (cap(e.data) - offset) >= length {
		e.data = e.data[:offset+length]
	} else {
		newCap := 2 * cap(e.data)
		for (newCap - offset) < length {
			newCap *= 2
		}
		newData := make([]byte, offset+length, newCap)
		copy(newData, e.data)
		e.data = newData
	}

	return bits.Pack(e.data[offset:], dstWidth, data, srcWidth)
}

func (e *Encoder) encodeRunLength(count int, data []byte) {
	n := binary.PutUvarint(e.buffer[:], uint64(count)<<1)
	e.data = append(e.data, e.buffer[:n]...)

	if count > 0 {
		e.data = append(e.data, data...)
	}
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

func equalBool(a, b []byte) bool {
	return a[0] == b[0]
}

func equalInt32(a, b []byte) bool {
	return *(*[4]byte)(a) == *(*[4]byte)(b)
}

func equalInt64(a, b []byte) bool {
	return *(*[8]byte)(a) == *(*[8]byte)(b)
}

func equalInt96(a, b []byte) bool {
	return *(*[12]byte)(a) == *(*[12]byte)(b)
}
