package rle

import (
	"encoding/binary"
	"io"
)

type encoder struct {
	w         io.Writer
	buf       [binary.MaxVarintLen32]byte
	data      []byte
	width     uint32
	bitWidth  uint32
	bitOffset uint32
}

func newEncoder(w io.Writer) *encoder {
	return &encoder{
		w:    w,
		data: make([]byte, 4, 1024),
	}
}

func (e *encoder) SetBitWidth(bitWidth int) {
	e.width = uint32(bitWidth+7) / 8
	e.bitWidth = uint32(bitWidth)
}

func (e *encoder) Close() error {
	if len(e.data) > 4 {
		defer e.Reset(e.w)
		binary.LittleEndian.PutUint32(e.data[:4], uint32(len(e.data)-4))
		_, err := e.w.Write(e.data)
		return err
	}
	return nil
}

func (e *encoder) Reset(w io.Writer) {
	e.w = w
	e.data = e.data[:4]
}

func (e *encoder) EncodeBoolean(data []bool) error {
	if len(data) >= 8 {
		bits := data[:(len(data)/8)*8]
		e.encodeBooleanBitPack(bits)
		data = data[len(bits):]
	}
	if len(data) > 0 {
		e.encodeBooleanRunLength(data)
	}
	return nil
}

func (e *encoder) encodeBooleanBitPack(data []bool) {
	e.encodeBitsLen(len(data))

	for i := 0; i < len(data); i += 8 {
		bits := byte(0)

		if data[i+7] {
			bits |= 1 << 7
		}
		if data[i+6] {
			bits |= 1 << 6
		}
		if data[i+5] {
			bits |= 1 << 5
		}
		if data[i+4] {
			bits |= 1 << 4
		}
		if data[i+3] {
			bits |= 1 << 3
		}
		if data[i+2] {
			bits |= 1 << 2
		}
		if data[i+1] {
			bits |= 1 << 1
		}
		if data[i+0] {
			bits |= 1 << 0
		}

		e.encodeBits8(bits)
	}
}

func (e *encoder) encodeBooleanRunLength(data []bool) {
	forEachRun(len(data),
		func(i, j int) bool { return data[i] == data[j] },
		func(i, j int) {
			run := run{
				count: uint32(j - i),
				width: 1,
			}
			if data[i] {
				run.value[0] = 1
			}
			e.encodeRun(run)
		},
	)
}

func (e *encoder) EncodeInt32(data []int32) error {
	bitWidth := coalesceUint32(e.bitWidth, 32)

	if len(data) >= 8 && preferBitPack(len(data), bitWidth, func(i, j int) bool {
		return data[i] == data[j]
	}) {
		n := (len(data) / 8) * 8
		e.encodeIntBitPack(n, bitWidth, func(i int) uint64 { return uint64(data[i]) })
		data = data[n:]
	}

	if len(data) > 0 {
		e.encodeInt32RunLength(data)
	}
	return nil
}

func (e *encoder) encodeInt32RunLength(data []int32) {
	forEachRun(len(data),
		func(i, j int) bool { return data[i] == data[j] },
		func(i, j int) {
			run := run{
				count: uint32(j - i),
				width: coalesceUint32(e.width, 4),
			}
			binary.LittleEndian.PutUint32(run.value[:4], uint32(data[i]))
			e.encodeRun(run)
		},
	)
}

func (e *encoder) EncodeInt64(data []int64) error {
	e.encodeInt64RunLength(data)
	return nil
}

func (e *encoder) encodeInt64RunLength(data []int64) {
	forEachRun(len(data),
		func(i, j int) bool { return data[i] == data[j] },
		func(i, j int) {
			run := run{
				count: uint32(j - i),
				width: coalesceUint32(e.width, 8),
			}
			binary.LittleEndian.PutUint64(run.value[:8], uint64(data[i]))
			e.encodeRun(run)
		},
	)
}

func (e *encoder) EncodeInt96(data [][12]byte) error {
	e.encodeInt96RunLength(data)
	return nil
}

func (e *encoder) encodeInt96RunLength(data [][12]byte) {
	forEachRun(len(data),
		func(i, j int) bool { return data[i] == data[j] },
		func(i, j int) {
			e.encodeRun(run{
				count: uint32(j - i),
				width: coalesceUint32(e.width, 12),
				value: data[i],
			})
		},
	)
}

func (e *encoder) encodeIntBitPack(n int, bitWidth uint32, at func(int) uint64) {
	e.encodeBitsLen(n)
	for i := 0; i < n; i++ {
		e.encodeBits(uint(bitWidth), at(i))
	}
}

type run struct {
	count uint32
	width uint32
	value [12]byte
}

func (e *encoder) encodeBitsLen(length int) {
	n := binary.PutUvarint(e.buf[:], (uint64(length/8)<<1)|1)
	e.data = append(e.data, e.buf[:n]...)
	e.bitOffset = 0
}

func (e *encoder) encodeBits8(bits uint8) {
	e.data = append(e.data, bits)
	e.bitOffset += 8
}

func (e *encoder) encodeBits(count uint, bits uint64) {
	offset := uint(e.bitOffset % 8)
	e.bitOffset += uint32(count)

	if offset != 0 {
		e.data[len(e.data)-1] |= byte(bits) << offset
		bits >>= offset
		count -= offset
	}

	if count != 0 {
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], bits)
		e.data = append(e.data, tail[:((count+7)/8)]...)
	}
}

func (e *encoder) encodeRun(run run) {
	n := binary.PutUvarint(e.buf[:], uint64(run.count)<<1)
	e.data = append(e.data, e.buf[:n]...)
	if run.count > 0 {
		e.data = append(e.data, run.value[:run.width]...)
	}
}

func forEachRun(n int, eq func(i, j int) bool, do func(i, j int)) {
	for i := 0; i < n; {
		j := i + 1
		for j < n && eq(i, j) {
			j++
		}
		do(i, j)
		i = j
	}
}

func preferBitPack(n int, bitWidth uint32, eq func(i, j int) bool) bool {
	sizeOfItems := int64(bitWidth)
	numberOfItems := int64(n)
	numberOfRuns := int64(0)
	numberOfItemsInRuns := int64(0)

	forEachRun(n, eq, func(i, j int) {
		numberOfRuns++
		numberOfItemsInRuns += int64(j - i)
	})

	estimatedSizeOfBitPack := numberOfItems * sizeOfItems
	estimatedSizeOfRunLength := (numberOfRuns * (8 + sizeOfItems)) + ((numberOfItems - numberOfItemsInRuns) * sizeOfItems)
	return estimatedSizeOfBitPack < estimatedSizeOfRunLength
}
