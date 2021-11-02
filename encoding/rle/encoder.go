package rle

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"unsafe"
)

type booleanEncoder struct {
	w    io.Writer
	buf  [binary.MaxVarintLen32]byte
	runs []uint32
	bits []uint64
}

func (e *booleanEncoder) Close() error {
	if len(e.runs) > 0 {
		defer e.Reset(e.w)
		offset := 0
		length := 8 * len(e.bits)

		for _, run := range e.runs {
			length += binary.PutUvarint(e.buf[:], uint64(run<<1)|1)
		}

		binary.LittleEndian.PutUint32(e.buf[:4], uint32(length))
		if _, err := e.w.Write(e.buf[:4]); err != nil {
			return err
		}

		for _, run := range e.runs {
			n := binary.PutUvarint(e.buf[:], uint64(run<<1)|1)
			if _, err := e.w.Write(e.buf[:n]); err != nil {
				return err
			}
			r := (int(run) + 63) / 64
			u := e.bits[offset : offset+r]
			b := unsafe.Slice(*(**byte)(unsafe.Pointer(&u)), 8*len(u))
			if _, err := e.w.Write(b); err != nil {
				return err
			}
			offset += r
		}
	}
	return nil
}

func (e *booleanEncoder) Reset(w io.Writer) {
	e.w = w
	e.runs = e.runs[:0]
	e.bits = e.bits[:0]
}

func (e *booleanEncoder) EncodeBoolean(data []bool) error {
	if len(data) > math.MaxInt32 {
		return fmt.Errorf("boolean run is too long to be represented by the bitpack encoding: %d", len(data))
	}

	i := 0
	u := uint64(0)

	for _, b := range data {
		if b {
			u |= 1 << i
		}

		if i = (i + 1) % 64; i == 0 {
			e.bits = append(e.bits, u)
			u = 0
		}
	}

	if i > 0 {
		e.bits = append(e.bits, u)
	}

	e.runs = append(e.runs, uint32(len(data)))
	return nil
}

type intRun struct {
	count uint32
	width uint32
	value [12]byte
}

type intEncoder struct {
	w     io.Writer
	buf   [12 + binary.MaxVarintLen32]byte
	width uint32
	data  []byte
}

func (e *intEncoder) Close() error {
	if len(e.data) > 4 {
		defer e.Reset(e.w)
		binary.LittleEndian.PutUint32(e.data[:4], uint32(len(e.data)-4))
		_, err := e.w.Write(e.data)
		return err
	}
	return nil
}

func (e *intEncoder) Reset(w io.Writer) {
	e.w = w
	e.data = e.data[:0]
}

func (e *intEncoder) SetBitWidth(bitWidth int) {
	e.width = uint32(bitWidth+7) / 8
}

func (e *intEncoder) EncodeInt32(data []int32) error {
	for i := 0; i < len(data); {
		j := i + 1

		for j < len(data) && data[i] == data[j] {
			j++
		}

		run := intRun{
			count: uint32(j - i),
			width: coalesceUint32(e.width, 4),
		}
		binary.LittleEndian.PutUint32(run.value[:4], uint32(data[i]))
		e.encodeRun(run)
		i = j
	}
	return nil
}

func (e *intEncoder) EncodeInt64(data []int64) error {
	for i := 0; i < len(data); {
		j := i + 1

		for j < len(data) && data[i] == data[j] {
			j++
		}

		run := intRun{
			count: uint32(j - i),
			width: coalesceUint32(e.width, 8),
		}
		binary.LittleEndian.PutUint64(run.value[:8], uint64(data[i]))
		e.encodeRun(run)
		i = j
	}
	return nil
}

func (e *intEncoder) EncodeInt96(data [][12]byte) error {
	for i := 0; i < len(data); {
		j := i + 1

		for j < len(data) && data[i] == data[j] {
			j++
		}

		e.encodeRun(intRun{
			count: uint32(j - i),
			width: coalesceUint32(e.width, 12),
			value: data[i],
		})
		i = j
	}
	return nil
}

func (e *intEncoder) encodeRun(run intRun) {
	if len(e.data) == 0 {
		if cap(e.data) < 4 {
			e.data = make([]byte, 4, 1024)
		} else {
			e.data = e.data[:4]
		}
	}

	n := binary.PutUvarint(e.buf[:], uint64(run.count)<<1)

	if (cap(e.data) - len(e.data)) < (int(run.width) + n) {
		data := make([]byte, len(e.data), 2*cap(e.data))
		copy(data, e.data)
		e.data = data
	}

	e.data = append(e.data, e.buf[:n]...)
	if run.count > 0 {
		e.data = append(e.data, run.value[:run.width]...)
	}
}
