package bytestreamsplit

import (
	"io"
	"io/ioutil"
	"math"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type Decoder struct {
	encoding.NotSupportedDecoder
	reader io.Reader
	buffer []byte
	offset int
}

func NewDecorder(r io.Reader) *Decoder {
	return &Decoder{reader: r}
}

func (d *Decoder) Encoding() format.Encoding {
	return format.ByteStreamSplit
}

func (d *Decoder) Reset(r io.Reader) {
	d.reader = r
	d.offset = 0
	d.buffer = make([]byte, 0)
}

func (d *Decoder) DecodeFloat(data []float32) (int, error) {
	if err := d.read(); err != nil {
		return 0, err
	}

	return d.decode32(data)
}

func (d *Decoder) DecodeDouble(data []float64) (int, error) {
	if err := d.read(); err != nil {
		return 0, err
	}

	return d.decode64(data)
}

func (d *Decoder) read() error {
	var err error

	if len(d.buffer) == 0 {
		d.buffer, err = ioutil.ReadAll(d.reader)
	}

	return err
}

func (d *Decoder) decode32(data []float32) (int, error) {
	if d.offset*4 >= len(d.buffer) {
		return 0, io.EOF
	}

	length := len(data)

	padding := len(d.buffer) / 4 // float32 size

	for i := 0; i < length; i++ {
		data[i] = d.float32frombits(i+d.offset, padding)
	}

	d.offset += length

	return length, nil
}

func (d *Decoder) float32frombits(idx, padding int) float32 {
	return math.Float32frombits(
		uint32(d.buffer[idx]) |
			uint32(d.buffer[idx+padding])<<8 |
			uint32(d.buffer[idx+padding*2])<<16 |
			uint32(d.buffer[idx+padding*3])<<24)
}

func (d *Decoder) decode64(data []float64) (int, error) {
	if d.offset*8 >= len(d.buffer) {
		return 0, io.EOF
	}

	length := len(data)

	padding := len(d.buffer) / 8

	for i := 0; i < length; i++ {
		data[i] = d.float64frombits(i+d.offset, padding)
	}

	d.offset += length

	return length, nil
}

func (d *Decoder) float64frombits(idx, padding int) float64 {
	return math.Float64frombits(
		uint64(d.buffer[idx]) |
			uint64(d.buffer[idx+padding])<<8 |
			uint64(d.buffer[idx+padding*2])<<16 |
			uint64(d.buffer[idx+padding*3])<<24 |
			uint64(d.buffer[idx+padding*4])<<32 |
			uint64(d.buffer[idx+padding*5])<<40 |
			uint64(d.buffer[idx+padding*6])<<48 |
			uint64(d.buffer[idx+padding*7])<<56)
}
