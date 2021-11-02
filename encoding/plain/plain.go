package plain

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/rle"
)

type Encoding struct{ rle.Encoding }

func (e *Encoding) NewInt32Decoder(r io.Reader) encoding.Int32Decoder {
	return &primitiveDecoder{r: r}
}

func (e *Encoding) NewInt32Encoder(w io.Writer) encoding.Int32Encoder {
	return &primitiveEncoder{w: w}
}

func (e *Encoding) NewInt64Decoder(r io.Reader) encoding.Int64Decoder {
	return &primitiveDecoder{r: r}
}

func (e *Encoding) NewInt64Encoder(w io.Writer) encoding.Int64Encoder {
	return &primitiveEncoder{w: w}
}

func (e *Encoding) NewInt96Decoder(r io.Reader) encoding.Int96Decoder {
	return &primitiveDecoder{r: r}
}

func (e *Encoding) NewInt96Encoder(w io.Writer) encoding.Int96Encoder {
	return &primitiveEncoder{w: w}
}

func (e *Encoding) NewFloatDecoder(r io.Reader) encoding.FloatDecoder {
	return &primitiveDecoder{r: r}
}

func (e *Encoding) NewFloatEncoder(w io.Writer) encoding.FloatEncoder {
	return &primitiveEncoder{w: w}
}

func (e *Encoding) NewDoubleDecoder(r io.Reader) encoding.DoubleDecoder {
	return &primitiveDecoder{r: r}
}

func (e *Encoding) NewDoubleEncoder(w io.Writer) encoding.DoubleEncoder {
	return &primitiveEncoder{w: w}
}

func (e *Encoding) NewByteArrayDecoder(r io.Reader) encoding.ByteArrayDecoder {
	return &byteArrayDecoder{r: r}
}

func (e *Encoding) NewByteArrayEncoder(w io.Writer) encoding.ByteArrayEncoder {
	return &byteArrayEncoder{w: w}
}

func (e *Encoding) NewFixedLenByteArrayDecoder(r io.Reader) encoding.FixedLenByteArrayDecoder {
	return &primitiveDecoder{r: r}
}

func (e *Encoding) NewFixedLenByteArrayEncoder(w io.Writer) encoding.FixedLenByteArrayEncoder {
	return &primitiveEncoder{w: w}
}
