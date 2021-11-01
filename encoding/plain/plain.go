package plain

import (
	"io"

	"github.com/segmentio/parquet/encoding"
)

type Encoding struct{}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return &decoder{r: r}
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return &encoder{w: w}
}
