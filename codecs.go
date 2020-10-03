package parquet

import "github.com/golang/snappy"

type compressionCodec interface {
	Decode(dst []byte, src []byte) error
}

type plainCodec struct{}

func (c *plainCodec) Decode(dst []byte, src []byte) error {
	copy(dst, src)
	return nil
}

type snappyCodec struct{}

func (s *snappyCodec) Decode(dst []byte, src []byte) error {
	_, err := snappy.Decode(dst, src)
	// TODO: should check that the returned slice is the same as dst (no realloc /
	// shrink)
	return err
}
