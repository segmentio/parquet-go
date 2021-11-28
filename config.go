package parquet

import (
	"fmt"
	"strings"
)

const (
	defaultBufferSize = 4096
)

type ReaderConfig struct {
	PageBufferSize int
}

func (c *ReaderConfig) Apply(options ...ReaderOption) {
	for _, opt := range options {
		opt.ConfigureReader(c)
	}
}

func (c *ReaderConfig) Configure(config *ReaderConfig) {
	*config = ReaderConfig{
		PageBufferSize: coalesceInt(c.PageBufferSize, config.PageBufferSize),
	}
}

func (c *ReaderConfig) Validate() error {
	const baseName = "parquet.(*ReaderConfig)."
	return errorInvalidConfiguration(
		validatePositiveInt(baseName+"PageBufferSize", c.PageBufferSize),
	)
}

type WriterConfig struct {
	CreatedBy          string
	ColumnPageBuffers  BufferPool
	PageBufferSize     int
	DataPageVersion    int
	RowGroupTargetSize int64
}

func (c *WriterConfig) Apply(options ...WriterOption) {
	for _, opt := range options {
		opt.ConfigureWriter(c)
	}
}

func (c *WriterConfig) Configure(config *WriterConfig) {
	*config = WriterConfig{
		CreatedBy:          coalesceString(c.CreatedBy, config.CreatedBy),
		ColumnPageBuffers:  coalesceBufferPool(c.ColumnPageBuffers, config.ColumnPageBuffers),
		PageBufferSize:     coalesceInt(c.PageBufferSize, config.PageBufferSize),
		DataPageVersion:    coalesceInt(c.DataPageVersion, config.DataPageVersion),
		RowGroupTargetSize: coalesceInt64(c.RowGroupTargetSize, config.RowGroupTargetSize),
	}
}

func (c *WriterConfig) Validate() error {
	const baseName = "parquet.(*WriterConfig)."
	return errorInvalidConfiguration(
		validateNotNil(baseName+"ColumnPageBuffers", c.ColumnPageBuffers),
		validatePositiveInt(baseName+"PageBufferSize", c.PageBufferSize),
		validatePositiveInt64(baseName+"RowGroupTargetSize", c.RowGroupTargetSize),
		validateOneOfInt(baseName+"DataPageVersion", c.DataPageVersion, 1, 2),
	)
}

type ReaderOption interface {
	ConfigureReader(*ReaderConfig)
}

type WriterOption interface {
	ConfigureWriter(*WriterConfig)
}

type PageBufferSize int

func (size PageBufferSize) ConfigureReader(config *ReaderConfig) { config.PageBufferSize = int(size) }
func (size PageBufferSize) ConfigureWriter(config *WriterConfig) { config.PageBufferSize = int(size) }

func CreatedBy(createdBy string) WriterOption {
	return writerOption(func(config *WriterConfig) { config.CreatedBy = createdBy })
}

func ColumnPageBuffers(buffers BufferPool) WriterOption {
	return writerOption(func(config *WriterConfig) { config.ColumnPageBuffers = buffers })
}

func DataPageVersion(version int) WriterOption {
	return writerOption(func(config *WriterConfig) { config.DataPageVersion = version })
}

func RowGroupTargetSize(size int64) WriterOption {
	return writerOption(func(config *WriterConfig) { config.RowGroupTargetSize = size })
}

type readerOption func(*ReaderConfig)

func (opt readerOption) ConfigureReader(config *ReaderConfig) { opt(config) }

type writerOption func(*WriterConfig)

func (opt writerOption) ConfigureWriter(config *WriterConfig) { opt(config) }

func coalesceInt(i1, i2 int) int {
	if i1 != 0 {
		return i1
	}
	return i2
}

func coalesceInt64(i1, i2 int64) int64 {
	if i1 != 0 {
		return i1
	}
	return i2
}

func coalesceString(s1, s2 string) string {
	if s1 != "" {
		return s1
	}
	return s2
}

func coalesceBytes(b1, b2 []byte) []byte {
	if b1 != nil {
		return b1
	}
	return b2
}

func coalesceBufferPool(p1, p2 BufferPool) BufferPool {
	if p1 != nil {
		return p1
	}
	return p2
}

func validatePositiveInt(optionName string, optionValue int) error {
	if optionValue > 0 {
		return nil
	}
	return errorInvalidOptionValue(optionName, optionValue)
}

func validatePositiveInt64(optionName string, optionValue int64) error {
	if optionValue > 0 {
		return nil
	}
	return errorInvalidOptionValue(optionName, optionValue)
}

func validateOneOfInt(optionName string, optionValue int, supportedValues ...int) error {
	for _, value := range supportedValues {
		if value == optionValue {
			return nil
		}
	}
	return errorInvalidOptionValue(optionName, optionValue)
}

func validateNotNil(optionName string, optionValue interface{}) error {
	if optionValue != nil {
		return nil
	}
	return errorInvalidOptionValue(optionName, optionValue)
}

func errorInvalidOptionValue(optionName string, optionValue interface{}) error {
	return fmt.Errorf("invalid option value: %s: %v", optionName, optionValue)
}

func errorInvalidConfiguration(reasons ...error) error {
	var err *invalidConfiguration

	for _, reason := range reasons {
		if reason != nil {
			if err == nil {
				err = new(invalidConfiguration)
			}
			err.reasons = append(err.reasons, err)
		}
	}

	if err != nil {
		return err
	}

	return nil
}

type invalidConfiguration struct {
	reasons []error
}

func (err *invalidConfiguration) Error() string {
	errorMessage := new(strings.Builder)
	for _, reason := range err.reasons {
		errorMessage.WriteString(reason.Error())
		errorMessage.WriteString("\n")
	}
	errorString := errorMessage.String()
	if errorString != "" {
		errorString = errorString[:len(errorString)-1]
	}
	return errorString
}
