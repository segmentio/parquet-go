package parquet

import (
	"fmt"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

// Statistics represent parquet statistics held on data pages.
type Statistics struct {
	DistinctCount int
	NullCount     int
	MinValue      []byte
	MaxValue      []byte
}

func makeStatistics(stats *format.Statistics) Statistics {
	return Statistics{
		DistinctCount: int(stats.DistinctCount),
		NullCount:     int(stats.NullCount),
		MinValue:      coalesceBytes(stats.MinValue, stats.Min),
		MaxValue:      coalesceBytes(stats.MaxValue, stats.Max),
	}
}

// PageHeader is an interface implemented by parquet page headers.
type PageHeader interface {
	fmt.Stringer

	// Returns the number of values in the page (including nulls).
	NumValues() int

	// Returns the page encoding.
	Encoding() encoding.Encoding
}

// DataPageHeader is a specialization of the PageHeader interface implemented by
// data pages.
type DataPageHeader interface {
	PageHeader

	// Returns the encoding of the repetition level section.
	RepetitionLevelEncoding() encoding.Encoding

	// Returns the encoding of the definition level section.
	DefinitionLevelEncoding() encoding.Encoding

	// Returns the data page statistics.
	//
	// The MinValue and MaxValue field may hold references to internal byte
	// slices which should be treated as immutable by the application.
	Statistics() Statistics
}

// DictionaryPageHeader is an implementation of the PageHeader interface
// representing dictionary pages.
type DictionaryPageHeader struct {
	header *format.DictionaryPageHeader
}

func (dict DictionaryPageHeader) NumValues() int {
	return int(dict.header.NumValues)
}

func (dict DictionaryPageHeader) Encoding() encoding.Encoding {
	return lookupEncoding(dict.header.Encoding)
}

func (dict DictionaryPageHeader) IsSorted() bool {
	return dict.header.IsSorted
}

func (dict DictionaryPageHeader) String() string {
	return fmt.Sprintf("DICTIONARY_PAGE_HEADER{NumValues=%d,Encoding=%s,IsSorted=%t}",
		dict.header.NumValues,
		dict.header.Encoding,
		dict.header.IsSorted)
}

// DataPageHeaderV1 is an implementation of the DataPageHeader interface
// representing data pages version 1.
type DataPageHeaderV1 struct {
	header *format.DataPageHeader
}

func (v1 DataPageHeaderV1) NumValues() int {
	return int(v1.header.NumValues)
}

func (v1 DataPageHeaderV1) RepetitionLevelEncoding() encoding.Encoding {
	return lookupEncoding(v1.header.RepetitionLevelEncoding)
}

func (v1 DataPageHeaderV1) DefinitionLevelEncoding() encoding.Encoding {
	return lookupEncoding(v1.header.DefinitionLevelEncoding)
}

func (v1 DataPageHeaderV1) Encoding() encoding.Encoding {
	return lookupEncoding(v1.header.Encoding)
}

func (v1 DataPageHeaderV1) Statistics() Statistics {
	return makeStatistics(&v1.header.Statistics)
}

func (v1 DataPageHeaderV1) String() string {
	return fmt.Sprintf("DATA_PAGE_HEADER{NumValues=%d,Encoding=%s}",
		v1.header.NumValues,
		v1.header.Encoding)
}

// DataPageHeaderV2 is an implementation of the DataPageHeader interface
// representing data pages version 2.
type DataPageHeaderV2 struct {
	header *format.DataPageHeaderV2
}

func (v2 DataPageHeaderV2) NumValues() int {
	return int(v2.header.NumValues)
}

func (v2 DataPageHeaderV2) NumNulls() int {
	return int(v2.header.NumNulls)
}

func (v2 DataPageHeaderV2) NumRows() int {
	return int(v2.header.NumRows)
}

func (v2 DataPageHeaderV2) RepetitionLevelEncoding() encoding.Encoding {
	return &RLE
}

func (v2 DataPageHeaderV2) DefinitionLevelEncoding() encoding.Encoding {
	return &RLE
}

func (v2 DataPageHeaderV2) Encoding() encoding.Encoding {
	return lookupEncoding(v2.header.Encoding)
}

func (v2 DataPageHeaderV2) Statistics() Statistics {
	return makeStatistics(&v2.header.Statistics)
}

func (v2 DataPageHeaderV2) String() string {
	return fmt.Sprintf("DATA_PAGE_HEADER_V2{NumValues=%d,NumNulls=%d,NumRows=%d,Encoding=%s}",
		v2.header.NumValues,
		v2.header.NumNulls,
		v2.header.NumRows,
		v2.header.Encoding)
}

type unknownPageHeader struct {
	header *format.PageHeader
}

func (u unknownPageHeader) NumValues() int {
	return 0
}

func (u unknownPageHeader) Encoding() encoding.Encoding {
	return encoding.NotImplemented{}
}

func (u unknownPageHeader) String() string {
	return fmt.Sprintf("UNKNOWN_PAGE_HEADER{Type=%d}", u.header.Type)
}

var (
	_ PageHeader     = DictionaryPageHeader{}
	_ DataPageHeader = DataPageHeaderV1{}
	_ DataPageHeader = DataPageHeaderV2{}
	_ PageHeader     = unknownPageHeader{}
)
