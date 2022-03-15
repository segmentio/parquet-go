//go:build !go1.18

package parquet

import (
	"bytes"

	"github.com/segmentio/parquet-go/encoding/plain"
)

type booleanPageIndex struct{ page *booleanPage }

func (index booleanPageIndex) NumPages() int       { return 1 }
func (index booleanPageIndex) NullCount(int) int64 { return 0 }
func (index booleanPageIndex) NullPage(int) bool   { return false }
func (index booleanPageIndex) MinValue(int) []byte { return plain.Boolean(index.page.min()) }
func (index booleanPageIndex) MaxValue(int) []byte { return plain.Boolean(index.page.max()) }
func (index booleanPageIndex) IsAscending() bool   { return compareBool(index.page.bounds()) < 0 }
func (index booleanPageIndex) IsDescending() bool  { return compareBool(index.page.bounds()) > 0 }

type int32PageIndex struct{ page *int32Page }

func (index int32PageIndex) NumPages() int       { return 1 }
func (index int32PageIndex) NullCount(int) int64 { return 0 }
func (index int32PageIndex) NullPage(int) bool   { return false }
func (index int32PageIndex) MinValue(int) []byte { return plain.Int32(index.page.min()) }
func (index int32PageIndex) MaxValue(int) []byte { return plain.Int32(index.page.max()) }
func (index int32PageIndex) IsAscending() bool   { return compareInt32(index.page.bounds()) < 0 }
func (index int32PageIndex) IsDescending() bool  { return compareInt32(index.page.bounds()) > 0 }

type int64PageIndex struct{ page *int64Page }

func (index int64PageIndex) NumPages() int       { return 1 }
func (index int64PageIndex) NullCount(int) int64 { return 0 }
func (index int64PageIndex) NullPage(int) bool   { return false }
func (index int64PageIndex) MinValue(int) []byte { return plain.Int64(index.page.min()) }
func (index int64PageIndex) MaxValue(int) []byte { return plain.Int64(index.page.max()) }
func (index int64PageIndex) IsAscending() bool   { return compareInt64(index.page.bounds()) < 0 }
func (index int64PageIndex) IsDescending() bool  { return compareInt64(index.page.bounds()) > 0 }

type int96PageIndex struct{ page *int96Page }

func (index int96PageIndex) NumPages() int       { return 1 }
func (index int96PageIndex) NullCount(int) int64 { return 0 }
func (index int96PageIndex) NullPage(int) bool   { return false }
func (index int96PageIndex) MinValue(int) []byte { return plain.Int96(index.page.min()) }
func (index int96PageIndex) MaxValue(int) []byte { return plain.Int96(index.page.max()) }
func (index int96PageIndex) IsAscending() bool   { return compareInt96(index.page.bounds()) < 0 }
func (index int96PageIndex) IsDescending() bool  { return compareInt96(index.page.bounds()) > 0 }

type floatPageIndex struct{ page *floatPage }

func (index floatPageIndex) NumPages() int       { return 1 }
func (index floatPageIndex) NullCount(int) int64 { return 0 }
func (index floatPageIndex) NullPage(int) bool   { return false }
func (index floatPageIndex) MinValue(int) []byte { return plain.Float(index.page.min()) }
func (index floatPageIndex) MaxValue(int) []byte { return plain.Float(index.page.max()) }
func (index floatPageIndex) IsAscending() bool   { return compareFloat32(index.page.bounds()) < 0 }
func (index floatPageIndex) IsDescending() bool  { return compareFloat32(index.page.bounds()) > 0 }

type doublePageIndex struct{ page *doublePage }

func (index doublePageIndex) NumPages() int       { return 1 }
func (index doublePageIndex) NullCount(int) int64 { return 0 }
func (index doublePageIndex) NullPage(int) bool   { return false }
func (index doublePageIndex) MinValue(int) []byte { return plain.Double(index.page.min()) }
func (index doublePageIndex) MaxValue(int) []byte { return plain.Double(index.page.max()) }
func (index doublePageIndex) IsAscending() bool   { return compareFloat64(index.page.bounds()) < 0 }
func (index doublePageIndex) IsDescending() bool  { return compareFloat64(index.page.bounds()) > 0 }

type byteArrayPageIndex struct{ page *byteArrayPage }

func (index byteArrayPageIndex) NumPages() int       { return 1 }
func (index byteArrayPageIndex) NullCount(int) int64 { return 0 }
func (index byteArrayPageIndex) NullPage(int) bool   { return false }
func (index byteArrayPageIndex) MinValue(int) []byte { return copyBytes(index.page.min()) }
func (index byteArrayPageIndex) MaxValue(int) []byte { return copyBytes(index.page.max()) }
func (index byteArrayPageIndex) IsAscending() bool   { return bytes.Compare(index.page.bounds()) < 0 }
func (index byteArrayPageIndex) IsDescending() bool  { return bytes.Compare(index.page.bounds()) > 0 }

type fixedLenByteArrayPageIndex struct{ page *fixedLenByteArrayPage }

func (index fixedLenByteArrayPageIndex) NumPages() int       { return 1 }
func (index fixedLenByteArrayPageIndex) NullCount(int) int64 { return 0 }
func (index fixedLenByteArrayPageIndex) NullPage(int) bool   { return false }
func (index fixedLenByteArrayPageIndex) MinValue(int) []byte { return copyBytes(index.page.min()) }
func (index fixedLenByteArrayPageIndex) MaxValue(int) []byte { return copyBytes(index.page.max()) }
func (index fixedLenByteArrayPageIndex) IsAscending() bool {
	return bytes.Compare(index.page.bounds()) < 0
}
func (index fixedLenByteArrayPageIndex) IsDescending() bool {
	return bytes.Compare(index.page.bounds()) > 0
}

type uint32PageIndex struct{ page uint32Page }

func (index uint32PageIndex) NumPages() int       { return 1 }
func (index uint32PageIndex) NullCount(int) int64 { return 0 }
func (index uint32PageIndex) NullPage(int) bool   { return false }
func (index uint32PageIndex) MinValue(int) []byte { return plain.Int32(int32(index.page.min())) }
func (index uint32PageIndex) MaxValue(int) []byte { return plain.Int32(int32(index.page.max())) }
func (index uint32PageIndex) IsAscending() bool   { return compareUint32(index.page.bounds()) < 0 }
func (index uint32PageIndex) IsDescending() bool  { return compareUint32(index.page.bounds()) > 0 }

type uint64PageIndex struct{ page uint64Page }

func (index uint64PageIndex) NumPages() int       { return 1 }
func (index uint64PageIndex) NullCount(int) int64 { return 0 }
func (index uint64PageIndex) NullPage(int) bool   { return false }
func (index uint64PageIndex) MinValue(int) []byte { return plain.Int64(int64(index.page.min())) }
func (index uint64PageIndex) MaxValue(int) []byte { return plain.Int64(int64(index.page.max())) }
func (index uint64PageIndex) IsAscending() bool   { return compareUint64(index.page.bounds()) < 0 }
func (index uint64PageIndex) IsDescending() bool  { return compareUint64(index.page.bounds()) > 0 }
