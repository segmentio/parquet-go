//go:build !go1.18

package parquet

func (index booleanPageIndex) Offset(int) int64             { return 0 }
func (index booleanPageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index booleanPageIndex) FirstRowIndex(int) int64      { return 0 }

func (index int32PageIndex) Offset(int) int64             { return 0 }
func (index int32PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index int32PageIndex) FirstRowIndex(int) int64      { return 0 }

func (index int64PageIndex) Offset(int) int64             { return 0 }
func (index int64PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index int64PageIndex) FirstRowIndex(int) int64      { return 0 }

func (index int96PageIndex) Offset(int) int64             { return 0 }
func (index int96PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index int96PageIndex) FirstRowIndex(int) int64      { return 0 }

func (index floatPageIndex) Offset(int) int64             { return 0 }
func (index floatPageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index floatPageIndex) FirstRowIndex(int) int64      { return 0 }

func (index doublePageIndex) Offset(int) int64             { return 0 }
func (index doublePageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index doublePageIndex) FirstRowIndex(int) int64      { return 0 }

func (index uint32PageIndex) Offset(int) int64             { return 0 }
func (index uint32PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index uint32PageIndex) FirstRowIndex(int) int64      { return 0 }

func (index uint64PageIndex) Offset(int) int64             { return 0 }
func (index uint64PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index uint64PageIndex) FirstRowIndex(int) int64      { return 0 }
