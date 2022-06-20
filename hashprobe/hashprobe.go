package hashprobe

import (
	"math"
	"math/bits"
)

type Uint64Table struct {
	flags  []uint64
	keys   []uint64
	values []int32
	limit  int
	len    int
	mod    uint64
}

func NewUint64Table(cap int) *Uint64Table {
	if cap < 64 {
		cap = 64
	}
	t := new(Uint64Table)
	t.init(nextPowerOf2(cap))
	return t
}

func (t *Uint64Table) init(cap int) {
	*t = Uint64Table{
		flags:  make([]uint64, cap/64),
		keys:   make([]uint64, cap),
		values: make([]int32, cap),
		limit:  int(math.Ceil(0.9 * float64(cap))),
		mod:    uint64(cap) - 1,
	}
}

func (t *Uint64Table) grow(size int) {
	cap := 2 * len(t.keys)
	size = nextPowerOf2(size)
	if size > cap {
		cap = size
	}
	tmp := Uint64Table{}
	tmp.init(cap)
	tmp.probe(t.keys, t.values)
	*t = tmp
}

func (t *Uint64Table) Reset() {
	for i := range t.flags {
		t.flags[i] = 0
	}
	t.len = 0
}

func (t *Uint64Table) Len() int {
	return t.len
}

func (t *Uint64Table) Cap() int {
	return len(t.keys)
}

func (t *Uint64Table) Probe(keys []uint64, values []int32) {
	if size := t.len + len(keys); size > t.limit {
		t.grow(size)
	}
	t.probe(keys, values)
}

func (t *Uint64Table) probe(keys []uint64, values []int32) {
	for i, key := range keys {
		hash := xxhash64(key)
		mod := uint32(t.mod)

		for {
			position := hash & mod
			index := position / 64
			shift := position % 64

			if (t.flags[index] & (1 << shift)) == 0 {
				value := int32(t.len)
				t.flags[index] |= 1 << shift
				t.keys[position] = key
				t.values[position] = value
				t.len++
				values[i] = value
				break
			}

			if t.keys[position] == keys[i] {
				values[i] = t.values[position]
				break
			}

			hash++
		}
	}
}

func nextPowerOf2(n int) int {
	return 1 << (64 - bits.LeadingZeros64(uint64(n-1)))
}
