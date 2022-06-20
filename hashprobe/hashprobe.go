package hashprobe

import (
	"math"
	"math/bits"
)

type Uint64Table struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	table   []uint64
}

func NewUint64Table(cap int, maxLoad float64) *Uint64Table {
	if cap < 64 {
		cap = 64
	}
	t := new(Uint64Table)
	t.init(nextPowerOf2(cap), maxLoad)
	return t
}

func (t *Uint64Table) content() (flags, pairs []uint64) {
	n := t.cap / 64
	return t.table[:n:n], t.table[n:len(t.table):len(t.table)]
}

func (t *Uint64Table) init(cap int, maxLoad float64) {
	*t = Uint64Table{
		cap:     cap,
		maxLen:  int(math.Ceil(maxLoad * float64(cap))),
		maxLoad: maxLoad,
		table:   make([]uint64, cap/64+2*cap),
	}
}

func (t *Uint64Table) grow(totalValues int) {
	cap := 2 * t.cap
	totalValues = nextPowerOf2(totalValues)
	if totalValues > cap {
		cap = totalValues
	}

	tmp := Uint64Table{}
	tmp.init(cap, t.maxLoad)
	tmp.len = t.len

	flags, pairs := t.content()

	for i, f := range flags {
		if f != 0 {
			j := 64 * i
			n := bits.TrailingZeros64(f)
			j += n
			f >>= uint(n)

			for {
				n := bits.TrailingZeros64(^f)
				k := j + n
				tmp.insert(pairs[2*j : 2*k : 2*k])
				j += n
				f >>= uint(n)
				if f == 0 {
					break
				}
				n = bits.TrailingZeros64(f)
				j += n
				f >>= uint(n)
			}
		}
	}

	*t = tmp
}

func (t *Uint64Table) insert(pairs []uint64) {
	flags, table := t.content()
	mod := uint64(t.cap) - 1

	for i := 0; i < len(pairs); i += 2 {
		hash := hash64(0, pairs[i])

		for {
			position := hash & mod
			index := position / 64
			shift := position % 64

			if (flags[index] & (1 << shift)) == 0 {
				flags[index] |= 1 << shift
				table[2*position+0] = pairs[i+0]
				table[2*position+1] = pairs[i+1]
				break
			}

			hash++
		}
	}
}

func (t *Uint64Table) Reset() {
	for i := range t.table {
		t.table[i] = 0
	}
	t.len = 0
}

func (t *Uint64Table) Len() int { return t.len }

func (t *Uint64Table) Cap() int { return t.cap }

func (t *Uint64Table) Probe(keys []uint64, values []int32) {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}
	t.len = probe64(t.table, t.len, t.cap, keys, values)
}

func nextPowerOf2(n int) int {
	return 1 << (64 - bits.LeadingZeros64(uint64(n-1)))
}
