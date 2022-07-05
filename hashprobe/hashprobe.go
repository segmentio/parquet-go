package hashprobe

import (
	"math"
	"math/bits"
	"math/rand"

	"github.com/segmentio/parquet-go/hashprobe/aeshash"
	"github.com/segmentio/parquet-go/hashprobe/wyhash"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func hash32bits(value, seed uint32) uint32 {
	if aeshash.Enabled() {
		return aeshash.Sum32Uint32(value, seed)
	} else {
		return wyhash.Sum32Uint32(value, seed)
	}
}

func multiHash32bits(hashes, values []uint32, seed uint32) {
	if aeshash.Enabled() {
		aeshash.MultiSum32Uint32(hashes, values, seed)
	} else {
		wyhash.MultiSum32Uint32(hashes, values, seed)
	}
}

func hash64bits(value, seed uint64) uint64 {
	if aeshash.Enabled() {
		return aeshash.Sum64Uint64(value, seed)
	} else {
		return wyhash.Sum64Uint64(value, seed)
	}
}

func multiHash64bits(hashes, values []uint64, seed uint64) {
	if aeshash.Enabled() {
		aeshash.MultiSum64Uint64(hashes, values, seed)
	} else {
		wyhash.MultiSum64Uint64(hashes, values, seed)
	}
}

func nextPowerOf2(n int) int {
	return 1 << (64 - bits.LeadingZeros64(uint64(n-1)))
}

type Int32Table struct{ table32bits }

func NewInt32Table(cap int, maxLoad float64) *Int32Table {
	return &Int32Table{makeTable32bits(cap, maxLoad)}
}

func (t *Int32Table) Reset() { t.reset() }

func (t *Int32Table) Len() int { return t.len }

func (t *Int32Table) Cap() int { return t.cap }

func (t *Int32Table) Probe(keys, values []int32) {
	t.probe(unsafecast.Int32ToUint32(keys), values)
}

type Float32Table struct{ table32bits }

func NewFloat32Table(cap int, maxLoad float64) *Float32Table {
	return &Float32Table{makeTable32bits(cap, maxLoad)}
}

func (t *Float32Table) Reset() { t.reset() }

func (t *Float32Table) Len() int { return t.len }

func (t *Float32Table) Cap() int { return t.cap }

func (t *Float32Table) Probe(keys []float32, values []int32) {
	t.probe(unsafecast.Float32ToUint32(keys), values)
}

type Uint32Table struct{ table32bits }

func NewUint32Table(cap int, maxLoad float64) *Uint32Table {
	return &Uint32Table{makeTable32bits(cap, maxLoad)}
}

func (t *Uint32Table) Reset() { t.reset() }

func (t *Uint32Table) Len() int { return t.len }

func (t *Uint32Table) Cap() int { return t.cap }

func (t *Uint32Table) Probe(keys []uint32, values []int32) { t.probe(keys, values) }

type table32bits struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uint32
	table   []uint32
}

func makeTable32bits(cap int, maxLoad float64) (t table32bits) {
	if cap < 32 {
		cap = 32
	}
	t.init(nextPowerOf2(cap), maxLoad)
	return t
}

func (t *table32bits) init(cap int, maxLoad float64) {
	*t = table32bits{
		cap:     cap,
		maxLen:  int(math.Ceil(maxLoad * float64(cap))),
		maxLoad: maxLoad,
		seed:    rand.Uint32(),
		table:   make([]uint32, cap/32+2*cap),
	}
}

func (t *table32bits) grow(totalValues int) {
	cap := 2 * t.cap
	totalValues = nextPowerOf2(totalValues)
	if totalValues > cap {
		cap = totalValues
	}

	tmp := table32bits{}
	tmp.init(cap, t.maxLoad)
	tmp.len = t.len

	flags, pairs := t.content()

	for i, f := range flags {
		if f != 0 {
			j := 32 * i
			n := bits.TrailingZeros32(f)
			j += n
			f >>= uint(n)

			for {
				n := bits.TrailingZeros32(^f)
				k := j + n
				tmp.insert(pairs[2*j : 2*k : 2*k])
				j += n
				f >>= uint(n)
				if f == 0 {
					break
				}
				n = bits.TrailingZeros32(f)
				j += n
				f >>= uint(n)
			}
		}
	}

	*t = tmp
}

func (t *table32bits) insert(pairs []uint32) {
	flags, table := t.content()
	mod := uint32(t.cap) - 1

	for i := 0; i < len(pairs); i += 2 {
		hash := hash32bits(pairs[i], t.seed)

		for {
			position := hash & mod
			index := position / 32
			shift := position % 32

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

func (t *table32bits) content() (flags, pairs []uint32) {
	n := t.cap / 32
	return t.table[:n:n], t.table[n:len(t.table):len(t.table)]
}

func (t *table32bits) reset() {
	for i := range t.table {
		t.table[i] = 0
	}
	t.len = 0
}

func (t *table32bits) probe(keys []uint32, values []int32) {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}

	var hashes [256]uint32

	for i := 0; i < len(keys); {
		j := len(hashes) + i
		n := len(hashes)

		if j > len(keys) {
			j = len(keys)
			n = len(keys) - i
		}

		multiHash32bits(hashes[:n:n], keys[i:j:j], t.seed)
		t.len = multiProbe32bits(t.table, t.len, t.cap, hashes[:n:n], keys[i:j:j], values[i:j:j])

		i = j
	}
}

type Int64Table struct{ table64bits }

func NewInt64Table(cap int, maxLoad float64) *Int64Table {
	return &Int64Table{makeTable64bits(cap, maxLoad)}
}

func (t *Int64Table) Reset() { t.reset() }

func (t *Int64Table) Len() int { return t.len }

func (t *Int64Table) Cap() int { return t.cap }

func (t *Int64Table) Probe(keys []int64, values []int32) {
	t.probe(unsafecast.Int64ToUint64(keys), values)
}

type Float64Table struct{ table64bits }

func NewFloat64Table(cap int, maxLoad float64) *Float64Table {
	return &Float64Table{makeTable64bits(cap, maxLoad)}
}

func (t *Float64Table) Reset() { t.reset() }

func (t *Float64Table) Len() int { return t.len }

func (t *Float64Table) Cap() int { return t.cap }

func (t *Float64Table) Probe(keys []float64, values []int32) {
	t.probe(unsafecast.Float64ToUint64(keys), values)
}

type Uint64Table struct{ table64bits }

func NewUint64Table(cap int, maxLoad float64) *Uint64Table {
	return &Uint64Table{makeTable64bits(cap, maxLoad)}
}

func (t *Uint64Table) Reset() { t.reset() }

func (t *Uint64Table) Len() int { return t.len }

func (t *Uint64Table) Cap() int { return t.cap }

func (t *Uint64Table) Probe(keys []uint64, values []int32) { t.probe(keys, values) }

type table64bits struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uint64
	table   []uint64
}

func makeTable64bits(cap int, maxLoad float64) (t table64bits) {
	if cap < 64 {
		cap = 64
	}
	t.init(nextPowerOf2(cap), maxLoad)
	return t
}

func (t *table64bits) init(cap int, maxLoad float64) {
	*t = table64bits{
		cap:     cap,
		maxLen:  int(math.Ceil(maxLoad * float64(cap))),
		maxLoad: maxLoad,
		seed:    rand.Uint64(),
		table:   make([]uint64, cap/64+2*cap),
	}
}

func (t *table64bits) grow(totalValues int) {
	cap := 2 * t.cap
	totalValues = nextPowerOf2(totalValues)
	if totalValues > cap {
		cap = totalValues
	}

	tmp := table64bits{}
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

func (t *table64bits) insert(pairs []uint64) {
	flags, table := t.content()
	mod := uint64(t.cap) - 1

	for i := 0; i < len(pairs); i += 2 {
		hash := hash64bits(pairs[i], t.seed)

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

func (t *table64bits) content() (flags, pairs []uint64) {
	n := t.cap / 64
	return t.table[:n:n], t.table[n:len(t.table):len(t.table)]
}

func (t *table64bits) reset() {
	for i := range t.table {
		t.table[i] = 0
	}
	t.len = 0
}

func (t *table64bits) probe(keys []uint64, values []int32) {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}

	var hashes [128]uint64

	for i := 0; i < len(keys); {
		j := len(hashes) + i
		n := len(hashes)

		if j > len(keys) {
			j = len(keys)
			n = len(keys) - i
		}

		multiHash64bits(hashes[:n:n], keys[i:j:j], t.seed)
		t.len = multiProbe64bits(t.table, t.len, t.cap, hashes[:n:n], keys[i:j:j], values[i:j:j])

		i = j
	}
}
