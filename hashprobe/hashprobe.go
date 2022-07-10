// Package hashprobe provides implementations of probing tables for various
// data types.
//
// Probing tables are specialized hash tables supporting only a single
// "probing" operation which behave like a "lookup or insert". When a key
// is probed, either its value is retrieved if it already existed in the table,
// or it is inserted and assigned its index in the insert sequence as value.
//
// Values are represented as signed 32 bits integers, which means that probing
// tables defined in this package may contain at most 2^31-1 entries.
//
// Probing tables have a method named Probe with the following signature:
//
//	func (t *Int64Table) Probe(keys []int64, values []int32) int {
//		...
//	}
//
// The method takes an array of keys to probe as first argument, an array of
// values where the indexes of each key will be written as second argument, and
// returns the number of keys that were inserted during the call.
//
// Applications that need to determine which keys were inserted can capture the
// length of the probing table prior to the call, and scan the list of values
// looking for indexes greater or equal to the length of the table before the
// call.
package hashprobe

import (
	cryptoRand "crypto/rand"
	"encoding/binary"
	"math"
	"math/bits"
	"math/rand"
	"sync"

	"github.com/segmentio/parquet-go/hashprobe/aeshash"
	"github.com/segmentio/parquet-go/hashprobe/wyhash"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

const (
	// Number of probes tested per iteration. This parameter balances between
	// the amount of memory allocated on the stack to hold the computed hashes
	// of the keys being probed, and amortizing the baseline cost of the probing
	// algorithm.
	//
	// The larger the value, the more memory is required, but lower the baseline
	// cost will be.
	//
	// We chose a value that is somewhat large, resulting in reserving 2KiB of
	// stack but mostly erasing the baseline cost.
	probesPerLoop = 256
)

var (
	prngSeed   [8]byte
	prngMutex  sync.Mutex
	prngSource rand.Source64
)

func init() {
	_, err := cryptoRand.Read(prngSeed[:])
	if err != nil {
		panic("cannot seed random number generator from system source: " + err.Error())
	}
	seed := int64(binary.LittleEndian.Uint64(prngSeed[:]))
	prngSource = rand.NewSource(seed).(rand.Source64)
}

func nextPowerOf2(n int) int {
	return 1 << (64 - bits.LeadingZeros64(uint64(n-1)))
}

func randSeed() uintptr {
	prngMutex.Lock()
	defer prngMutex.Unlock()
	return uintptr(prngSource.Uint64())
}

type Int32Table struct{ table32 }

func NewInt32Table(cap int, maxLoad float64) *Int32Table {
	return &Int32Table{makeTable32(cap, maxLoad)}
}

func (t *Int32Table) Reset() { t.reset() }

func (t *Int32Table) Len() int { return t.len }

func (t *Int32Table) Cap() int { return t.size() }

func (t *Int32Table) Probe(keys, values []int32) int {
	return t.probe(unsafecast.Int32ToUint32(keys), values)
}

type Float32Table struct{ table32 }

func NewFloat32Table(cap int, maxLoad float64) *Float32Table {
	return &Float32Table{makeTable32(cap, maxLoad)}
}

func (t *Float32Table) Reset() { t.reset() }

func (t *Float32Table) Len() int { return t.len }

func (t *Float32Table) Cap() int { return t.size() }

func (t *Float32Table) Probe(keys []float32, values []int32) int {
	return t.probe(unsafecast.Float32ToUint32(keys), values)
}

type Uint32Table struct{ table32 }

func NewUint32Table(cap int, maxLoad float64) *Uint32Table {
	return &Uint32Table{makeTable32(cap, maxLoad)}
}

func (t *Uint32Table) Reset() { t.reset() }

func (t *Uint32Table) Len() int { return t.len }

func (t *Uint32Table) Cap() int { return t.size() }

func (t *Uint32Table) Probe(keys []uint32, values []int32) int {
	return t.probe(keys, values)
}

// table32 is the generic implementation of probing tables for 32 bit types.
//
// The table uses the following memory layout:
//
//		[group 0][group 1][...][group N]
//
// Each group contains up to 7 key/value pairs, and is exactly 64 bytes in size,
// which allows it to fit within a single cache line, and ensures that probes
// can be performed with a single memory load per key.
//
// Groups fill up by appending new entries to the keys and values arrays. When a
// group is full, the probe checks the next group.
//
// https://en.wikipedia.org/wiki/Linear_probing
type table32 struct {
	len     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []table32Group
}

const table32GroupSize = 7

type table32Group struct {
	keys   [table32GroupSize]uint32
	values [table32GroupSize]uint32
	bits   uint32
	_      uint32
}

func makeTable32(cap int, maxLoad float64) (t table32) {
	if maxLoad < 0 || maxLoad > 1 {
		panic("max load of probing table must be a value between 0 and 1")
	}
	if cap < table32GroupSize {
		cap = table32GroupSize
	}
	t.init(cap, maxLoad)
	return t
}

func (t *table32) size() int {
	return table32GroupSize * len(t.table)
}

func (t *table32) init(cap int, maxLoad float64) {
	m := int(math.Ceil((1 / maxLoad) * float64(cap)))
	n := nextPowerOf2((m + (table32GroupSize - 1)) / table32GroupSize)
	*t = table32{
		maxLen:  int(math.Ceil(maxLoad * float64(table32GroupSize*n))),
		maxLoad: maxLoad,
		seed:    randSeed(),
		table:   make([]table32Group, n),
	}
}

func (t *table32) grow(totalValues int) {
	tmp := table32{}
	tmp.init(totalValues, t.maxLoad)
	tmp.len = t.len

	hashes := make([]uintptr, table32GroupSize)
	modulo := uintptr(len(tmp.table)) - 1

	for i := range t.table {
		g := &t.table[i]
		n := bits.OnesCount32(g.bits)

		if aeshash.Enabled() {
			aeshash.MultiHash32(hashes[:n], g.keys[:n], tmp.seed)
		} else {
			wyhash.MultiHash32(hashes[:n], g.keys[:n], tmp.seed)
		}

		for j, hash := range hashes[:n] {
			for {
				group := &tmp.table[hash&modulo]

				if n := bits.OnesCount32(group.bits); n < table32GroupSize {
					group.bits = (group.bits << 1) | 1
					group.keys[n] = g.keys[j]
					group.values[n] = g.values[j]
					break
				}

				hash++
			}
		}
	}

	*t = tmp
}

func (t *table32) reset() {
	t.len = 0

	for i := range t.table {
		t.table[i] = table32Group{}
	}
}

func (t *table32) probe(keys []uint32, values []int32) int {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}

	var hashes [probesPerLoop]uintptr
	var baseLength = t.len
	var useAesHash = aeshash.Enabled()

	_ = values[:len(keys)]

	for i := 0; i < len(keys); {
		j := len(hashes) + i
		n := len(hashes)

		if j > len(keys) {
			j = len(keys)
			n = len(keys) - i
		}

		h := hashes[:n:n]
		k := keys[i:j:j]
		v := values[i:j:j]

		if useAesHash {
			aeshash.MultiHash32(h, k, t.seed)
		} else {
			wyhash.MultiHash32(h, k, t.seed)
		}

		t.len = multiProbe32(t.table, t.len, h, k, v)
		i = j
	}

	return t.len - baseLength
}

func multiProbe32Default(table []table32Group, numKeys int, hashes []uintptr, keys []uint32, values []int32) int {
	modulo := uintptr(len(table)) - 1

	for i, hash := range hashes {
		key := keys[i]
		for {
			group := &table[hash&modulo]
			index := table32GroupSize
			value := int32(0)

			for i, k := range group.keys {
				if k == key {
					index = i
					break
				}
			}

			if n := bits.OnesCount32(group.bits); index < n {
				value = int32(group.values[index])
			} else {
				if n == table32GroupSize {
					hash++
					continue
				}

				value = int32(numKeys)
				group.bits = (group.bits << 1) | 1
				group.keys[n] = key
				group.values[n] = uint32(value)
				numKeys++
			}

			values[i] = value
			break
		}
	}

	return numKeys
}

type Int64Table struct{ table64 }

func NewInt64Table(cap int, maxLoad float64) *Int64Table {
	return &Int64Table{makeTable64(cap, maxLoad)}
}

func (t *Int64Table) Reset() { t.reset() }

func (t *Int64Table) Len() int { return t.len }

func (t *Int64Table) Cap() int { return t.size() }

func (t *Int64Table) Probe(keys []int64, values []int32) int {
	return t.probe(unsafecast.Int64ToUint64(keys), values)
}

type Float64Table struct{ table64 }

func NewFloat64Table(cap int, maxLoad float64) *Float64Table {
	return &Float64Table{makeTable64(cap, maxLoad)}
}

func (t *Float64Table) Reset() { t.reset() }

func (t *Float64Table) Len() int { return t.len }

func (t *Float64Table) Cap() int { return t.size() }

func (t *Float64Table) Probe(keys []float64, values []int32) int {
	return t.probe(unsafecast.Float64ToUint64(keys), values)
}

type Uint64Table struct{ table64 }

func NewUint64Table(cap int, maxLoad float64) *Uint64Table {
	return &Uint64Table{makeTable64(cap, maxLoad)}
}

func (t *Uint64Table) Reset() { t.reset() }

func (t *Uint64Table) Len() int { return t.len }

func (t *Uint64Table) Cap() int { return t.size() }

func (t *Uint64Table) Probe(keys []uint64, values []int32) int {
	return t.probe(keys, values)
}

// table64 is the generic implementation of probing tables for 64 bit types.
//
// The table uses a layout similar to the one documented on the table for 32 bit
// keys (see table32). Each group holds up to 4 key/value pairs (instead of 7
// like table32) so that each group fits in a single CPU cache line. This table
// version has a bit lower memory density, with ~23% of table memory being used
// for padding.
//
// Technically we could hold up to 5 entries per group and still fit within the
// 64 bytes of a CPU cache line; on x86 platforms, AVX2 registers can only hold
// four 64 bit values, we would need twice as many instructions per probe if the
// groups were holding 5 values. The trade off of memory for compute efficiency
// appeared to be the right choice at the time.
type table64 struct {
	len     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []table64Group
}

const table64GroupSize = 4

type table64Group struct {
	keys   [table64GroupSize]uint64
	values [table64GroupSize]uint32
	bits   uint32
	_      uint32
	_      uint32
	_      uint32
}

func makeTable64(cap int, maxLoad float64) (t table64) {
	if maxLoad < 0 || maxLoad > 1 {
		panic("max load of probing table must be a value between 0 and 1")
	}
	if cap < table64GroupSize {
		cap = table64GroupSize
	}
	t.init(nextPowerOf2(cap), maxLoad)
	return t
}

func (t *table64) size() int {
	return table64GroupSize * len(t.table)
}

func (t *table64) init(cap int, maxLoad float64) {
	m := int(math.Ceil((1 / maxLoad) * float64(cap)))
	n := nextPowerOf2((m + (table64GroupSize - 1)) / table64GroupSize)
	*t = table64{
		maxLen:  int(math.Ceil(maxLoad * float64(table64GroupSize*n))),
		maxLoad: maxLoad,
		seed:    randSeed(),
		table:   make([]table64Group, n),
	}
}

func (t *table64) grow(totalValues int) {
	tmp := table64{}
	tmp.init(totalValues, t.maxLoad)
	tmp.len = t.len

	hashes := make([]uintptr, table64GroupSize)
	modulo := uintptr(len(tmp.table)) - 1

	for i := range t.table {
		g := &t.table[i]
		n := bits.OnesCount32(g.bits)

		if aeshash.Enabled() {
			aeshash.MultiHash64(hashes[:n], g.keys[:n], tmp.seed)
		} else {
			wyhash.MultiHash64(hashes[:n], g.keys[:n], tmp.seed)
		}

		for j, hash := range hashes[:n] {
			for {
				group := &tmp.table[hash&modulo]

				if n := bits.OnesCount32(group.bits); n < table64GroupSize {
					group.bits = (group.bits << 1) | 1
					group.keys[n] = g.keys[j]
					group.values[n] = g.values[j]
					break
				}

				hash++
			}
		}
	}

	*t = tmp
}

func (t *table64) reset() {
	t.len = 0

	for i := range t.table {
		t.table[i] = table64Group{}
	}
}

func (t *table64) probe(keys []uint64, values []int32) int {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}

	var hashes [probesPerLoop]uintptr
	var baseLength = t.len
	var useAesHash = aeshash.Enabled()

	_ = values[:len(keys)]

	for i := 0; i < len(keys); {
		j := len(hashes) + i
		n := len(hashes)

		if j > len(keys) {
			j = len(keys)
			n = len(keys) - i
		}

		h := hashes[:n:n]
		k := keys[i:j:j]
		v := values[i:j:j]

		if useAesHash {
			aeshash.MultiHash64(h, k, t.seed)
		} else {
			wyhash.MultiHash64(h, k, t.seed)
		}

		t.len = multiProbe64(t.table, t.len, h, k, v)
		i = j
	}

	return t.len - baseLength
}

func multiProbe64Default(table []table64Group, numKeys int, hashes []uintptr, keys []uint64, values []int32) int {
	modulo := uintptr(len(table)) - 1

	for i, hash := range hashes {
		key := keys[i]
		for {
			group := &table[hash&modulo]
			index := table64GroupSize
			value := int32(0)

			for i, k := range group.keys {
				if k == key {
					index = i
					break
				}
			}

			if n := bits.OnesCount32(group.bits); index < n {
				value = int32(group.values[index])
			} else {
				if n == table64GroupSize {
					hash++
					continue
				}

				value = int32(numKeys)
				group.bits = (group.bits << 1) | 1
				group.keys[n] = key
				group.values[n] = uint32(value)
				numKeys++
			}

			values[i] = value
			break
		}
	}

	return numKeys
}

type Uint128Table struct{ table128 }

func NewUint128Table(cap int, maxLoad float64) *Uint128Table {
	return &Uint128Table{makeTable128(cap, maxLoad)}
}

func (t *Uint128Table) Reset() { t.reset() }

func (t *Uint128Table) Len() int { return t.len }

func (t *Uint128Table) Cap() int { return t.cap }

func (t *Uint128Table) Probe(keys [][16]byte, values []int32) int {
	return t.probe(keys, values)
}

// table128 is the generic implementation of probing tables for 128 bit types.
//
// The datastructure follows the same implementation as the one used by table32
// but the keys are 128 bit values.
type table128 struct {
	len     int
	cap     int
	maxLen  int
	maxLoad float64
	seed    uintptr
	table   []byte
}

func makeTable128(cap int, maxLoad float64) (t table128) {
	if maxLoad < 0 || maxLoad > 1 {
		panic("max load of probing table must be a value between 0 and 1")
	}
	if cap < 64 {
		cap = 64
	}
	t.init(nextPowerOf2(cap), maxLoad)
	return t
}

func (t *table128) init(cap int, maxLoad float64) {
	*t = table128{
		cap:     cap,
		maxLen:  int(math.Ceil(maxLoad * float64(cap))),
		maxLoad: maxLoad,
		seed:    randSeed(),
		table:   make([]byte, cap/8+16*cap+4*cap),
	}
}

func (t *table128) grow(totalValues int) {
	cap := 2 * t.cap
	totalValues = nextPowerOf2(totalValues)
	if totalValues > cap {
		cap = totalValues
	}

	tmp := table128{}
	tmp.init(cap, t.maxLoad)
	tmp.len = t.len

	flags, keys, values := t.content()

	for i, f := range flags {
		if f != 0 {
			j := 64 * i
			n := bits.TrailingZeros64(f)
			j += n
			f >>= uint(n)

			for {
				n := bits.TrailingZeros64(^f)
				k := j + n
				tmp.insert(keys[j:k:k], values[j:k:k])
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

func (t *table128) insert(keys [][16]byte, values []int32) {
	tableFlags, tableKeys, tableValues := t.content()
	hashes := make([]uintptr, len(keys), 64)
	modulo := uintptr(t.cap) - 1

	if aeshash.Enabled() {
		aeshash.MultiHash128(hashes, keys, t.seed)
	} else {
		wyhash.MultiHash128(hashes, keys, t.seed)
	}

	for i, hash := range hashes {
		for {
			hash &= modulo
			index := hash / 64
			shift := hash % 64

			if (tableFlags[index] & (1 << shift)) == 0 {
				tableFlags[index] |= 1 << shift
				tableKeys[hash] = keys[i]
				tableValues[hash] = values[i]
				break
			}

			hash++
		}
	}
}

func (t *table128) content() (flags []uint64, keys [][16]byte, values []int32) {
	i := t.cap / 8
	j := 16*t.cap + i
	k := len(t.table)
	return unsafecast.BytesToUint64(t.table[:i:i]), unsafecast.BytesToUint128(t.table[i:j:j]), unsafecast.BytesToInt32(t.table[j:k:k])
}

func (t *table128) reset() {
	t.len = 0

	for i := range t.table {
		t.table[i] = 0
	}
}

func (t *table128) probe(keys [][16]byte, values []int32) int {
	if totalValues := t.len + len(keys); totalValues > t.maxLen {
		t.grow(totalValues)
	}

	var hashes [probesPerLoop]uintptr
	var baseLength = t.len
	var useAesHash = aeshash.Enabled()

	_ = values[:len(keys)]

	for i := 0; i < len(keys); {
		j := len(hashes) + i
		n := len(hashes)

		if j > len(keys) {
			j = len(keys)
			n = len(keys) - i
		}

		h := hashes[:n:n]
		k := keys[i:j:j]
		v := values[i:j:j]

		if useAesHash {
			aeshash.MultiHash128(h, k, t.seed)
		} else {
			wyhash.MultiHash128(h, k, t.seed)
		}

		t.len = multiProbe128(t.table, t.len, t.cap, h, k, v)
		i = j
	}

	return t.len - baseLength
}
