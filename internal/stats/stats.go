package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

type Counter int64

func (c *Counter) ptr() *int64 {
	return (*int64)(c)
}

func (c *Counter) Inc() {
	atomic.AddInt64(c.ptr(), 1)
}

func (c *Counter) Snapshot() int64 {
	return atomic.SwapInt64(c.ptr(), 0)
}

func (c *Counter) Add(n int64) {
	atomic.AddInt64(c.ptr(), n)
}

type Duration struct {
	t     time.Time
	mutex sync.Mutex
}

func (d *Duration) Init() {
	if !d.t.IsZero() {
		return
	}
	d.mutex.Lock()
	if !d.t.IsZero() {
		return
	}
	d.t = time.Now()
	d.mutex.Unlock()
}

func (d *Duration) Snapshot() time.Duration {
	d.mutex.Lock()
	now := time.Now()
	delta := now.Sub(d.t)
	d.t = now
	d.mutex.Unlock()
	return delta
}
