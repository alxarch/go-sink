package sink

import (
	"sync/atomic"
	"time"
)

type Flusher interface {
	Flush([]interface{}) error
}

type Pusher interface {
	Push(interface{})
}

type FlushFunc func([]interface{}) error

func (f FlushFunc) Flush(batch []interface{}) error {
	return f(batch)
}

// type Options struct {
// 	BatchSize     int
// 	FlushInterval time.Duration
// 	// Retries       int
// }
//
// func (o *Options) Flags(f *flag.FlagSet, prefix string) {
// 	if prefix = strings.Trim(prefix, "- "); prefix == "" {
// 		prefix = "sink-"
// 	} else {
// 		prefix += "-"
// 	}
// 	f.DurationVar(&o.FlushInterval, prefix+"flush-interval", DefaultFlushInterval, "Sink flush interval")
// 	// f.IntVar(&o.Retries, prefix+"flush-retries", 0, "Sink flush retries on error")
// 	f.IntVar(&o.BatchSize, prefix+"batch-size", DefaultBatchSize, "Sink batch size")
// }
//
// func (o *Options) Sink(f Flusher) *Sink {
// 	return NewSink(f, *o)
// }

type Sink struct {
	f             Flusher
	batchSize     int
	flushInterval time.Duration
	metrics       Metrics
	queue         chan interface{}
	done          chan struct{}
}

type Metrics struct {
	FlushErrors, FlushSize, FlushTick, FlushItems int64
}

func (s *Sink) Metrics() (m Metrics) {
	m.FlushErrors = atomic.LoadInt64(&s.metrics.FlushErrors)
	m.FlushSize = atomic.LoadInt64(&s.metrics.FlushSize)
	m.FlushTick = atomic.LoadInt64(&s.metrics.FlushTick)
	m.FlushItems = atomic.LoadInt64(&s.metrics.FlushItems)
	return
}

func New(f Flusher, size int, interval time.Duration) *Sink {
	if f == nil {
		return nil
	}
	if size <= 0 {
		size = DefaultBatchSize
	}
	s := &Sink{
		f:             f,
		batchSize:     size,
		flushInterval: interval,
		queue:         make(chan interface{}, 2*size+1),
	}
	go s.run()
	return s
}

func (s *Sink) Close() {
	if s.done != nil {
		close(s.done)
	}
}

func (s *Sink) Push(x interface{}) {
	s.queue <- x
}

func (s *Sink) Flush(batch []interface{}) error {
	return s.f.Flush(batch)
}

func (s *Sink) BatchSize() int {
	return s.batchSize
}

func (s *Sink) FlushInterval() time.Duration {
	return s.flushInterval
}

func (s *Sink) run() {
	if s.done == nil {
		s.done = make(chan struct{})
	}
	size := s.batchSize
	batch := make([]interface{}, 0, size)
	flush := func() {
		if n := len(batch); n > 0 {
			atomic.AddInt64(&s.metrics.FlushItems, int64(n))
			// Non-blocking flush
			go func(batch []interface{}) {
				if err := s.f.Flush(batch); err != nil {
					atomic.AddInt64(&s.metrics.FlushErrors, 1)
				}
			}(batch)
			batch = make([]interface{}, 0, size)
		}
	}
	defer flush()
	defer close(s.queue)
	var tick *time.Ticker
	if dt := s.flushInterval; dt > 0 {
		tick = time.NewTicker(dt)
		defer tick.Stop()
	} else {
		tick = time.NewTicker(time.Hour)
		tick.Stop()
	}
	for {
		select {
		case _ = <-s.done:
			s.done = nil
			return
		case _ = <-tick.C:
			flush()
			atomic.AddInt64(&s.metrics.FlushTick, 1)
		case x := <-s.queue:
			if len(batch) == cap(batch) {
				flush()
				atomic.AddInt64(&s.metrics.FlushSize, 1)
			}
			batch = append(batch, x)
		}
	}
}

const (
	DefaultBatchSize = 100
)
