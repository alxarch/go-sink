package sink

import (
	"log"
	"sync/atomic"
	"time"
)

type Flusher interface {
	Flush([]interface{}) error
}

type FlushFunc func([]interface{}) error

func (f FlushFunc) Flush(batch []interface{}) error {
	return f(batch)
}

type Options struct {
	BatchSize     int
	FlushInterval time.Duration
	Retries       int
	Logger        *log.Logger
}

type Sink struct {
	flush   FlushFunc
	opts    Options
	metrics Metrics
	queue   chan interface{}
	done    chan struct{}
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

func NewSink(f Flusher, opts Options) *Sink {
	if f == nil {
		return nil
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBatchSize
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultFlushInterval
	}
	if opts.Retries < 0 {
		opts.Retries = 0
	}
	s := &Sink{
		flush: f.Flush,
		opts:  opts,
		queue: make(chan interface{}, 2*opts.BatchSize),
		done:  make(chan struct{}),
	}
	go s.init()
	return s
}

func (s *Sink) Close() {
	if s.done != nil {
		close(s.done)
	}
}

func (s *Sink) Add(x interface{}) {
	s.queue <- x
}

func (s *Sink) init() {
	tick := time.NewTicker(s.opts.FlushInterval)
	batch := make([]interface{}, 0, s.opts.BatchSize)
	flush := func() {
		if n := len(batch); n > 0 {
			atomic.AddInt64(&s.metrics.FlushItems, int64(n))
			// Non-blocking flush
			go func(batch []interface{}) {
				if err := s.Flush(batch); err != nil {
					atomic.AddInt64(&s.metrics.FlushErrors, 1)
					if s.opts.Logger != nil {
						s.opts.Logger.Println(err)
					}
				}
			}(batch)
			batch = make([]interface{}, 0, s.opts.BatchSize)
		}
	}
	defer func() {
		tick.Stop()
		close(s.queue)
		flush()
	}()
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

func (s *Sink) Flush(batch []interface{}) (err error) {
	for r := 0; r <= s.opts.Retries; r++ {
		if err = s.flush(batch); err == nil {
			break
		}
	}

	return
}

const (
	DefaultFlushInterval = time.Second
	DefaultBatchSize     = 1000
)
