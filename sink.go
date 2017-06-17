package sink

import (
	"sync/atomic"
	"time"
)

type Batch []interface{}
type Flusher interface {
	Flush(batch Batch) (err error)
}

type FlusherCloser interface {
	Flusher
	Closer
}

type nopCloser struct {
	Flusher
}

func (c nopCloser) Close() {}

type FlusherTo interface {
	FlushTo(f Flusher) error
}
type multiworker struct {
	parent Flusher
	tasks  chan<- task
}

type task struct {
	Batch Batch
	Err   chan<- error
}

func (m *multiworker) Flush(b Batch) error {
	err := make(chan error, 1)
	m.tasks <- task{b, err}
	return <-err
}

type Closer interface {
	Close()
}

func (m *multiworker) Close() {
	if c, ok := m.parent.(Closer); ok {
		c.Close()
	}
	close(m.tasks)
}

func MultiWorker(f Flusher, n int) Flusher {
	if n <= 1 {
		return f
	}
	tasks := make(chan task, n)
	for i := 0; i < n; i++ {
		go func() {
			for t := range tasks {
				t.Err <- f.Flush(t.Batch)
			}
		}()
	}
	return &multiworker{f, tasks}
}

func Retry(f Flusher, retries int) Flusher {
	if retries <= 1 {
		return f
	}
	return FlushFunc(func(b Batch) (err error) {
		for i := 0; i < retries; i++ {
			if err = f.Flush(b); err == nil {
				break
			}
		}
		return
	})
}

type Pusher interface {
	Push(interface{})
}

type Options struct {
	NumWorkers    int
	Retries       int
	FlushInterval time.Duration
	BatchSize     int
	QueueSize     int
}

func New(f Flusher, options Options) *Sink {
	if options.BatchSize <= 0 {
		options.BatchSize = DefaultBatchSize
	}
	if options.QueueSize <= 0 {
		options.QueueSize = 2*options.BatchSize + 1
	}
	s := &Sink{
		options: options,
		queue:   make(chan interface{}, options.QueueSize),
		done:    make(chan struct{}),
	}
	f = Retry(f, options.Retries)
	f = Instrument(f, &s.metrics)
	f = MultiWorker(f, options.NumWorkers)
	s.flusher = f
	go s.run(s.done)
	return s
}

type FlushFunc func(batch Batch) (err error)

func (f FlushFunc) Flush(batch Batch) error {
	return f(batch)
}

type Sink struct {
	flusher Flusher
	options Options
	metrics Metrics
	queue   chan interface{}
	done    chan struct{}
}

type Metrics struct {
	FlushErrors, Flush, FlushItems int64
}

func Instrument(f Flusher, m *Metrics) Flusher {
	return FlushFunc(func(b Batch) (err error) {
		if n := len(b); n > 0 {
			atomic.AddInt64(&m.FlushItems, int64(n))
			if err = f.Flush(b); err != nil {
				atomic.AddInt64(&m.FlushErrors, 1)
			} else {
				atomic.AddInt64(&m.Flush, 1)
			}
		}
		return
	})
}

func (s *Sink) Metrics() (m Metrics) {
	m.FlushErrors = atomic.LoadInt64(&s.metrics.FlushErrors)
	m.Flush = atomic.LoadInt64(&s.metrics.Flush)
	m.FlushItems = atomic.LoadInt64(&s.metrics.FlushItems)
	return
}

func (s *Sink) Close() {
	close(s.done)
}

func (s *Sink) Push(x interface{}) {
	s.queue <- x
}

func (s *Sink) run(done <-chan struct{}) {
	if c, ok := s.flusher.(Closer); ok {
		defer c.Close()
	}
	batch := make([]interface{}, 0, s.options.BatchSize)
	flush := func() {
		if len(batch) > 0 {
			s.flusher.Flush(batch)
			batch = make([]interface{}, 0, s.options.BatchSize)
		}
	}
	defer flush()
	defer close(s.queue)
	var tick *time.Ticker
	if dt := s.options.FlushInterval; dt > 0 {
		tick = time.NewTicker(dt)
		defer tick.Stop()
	} else {
		tick = time.NewTicker(time.Hour)
		tick.Stop()
	}
	for {
		select {
		case _ = <-done:
			return
		case _ = <-tick.C:
			flush()
		case x := <-s.queue:
			if len(batch) >= cap(batch) {
				flush()
			}
			batch = append(batch, x)
		}
	}
}

const (
	DefaultBatchSize = 100
)
