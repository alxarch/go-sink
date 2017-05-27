package sink_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/alxarch/go-sink"
)

func Test_Retries(t *testing.T) {
	n := int64(0)
	done := make(chan struct{})
	once := new(sync.Once)
	f := sink.FlushFunc(func(b []interface{}) error {
		if atomic.AddInt64(&n, 1) > 2 {
			once.Do(func() {
				close(done)

			})
			return nil
		}
		return errors.New("ERR")
	})
	q := sink.NewSink(f, sink.Options{
		BatchSize:     2,
		Retries:       3,
		FlushInterval: 5 * time.Second,
	})
	defer q.Close()

	q.Add(1)
	q.Add(1)
	q.Add(1)
	<-done
	if n != 3 {
		t.Error("No retries")
	}
}
func Test_Queue(t *testing.T) {

	done := make(chan []interface{}, 1)
	f := sink.FlushFunc(func(b []interface{}) error {
		done <- b
		return nil
	})
	q := sink.NewSink(f, sink.Options{
		BatchSize:     2,
		FlushInterval: 5 * time.Second,
	})

	q.Add("a")
	q.Add("b")
	q.Add("c")
	b := <-done
	if len(b) != 2 {
		t.Errorf("Invalid batch length %d", len(b))
	}
	if v, ok := b[0].(string); !ok || v != "a" {
		t.Error("Invalid batch elem 0")
	}
	if v, ok := b[1].(string); !ok || v != "b" {
		t.Errorf("Invalid batch elem 1 %s", v)
	}
	q.Close()
	b = <-done
	metrics := q.Metrics()
	if metrics.FlushItems != 3 {
		t.Errorf("Invalid stats items %d", metrics.FlushItems)
	}
	if metrics.FlushSize != 1 {
		t.Error("Invalid stats fsize")
	}
	if metrics.FlushTick != 0 {
		t.Error("Invalid stats ftick")
	}
	if metrics.FlushErrors != 0 {
		t.Error("Invalid stats ferr")
	}

	if len(b) != 1 {
		t.Error("Invalid batch length")
	}
	if v, ok := b[0].(string); !ok || v != "c" {
		t.Error("Invalid batch elem 0")
	}
}
