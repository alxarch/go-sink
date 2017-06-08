package sink_test

import (
	"testing"
	"time"

	sink "github.com/alxarch/go-sink"
)

func Test_Queue(t *testing.T) {

	done := make(chan []interface{}, 1)
	f := sink.FlushFunc(func(b []interface{}) error {
		done <- b
		return nil
	})
	q := sink.New(f, 2, 5*time.Second)

	q.Push("a")
	q.Push("b")
	q.Push("c")
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
