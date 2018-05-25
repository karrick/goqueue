package goqueue

import (
	"math/rand"
	"testing"
)

const oneMillion = 1000000

type queue interface {
	Enqueue(interface{})
	Dequeue() (interface{}, bool)
}

func testQueue(t *testing.T, init func() queue) {
	q := init()

	q.Enqueue(11)
	q.Enqueue(22)
	q.Enqueue(33)
	q.Enqueue(44)

	t.Run("first dequeue", func(t *testing.T) {
		v, ok := q.Dequeue()
		if got, want := v, 11; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("second dequeue", func(t *testing.T) {
		v, ok := q.Dequeue()
		if got, want := v, 22; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("third dequeue", func(t *testing.T) {
		v, ok := q.Dequeue()
		if got, want := v, 33; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("fourth dequeue", func(t *testing.T) {
		v, ok := q.Dequeue()
		if got, want := v, 44; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("fifth dequeue", func(t *testing.T) {
		_, ok := q.Dequeue()
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
}

func benchmarkEnqueueThenDequeue(b *testing.B, init func() queue) {
	values := rand.Perm(b.N)
	q := init()

	b.ReportAllocs()
	b.ResetTimer()

	for _, v := range values {
		q.Enqueue(v)
	}

	for i := 0; i < b.N; i++ {
		if _, ok := q.Dequeue(); !ok {
			b.Fatalf("GOT: %v; WANT: %v", ok, true)
		}
	}
}

func benchmarkInterleaved(b *testing.B, init func() queue) {
	values := rand.Perm(b.N)
	q := init()

	b.ReportAllocs()
	b.ResetTimer()

	for i, v := range values {
		switch i % 10 {
		case 0:
			_, _ = q.Dequeue()
		default:
			q.Enqueue(v)
		}
	}
}

func benchmarkParallel(b *testing.B, init func() queue) {
	values := rand.Perm(oneMillion)
	q := init()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i, v := range values {
				switch i % 10 {
				case 0:
					_, _ = q.Dequeue()
				default:
					q.Enqueue(v)
				}
			}
		}
	})
}

func newQueue() queue {
	return new(Queue)
}

func TestQueue(t *testing.T) {
	testQueue(t, newQueue)
}

func BenchmarkQueueEnqueueThenDequeue(b *testing.B) {
	benchmarkEnqueueThenDequeue(b, newQueue)
}

func BenchmarkQueueInterleaved(b *testing.B) {
	benchmarkInterleaved(b, newQueue)
}

func BenchmarkQueueParallel(b *testing.B) {
	benchmarkParallel(b, newQueue)
}
