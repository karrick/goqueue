package goqueue

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBlockingEnqueuesThenDequeues(t *testing.T) {
	q := NewQueue()

	t.Run("enqueues", func(t *testing.T) {
		q.Enqueue(11)
		q.Enqueue(22)
		q.Enqueue(33)
	})

	t.Run("first dequeue", func(t *testing.T) {
		v := q.Dequeue()
		if got, want := v, 11; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("second dequeue", func(t *testing.T) {
		v := q.Dequeue()
		if got, want := v, 22; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("third dequeue", func(t *testing.T) {
		v := q.Dequeue()
		if got, want := v, 33; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
}

func TestBlockingInterleaved(t *testing.T) {
	// t.Skip()
	q := NewQueue()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		t.Run("enqueues", func(t *testing.T) {
			q.Enqueue(11)
			q.Enqueue(22)
			q.Enqueue(33)
		})
		wg.Done()
	}()

	go func() {
		time.Sleep(time.Millisecond)
		q.Enqueue(44)
		wg.Done()
	}()

	go func() {
		t.Run("first dequeue", func(t *testing.T) {
			v := q.Dequeue()
			if got, want := v, 11; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})

		t.Run("second dequeue", func(t *testing.T) {
			v := q.Dequeue()
			if got, want := v, 22; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})

		t.Run("third dequeue", func(t *testing.T) {
			v := q.Dequeue()
			if got, want := v, 33; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})

		t.Run("fourth dequeue", func(t *testing.T) {
			v := q.Dequeue()
			if got, want := v, 44; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})

		wg.Done()
	}()

	wg.Wait()
}

func combinedBlockingParallel(tb testing.TB, init func() blockingQueue) {
	q := init()
	ls := new(lockingSlice)
	concurrency := testConcurrency
	loops := testLoops

	if b, ok := tb.(*testing.B); ok {
		concurrency = benchmarkConcurrency
		loops = b.N
		// tb.Logf("%s: concurrency(%d) loops(%d)\n", tb.Name(), concurrency, loops)
		b.ReportAllocs()
		b.ResetTimer()
	}

	values := rand.Perm(loops)

	var wg sync.WaitGroup
	wg.Add(2 * concurrency)

	var dequeuesRemaining int64

	for i := 0; i < concurrency; i++ {
		// producers add to the queue
		go func(i int) {
			for j := i; j < len(values); j += concurrency {
				q.Enqueue(values[j])
			}
			wg.Done()
		}(i)

		// consumers remove from queue and append to locking list
		go func() {
			for atomic.AddInt64(&dequeuesRemaining, 1) <= int64(loops) {
				ls.Append(q.Dequeue().(int))
			}
			wg.Done()
		}()
	}

	wg.Wait()

	// Each number was placed in queue once. Ensure all values were moved from
	// queue to locked slice.
	if got, want := len(ls.values), len(values); got != want {
		tb.Fatalf("GOT: %v; WANT: %v", got, want)
	}
	sort.Ints(ls.values)
	for i := 0; i < len(ls.values); i++ {
		if got, want := ls.values[i], i; got != want {
			tb.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func TestBlockingParallel(t *testing.T) {
	combinedBlockingParallel(t, func() blockingQueue { return NewQueue() })
}

func BenchmarkBlockingParallel(b *testing.B) {
	combinedBlockingParallel(b, func() blockingQueue { return NewQueue() })
}
