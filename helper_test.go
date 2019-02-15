package goqueue

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
)

const oneMillion = 1000000

const benchmarkConcurrency = 137
const testConcurrency = 13
const testLoops = 1000

type blockingQueue interface {
	Enqueue(interface{})
	Dequeue() interface{}
}

type nonBlockingQueue interface {
	Enqueue(interface{})
	Dequeue() (interface{}, bool)
}

////////////////////////////////////////

type lockingSlice struct {
	lock   sync.RWMutex
	values []int
}

func (ls *lockingSlice) Append(datum int) {
	ls.lock.Lock()
	ls.values = append(ls.values, datum)
	ls.lock.Unlock()
}

func (ls *lockingSlice) Len() int {
	ls.lock.RLock()
	l := len(ls.values)
	ls.lock.RUnlock()
	return l
}

////////////////////////////////////////

func testNonBlockingFillsAndDrains(t *testing.T, init func() nonBlockingQueue) {
	q := init()

	t.Run("first cycle", func(t *testing.T) {
		q.Enqueue(11)
		q.Enqueue(22)

		v, ok := q.Dequeue()
		if got, want := v, 11; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		v, ok = q.Dequeue()
		if got, want := v, 22; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		_, ok = q.Dequeue()
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("second cycle", func(t *testing.T) {
		q.Enqueue(33)
		q.Enqueue(44)

		v, ok := q.Dequeue()
		if got, want := v, 33; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		v, ok = q.Dequeue()
		if got, want := v, 44; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		_, ok = q.Dequeue()
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
}

func testNonBlockingInterleaved(t *testing.T, init func() nonBlockingQueue) {
	q := init()
	var list []int
	var wg sync.WaitGroup

	t.Run("first cycle", func(t *testing.T) {
		wg.Add(2)

		go func() {
			// produce 3 items
			for i := 0; i < 3; i++ {
				q.Enqueue(i)
			}
			wg.Done()
		}()

		go func() {
			// consume 3 items
			for len(list) < 3 {
				v, ok := q.Dequeue()
				if ok {
					list = append(list, v.(int))
				}
			}
			wg.Done()
		}()

		wg.Wait()

		if got, want := len(list), 3; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := list[0], 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := list[1], 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := list[2], 2; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})

	t.Run("second cycle", func(t *testing.T) {
		wg.Add(2)

		go func() {
			// produce 3 items
			for i := 0; i < 3; i++ {
				q.Enqueue(i)
			}
			wg.Done()
		}()

		go func() {
			// consume 3 items
			for len(list) < 3 {
				v, ok := q.Dequeue()
				if ok {
					list = append(list, v.(int))
				}
			}
			wg.Done()
		}()

		wg.Wait()

		if got, want := len(list), 3; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := list[0], 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := list[1], 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := list[2], 2; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
}

func combinedNonBlockingParallel(tb testing.TB, init func() nonBlockingQueue) {
	tb.Helper()
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
			for ls.Len() < len(values) {
				if v, ok := q.Dequeue(); ok {
					ls.Append(v.(int))
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	// Each number was placed in queue once. Ensure all values were moved from
	// queue to locked slice of values.
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
