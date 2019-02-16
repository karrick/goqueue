package goqueue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func producerConsumerChannel(tb testing.TB, consumerCount, producerCount, queueSize, productionSize int) int {
	var sum int64

	ch := make(chan interface{}, queueSize)

	var consumerGroup, producerGroup sync.WaitGroup
	consumerGroup.Add(consumerCount)
	producerGroup.Add(producerCount)

	// spawn consumer threads
	for i := 0; i < consumerCount; i++ {
		go func(i int) {
			defer consumerGroup.Done()
			for item := range ch {
				atomic.AddInt64(&sum, item.(int64)) // do something with item
			}
		}(i)
	}

	// spawn producer threads
	for i := 0; i < producerCount; i++ {
		go func(i int) {
			defer producerGroup.Done()
			for item := 0; item < productionSize; item++ {
				ch <- int64(i*100 + item)
			}
		}(i)
	}

	// Once all the producers are done, tell the consumers to terminate.
	producerGroup.Wait()
	close(ch)
	consumerGroup.Wait()

	return int(atomic.LoadInt64(&sum))
}

func TestProducerConsumerChannel(t *testing.T) {
	if got, want := producerConsumerChannel(t, lowConsumerCount, lowProducerCount, lowQueueSize, lowProductionSize), lowTotal; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

const (
	lowConsumerCount  = 3
	lowProducerCount  = 3
	lowQueueSize      = 5
	lowProductionSize = 10
	lowTotal          = 3135

	mediumConsumerCount  = 10
	mediumProducerCount  = 10
	mediumQueueSize      = 10
	mediumProductionSize = 100
	mediumTotal          = 499500

	highConsumerCount  = 100
	highProducerCount  = 100
	highQueueSize      = 100
	highProductionSize = 100
	highTotal          = 49995000

	veryHighConsumerCount  = 1000
	veryHighProducerCount  = 1000
	veryHighQueueSize      = 1000
	veryHighProductionSize = 1000
	veryHighTotal          = 50449500000
)

func BenchmarkProducerConsumerLowChannel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerChannel(b, lowConsumerCount, lowProducerCount, lowQueueSize, lowProductionSize), lowTotal; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func BenchmarkProducerConsumerMediumChannel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerChannel(b, mediumConsumerCount, mediumProducerCount, mediumQueueSize, mediumProductionSize), mediumTotal; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func BenchmarkProducerConsumerHighChannel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerChannel(b, highConsumerCount, highProducerCount, highQueueSize, highProductionSize), highTotal; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func BenchmarkProducerConsumerVeryHighChannel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerChannel(b, veryHighConsumerCount, veryHighProducerCount, veryHighQueueSize, veryHighProductionSize), veryHighTotal; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func producerConsumerQueue(tb testing.TB, consumerCount, producerCount, queueSize, productionSize int) int {
	var sum int64

	queue, err := newQ3(queueSize)
	if err != nil {
		tb.Fatal(err)
	}

	var consumerGroup, producerGroup sync.WaitGroup
	consumerGroup.Add(consumerCount)
	producerGroup.Add(producerCount)

	// spawn consumer threads
	for i := 0; i < consumerCount; i++ {
		go func(i int) {
			defer consumerGroup.Done()
			for {
				item, ok := queue.Dequeue()
				if !ok {
					return
				}
				atomic.AddInt64(&sum, item.(int64)) // do something with item
			}
		}(i)
	}

	// spawn producer threads
	for i := 0; i < producerCount; i++ {
		go func(i int) {
			defer producerGroup.Done()
			for item := 0; item < productionSize; item++ {
				queue.Enqueue(int64(i*100 + item))
			}
		}(i)
	}

	producerGroup.Wait() // wait for producers to complete
	queue.Close()        // close the queue
	consumerGroup.Wait() // wait for the consumers to complete
	return int(atomic.LoadInt64(&sum))
}

func TestProducerConsumerQueue(t *testing.T) {
	if got, want := producerConsumerQueue(t, lowConsumerCount, lowProducerCount, lowQueueSize, lowProductionSize), lowTotal; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func BenchmarkProducerConsumerLowQueue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerQueue(b, lowConsumerCount, lowProducerCount, lowQueueSize, lowProductionSize), lowTotal; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func BenchmarkProducerConsumerMediumQueue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerQueue(b, mediumConsumerCount, mediumProducerCount, mediumQueueSize, mediumProductionSize), mediumTotal; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func BenchmarkProducerConsumerHighQueue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerQueue(b, highConsumerCount, highProducerCount, highQueueSize, highProductionSize), highTotal; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func BenchmarkProducerConsumerVeryHighQueue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerQueue(b, veryHighConsumerCount, veryHighProducerCount, veryHighQueueSize, veryHighProductionSize), veryHighTotal; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

type q3 struct {
	items []interface{}
	cv    *sync.Cond
	max   int // size is set to -1 after the queue is closed
}

func newQ3(size int) (*q3, error) {
	if size <= 0 {
		return nil, fmt.Errorf("cannot create queue with size less than or equal to 0: %d", size)
	}
	q := &q3{
		items: make([]interface{}, 0, size),
		cv:    &sync.Cond{L: new(sync.Mutex)},
		max:   size,
	}
	return q, nil
}

func (q *q3) GoString() string {
	if q.max < 0 {
		return fmt.Sprintf("closed; length: %d; items: %#v", len(q.items), q.items)
	}
	return fmt.Sprintf("open; length: %d; items: %#v", len(q.items), q.items)
}

func (q *q3) Close() error {
	q.cv.L.Lock()
	if q.max < 0 {
		q.cv.L.Unlock()
		panic("close on closed queue")
	}
	q.max = -1
	q.cv.L.Unlock()
	q.cv.Broadcast() // wake up all consumers so they can exit
	return nil
}

// Dequeue will return the next item available from the queue, blocking until an
// item is available or the queue is closed.  When an item is available, this
// returns the item as its first return value and true as the second.  When the
// queue is closed, this returns nil as the first argument and false as its
// second.  This mirrors the behavior of a receive from a nil channel.
func (q *q3) Dequeue() (interface{}, bool) {
	q.cv.L.Lock()

	// If either the queue is closed or there are items available, then stop
	// waiting.  By DeMorgan's Theorum, continue to wait while both queue is not
	// closed and there are no items available.
	for q.max > 0 && len(q.items) == 0 {
		q.cv.Wait()
	}

	// It might be tempting to test whether queue is open right now, but that
	// would introduce a subtle bug, because a producer may have enqueued a
	// bunch of items onto the queue and immediately closed it.  When there are
	// items in the queue, return them.  Otherwise if we get here and the queue
	// is empty, the queue must be closed.
	if len(q.items) > 0 {
		var item interface{}
		item, q.items = q.items[0], q.items[1:]
		q.cv.L.Unlock() // release our lock, then
		q.cv.Signal()   // wake up the next go-routine waiting on this lock
		return item, true
	}

	// If here, the queue must be closed.
	q.cv.L.Unlock() // release our lock, then
	q.cv.Signal()   // wake up the next go-routine waiting on this lock
	return nil, false
}

// Enqueue inserts an item on the end of the queue, blocking until a vacancy is
// available on the queue.  If called when the queue is closed it will panic,
// indicating a bug in the design of the code that uses it.  This mirrors the
// behavior of a send to a closed channel.
func (q *q3) Enqueue(item interface{}) {
	q.cv.L.Lock()
	for {
		if q.max < 0 {
			panic("enqueue on closed queue")
		}
		if len(q.items) < q.max {
			q.items = append(q.items, item)
			q.cv.L.Unlock() // release our lock, then
			q.cv.Signal()   // wake up the next go-routine waiting on this lock
			return
		}
		q.cv.Wait()
	}
}
