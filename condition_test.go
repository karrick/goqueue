package goqueue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func producerConsumerChannel(tb testing.TB, consumerCount, producerCount, queueSize, productionSize int) int {
	var sum int32

	ch := make(chan interface{}, queueSize)

	var consumerGroup, producerGroup sync.WaitGroup
	consumerGroup.Add(consumerCount)
	producerGroup.Add(producerCount)

	// spawn consumer threads
	for i := 0; i < consumerCount; i++ {
		go func(i int) {
			defer consumerGroup.Done()
			for item := range ch {
				atomic.AddInt32(&sum, item.(int32)) // do something with item
			}
		}(i)
	}

	// spawn producer threads
	for i := 0; i < producerCount; i++ {
		go func(i int) {
			defer producerGroup.Done()
			for item := 0; item < productionSize; item++ {
				ch <- int32(i*100 + item)
			}
		}(i)
	}

	// Once all the producers are done, tell the consumers to terminate.
	producerGroup.Wait()
	close(ch)
	consumerGroup.Wait()

	return int(atomic.LoadInt32(&sum))
}

func TestProducerConsumerChannel(t *testing.T) {
	if got, want := producerConsumerChannel(t, 3, 3, 10, 10), 3135; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func BenchmarkProducerConsumerChannel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerChannel(b, 3, 3, 10, 10), 3135; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

func producerConsumerQueue(tb testing.TB, consumerCount, producerCount, queueSize, productionSize int) int {
	var sum int32

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
				atomic.AddInt32(&sum, item.(int32)) // do something with item
			}
		}(i)
	}

	// spawn producer threads
	for i := 0; i < producerCount; i++ {
		go func(i int) {
			defer producerGroup.Done()
			for item := 0; item < productionSize; item++ {
				queue.Enqueue(int32(i*100 + item))
			}
		}(i)
	}

	producerGroup.Wait() // wait for producers to complete
	queue.Close()        // close the queue
	consumerGroup.Wait() // wait for the consumers to complete
	return int(atomic.LoadInt32(&sum))
}

func TestProducerConsumerQueue(t *testing.T) {
	if got, want := producerConsumerQueue(t, 3, 3, 10, 10), 3135; got != want {
		// if got, want := producerConsumerQueue(t, 1, 1, 3, 10), 45; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func BenchmarkProducerConsumerQueue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if got, want := producerConsumerQueue(b, 3, 3, 10, 10), 3135; got != want {
			b.Errorf("GOT: %v; WANT: %v", got, want)
		}
	}
}

type q3 struct {
	items []interface{}
	cv    *sync.Cond
	size  int // size is set to -1 after the queue is closed
}

func newQ3(size int) (*q3, error) {
	if size <= 0 {
		return nil, fmt.Errorf("cannot create queue with size less than or equal to 0: %d", size)
	}
	q := &q3{
		items: make([]interface{}, 0, size),
		cv:    &sync.Cond{L: new(sync.Mutex)},
		size:  size,
	}
	return q, nil
}

func (q *q3) GoString() string {
	if q.size < 0 {
		return fmt.Sprintf("closed; length: %d; items: %#v", len(q.items), q.items)
	}
	return fmt.Sprintf("open; length: %d; items: %#v", len(q.items), q.items)
}

func (q *q3) Close() error {
	q.cv.L.Lock()
	if q.size < 0 {
		q.cv.L.Unlock()
		panic("close on closed queue")
	}
	q.size = -1
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
	for q.size > 0 && len(q.items) == 0 {
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
		if q.size < 0 {
			panic("enqueue on closed queue")
		}
		if len(q.items) < q.size {
			q.items = append(q.items, item)
			q.cv.L.Unlock() // release our lock, then
			q.cv.Signal()   // wake up the next go-routine waiting on this lock
			return
		}
		q.cv.Wait()
	}
}
