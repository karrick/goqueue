package goqueue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type queue interface {
	Close() error
	Dequeue() (interface{}, bool)
	Enqueue(interface{})
}

func runQ(tb testing.TB, q queue, consumerCount, producerCount, itemsPerProducer, want int) {
	var sum int64

	var consumerGroup, producerGroup sync.WaitGroup
	consumerGroup.Add(consumerCount)
	producerGroup.Add(producerCount)

	// spawn consumer threads
	for i := 0; i < consumerCount; i++ {
		go func(i int) {
			defer consumerGroup.Done()
			for {
				item, ok := q.Dequeue()
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
			for item := 0; item < itemsPerProducer; item++ {
				q.Enqueue(int64(i*100 + item))
			}
		}(i)
	}

	producerGroup.Wait() // wait for producers to complete
	q.Close()            // close the queue
	consumerGroup.Wait() // wait for the consumers to complete

	if got := int(atomic.LoadInt64(&sum)); got != want {
		tb.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func BenchmarkProducerConsumer(b *testing.B) {
	b.Run("low", func(b *testing.B) {
		const consumers = 10
		const producers = 10
		const queueSize = 10
		const itemsPerProducer = 10
		const total = 45450
		b.Run("channel", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q, err := newQueueChannel(queueSize)
				if err != nil {
					b.Fatal(err)
				}
				runQ(b, q, consumers, producers, itemsPerProducer, total)
			}
		})
		b.Run("queue3", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q, err := newQueue3(queueSize)
				if err != nil {
					b.Fatal(err)
				}
				runQ(b, q, consumers, producers, itemsPerProducer, total)
			}
		})
	})
	b.Run("medium", func(b *testing.B) {
		const consumers = 100
		const producers = 100
		const queueSize = 100
		const itemsPerProducer = 100
		const total = 49995000
		b.Run("channel", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q, err := newQueueChannel(queueSize)
				if err != nil {
					b.Fatal(err)
				}
				runQ(b, q, consumers, producers, itemsPerProducer, total)
			}
		})
		b.Run("queue3", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q, err := newQueue3(queueSize)
				if err != nil {
					b.Fatal(err)
				}
				runQ(b, q, consumers, producers, itemsPerProducer, total)
			}
		})
	})
	b.Run("high", func(b *testing.B) {
		const consumers = 1000
		const producers = 1000
		const queueSize = 1000
		const itemsPerProducer = 1000
		const total = 50449500000
		b.Run("channel", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q, err := newQueueChannel(queueSize)
				if err != nil {
					b.Fatal(err)
				}
				runQ(b, q, consumers, producers, itemsPerProducer, total)
			}
		})
		b.Run("queue3", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q, err := newQueue3(queueSize)
				if err != nil {
					b.Fatal(err)
				}
				runQ(b, q, consumers, producers, itemsPerProducer, total)
			}
		})
	})
}

////////////////////////////////////////

type queueChannel struct {
	ch chan interface{}
}

func newQueueChannel(size int) (*queueChannel, error) {
	if size <= 0 {
		return nil, fmt.Errorf("cannot create queue with size less than or equal to 0: %d", size)
	}
	q := &queueChannel{ch: make(chan interface{}, size)}
	return q, nil
}

func (q *queueChannel) Close() error {
	close(q.ch)
	return nil
}

func (q *queueChannel) Dequeue() (interface{}, bool) {
	item, ok := <-q.ch
	return item, ok
}

func (q *queueChannel) Enqueue(item interface{}) {
	q.ch <- item
}

func TestProducerConsumerChannel(t *testing.T) {
	q, err := newQueueChannel(3)
	if err != nil {
		t.Fatal(err)
	}
	runQ(t, q, 3, 3, 3, 909)
}

////////////////////////////////////////

type queue3 struct {
	items []interface{}
	cv    *sync.Cond
	max   int // size is set to -1 after the queue is closed
}

func newQueue3(size int) (*queue3, error) {
	if size <= 0 {
		return nil, fmt.Errorf("cannot create queue with size less than or equal to 0: %d", size)
	}
	q := &queue3{
		items: make([]interface{}, 0, size),
		cv:    &sync.Cond{L: new(sync.Mutex)},
		max:   size,
	}
	return q, nil
}

func (q *queue3) Close() error {
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
func (q *queue3) Dequeue() (interface{}, bool) {
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
func (q *queue3) Enqueue(item interface{}) {
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

func (q *queue3) GoString() string {
	if q.max < 0 {
		return fmt.Sprintf("closed; length: %d; items: %#v", len(q.items), q.items)
	}
	return fmt.Sprintf("open; length: %d; items: %#v", len(q.items), q.items)
}

func TestProducerConsumerQueue3(t *testing.T) {
	q, err := newQueue3(3)
	if err != nil {
		t.Fatal(err)
	}
	runQ(t, q, 3, 3, 3, 909)
}
