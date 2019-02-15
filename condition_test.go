package goqueue

import (
	"fmt"
	"os"
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

const debugging = true

type q3 struct {
	items       []interface{}
	cv          *sync.Cond
	count, size int
	closed      bool
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

func (q *q3) String() string {
	if q.closed {
		return fmt.Sprintf("closed; length: %d; items: %#v", q.count, q.items)
	}
	return fmt.Sprintf("open; length: %d; items: %#v", q.count, q.items)
}

func (q *q3) Close() error {
	q.cv.L.Lock()
	if q.closed {
		q.cv.L.Unlock()
		panic("close on closed queue")
	}
	if debugging {
		fmt.Fprintf(os.Stderr, "CLOSE: %s\n", q.String())
	}
	q.closed = true
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
	if debugging {
		fmt.Fprintf(os.Stderr, "DEQUEUE: getting lock\n")
	}
	q.cv.L.Lock()
	if debugging {
		fmt.Fprintf(os.Stderr, "DEQUEUE: have lock\n")
	}

	// If either the queue is closed or there are items available, then stop
	// waiting.  By DeMorgan's Theorum, continue to wait while both queue is not
	// closed and there are no items available.
	for !q.closed && q.count == 0 {
		if debugging {
			fmt.Fprintf(os.Stderr, "DEQUEUE: waiting\n")
		}
		q.cv.Wait()
		if debugging {
			fmt.Fprintf(os.Stderr, "DEQUEUE: done waiting\n")
		}
	}

	if debugging {
		fmt.Fprintf(os.Stderr, "DEQUEUE: %s\n", q.String())
	}

	// It might be tempting to test whether queue is open right now, but that
	// would introduce a subtle bug, because a producer may have enqueued a
	// bunch of items onto the queue and immediately closed it.  When there are
	// items in the queue, return them.  Otherwise if we get here and the queue
	// is empty, the queue must be closed.
	if q.count > 0 {
		var item interface{}
		item, q.items = q.items[0], q.items[1:]
		q.count--
		if debugging {
			fmt.Fprintf(os.Stderr, "DEQUEUE: returning %T %#v\n", item, item)
			fmt.Fprintf(os.Stderr, "DEQUEUE: releasing lock\n")
		}
		q.cv.L.Unlock() // release our lock, then
		q.cv.Signal()   // wake up the next go-routine waiting on this lock
		return item, true
	}

	// If here, the queue must be closed.
	if debugging {
		fmt.Fprintf(os.Stderr, "DEQUEUE: releasing lock\n")
	}
	q.cv.L.Unlock() // release our lock, then
	q.cv.Signal()   // wake up the next go-routine waiting on this lock
	return nil, false
}

// Enqueue inserts an item on the end of the queue, blocking until a vacancy is
// available on the queue.  If called when the queue is closed it will panic,
// indicating a bug in the design of the code that uses it.  This mirrors the
// behavior of a send to a closed channel.
func (q *q3) Enqueue(item interface{}) {
	if debugging {
		fmt.Fprintf(os.Stderr, "ENQUEUE: getting lock\n")
	}
	q.cv.L.Lock()
	if debugging {
		fmt.Fprintf(os.Stderr, "ENQUEUE: have lock\n")
	}
	for {
		if q.closed {
			panic("enqueue on closed queue")
		}
		if q.count < q.size {
			q.items = append(q.items, item)
			q.count++
			if debugging {
				fmt.Fprintf(os.Stderr, "ENQUEUE: %s\n", q.String())
			}
			q.cv.L.Unlock() // release our lock, then
			q.cv.Signal()   // wake up the next go-routine waiting on this lock
			return
		}
		if debugging {
			fmt.Fprintf(os.Stderr, "ENQUEUE: waiting\n")
		}
		q.cv.Wait()
		if debugging {
			fmt.Fprintf(os.Stderr, "ENQUEUE: done waiting\n")
		}
	}
}
