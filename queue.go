package goqueue

import (
	"sync"
	"sync/atomic"
)

// Queue is a multi-reader, multi-writer, first-in-first-out (FIFO) data
// structure that allows insertion and removal of values safely by multiple
// goroutines. The structure is named "Queue" because the dequeue operation will
// block until a datum is available when the queue is empty.
//
// This implementation is implemented using a lock-free singly-linked list. It
// enqueues by appending a new value to the tail and dequeues by removing the
// value pointed to by the head.
//
//    func ExampleQueue() {
//        const oneMillion = 1000000
//
//        values := rand.Perm(oneMillion)
//        q := goqueue.NewQueue()
//
//        for _, v := range values {
//            q.Enqueue(v)
//        }
//
//        for i := 0; i < len(values); i++ {
//            v := q.Dequeue()
//            if got, want := v, values[i]; got != want {
//                fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", got, want)
//            }
//        }
//    }
type Queue struct {
	// head is the sequence number of the goroutine whose turn it is to dequeue.
	head int64

	// tail is the sequence number of the goroutine which last appended itself
	// to the queue of goroutines waiting to dequeue a datum value.
	tail int64

	c *sync.Cond   // c is the condition variable used to synchronize dequeue goroutines.
	q *QueueNoWait // q is the queue of datum values waiting to be dequeued.
}

const nobody uint64 = 0 // nobody is using the queue

// NewQueue returns a queue whose dequeue operation waits until a value is
// available.
func NewQueue() *Queue {
	return &Queue{
		tail: -1,
		c:    sync.NewCond(new(sync.Mutex)),
		q:    new(QueueNoWait),
	}
}

// Dequeue returns the first datum in the queue, or, when the queue is empty,
// will wait until a datum is available.
func (b *Queue) Dequeue() interface{} {
	id := atomic.AddInt64(&b.tail, 1)

	// Wait for this goroutine's id to show up.
	b.c.L.Lock()
	for atomic.LoadInt64(&b.head) != id || b.q.IsEmpty() {
		b.c.Wait() // unlocks; waits for signal or broadcast; relocks before return
	}

	// This goroutine's turn to dequeue the next datum value.
	datum, ok := b.q.Dequeue()
	if !ok {
		panic("nothing to dequeue")
	}

	_ = atomic.AddInt64(&b.head, 1) // pass turn to following dequeue goroutine
	b.c.L.Unlock()
	b.c.Broadcast()
	return datum
}

// Enqueue appends datum to the tail of the queue. This operation is
// non-blocking, and can be considered wait-free because it completes in a
// finite number of steps regardless of any other queue operations.
func (b *Queue) Enqueue(datum interface{}) {
	b.q.Enqueue(datum)
	b.c.Broadcast()
}
