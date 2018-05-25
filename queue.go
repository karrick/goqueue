package goqueue

import (
	"sync/atomic"
	"unsafe"
)

// snode is a node in a singly-linked list.
type snode struct {
	next  *snode
	datum interface{}
}

// Queue is a multi-reader, multi-writer, first-in-first-out (FIFO) data
// structure that allows insertion and removal of values safely by multiple
// goroutines.
//
// This implementation is implemented as a lock-free singly-linked list. It
// enqueues by appending a new value to the tail and dequeues by removing the
// value pointed to by the head.
//
//    func ExampleQueue() {
//        const oneMillion = 1000000
//
//        values := rand.Perm(oneMillion)
//        q := new(goqueue.Queue)
//
//        for _, v := range values {
//            q.Enqueue(v)
//        }
//
//        for i := 0; i < len(values); i++ {
//            v, ok := q.Dequeue()
//            if !ok {
//                fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", ok, true)
//                os.Exit(1)
//            }
//            if got, want := v, values[i]; got != want {
//                fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", got, want)
//            }
//        }
//    }
type Queue struct {
	head, tail *snode
}

// Dequeue extracts and returns the first value at the head of the list.
//
// This dequeue implementation can be considered lock-free and may spin. At best
// this algorithm performs three simple atomic operations and completes without
// runtime preemption or interference from another concurrent dequeue
// operation. Enqueue operations never interfere with the performance of dequeue
// operations. However, with high enough dequeue contention, a different
// goroutine trying to dequeue may cause this dequeue process to need to execute
// another round of the same three atomic operations before it is able to remove
// the head node from the queue.
func (q *Queue) Dequeue() (interface{}, bool) {
	// Handle empty list condition.
	head := (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head))))
	if head == nil {
		return nil, false
	}

	// NOTE: head.next will be nil when only a single node in list.
	next := (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next))))

	for !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)), unsafe.Pointer(head), unsafe.Pointer(next)) {
		// When q.head has been updated, advance our head.
		newHead := (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head))))
		if newHead == nil {
			// When the new head pointer is nil after a failed CAS, then another
			// goroutine must have removed the last remaining value from the
			// queue. Multiple goroutines can get here simultaneously, but only
			// one of them needs to set the tail pointer to nil. The goroutine
			// which has matching head and tail pointers will set the tail to
			// nil to ensure only a single goroutine does it.
			_ = atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail)), unsafe.Pointer(head), nil)
			// In any case, when the new head pointer is nil, there are no
			// remaining values in the queue.
			return nil, false
		}
		head = newHead
		next = (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next))))
	}
	// POST: The local head variable now points to the former head of the queue.
	return head.datum, true
}

// Enqueue appends datum to the tail of the queue.
//
// This enqueue implementation never spins and by itself can be considered
// wait-free. No other concurrent enqueue or dequeue operation will adversely
// affect how long this enqueue will take. However, there remains a chance that
// the runtime scheduler may preempt this goroutine.
//
// If and only if the runtime preempts this goroutine after the atomic pointer
// swap and before the atomic store pointer, then dequeue operations that take
// place afterwards will neither see this node nor any additional nodes that are
// enqueued until this goroutine completes the final atomic pointer store. Even
// in this case, however, nodes that were part of the queue prior to the atomic
// pointer swap will remain visible to any dequeue operation.
//
// Benchmarks against this algorithm and a lock-free alternative enqueue
// algorithm at various levels of concurrency reveal the overall system
// performance improves significantly with this approach rather than the
// lock-free approach.
func (q *Queue) Enqueue(datum interface{}) {
	// For now, assume tail was nil with an empty queue. When the queue is not
	// in fact empty, the condition below will update this handle to point to
	// the pointer of the next field of the old tail.
	npp := &q.head

	// Make a new node to hold this value.
	n := &snode{datum: datum}

	ot := (*snode)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail)), unsafe.Pointer(n)))
	if ot != nil {
		// The queue was in fact not empty. Rather than updating queue's head to
		// point to this new node, update the old tail's next pointer to point
		// to this new node.
		npp = &ot.next
	}

	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(npp)), unsafe.Pointer(n))
}
