package goqueue

import (
	"fmt"
	"os"
	"sync/atomic"
	"unsafe"
)

// snode is a node in a singly-linked list.
type snode struct {
	datum interface{}
	next  *snode
}

// NonBlocking is a multi-reader, multi-writer, first-in-first-out (FIFO) data
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
//        q := goqueue.NewNonBlocking()
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
type NonBlocking struct {
	head, tail *snode
}

// NewNonBlocking returns a non-blocking queue.
func NewNonBlocking() *NonBlocking {
	return new(NonBlocking)
}

// Dequeue extracts and returns the first value at the head of the list. This
// function does not block. When there are no values in the queue, the second
// return value will be false to indicate an empty queue.
//
// This dequeue implementation can be considered lock-free and may spin. At best
// this algorithm performs four simple atomic operations and completes without
// runtime preemption or interference from another concurrent dequeue
// operation. Enqueue operations never interfere with the performance of dequeue
// operations. However, with high enough dequeue contention, a different
// goroutine trying to dequeue may cause this dequeue process to need to execute
// another round of the same four atomic operations before it is able to remove
// the head node from the queue.
func (q *NonBlocking) Dequeue() (interface{}, bool) {
	// q.showHeadAndTail("Dequeue PRE")

	head := (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head))))

	// When head is nil, the queue is empty.
	if head == nil {
		// q.showHeadAndTail("Dequeue POST")
		return nil, false
	}

	// When the tail points to the same node as the head, there remains only a
	// single item in the queue. In this case the head is ours for removal, and
	// we must set both the head and tail to nil so that future enqueue
	// operations will update the head pointer.
	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail)), unsafe.Pointer(head), nil) {
		// An enqueue operation might have already updated head pointer. Attempt
		// to update head, but only if it is still its previous value.
		_ = atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)), unsafe.Pointer(head), nil)
		// q.showHeadAndTail("Dequeue POST")
		return head.datum, true
	}
	// POST: There are one or more items in the queue.

	next := (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next))))

	for !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)), unsafe.Pointer(head), unsafe.Pointer(next)) {
		// When q.head has been updated, advance our head.
		head = (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head))))
		if head == nil {
			// When the new head pointer is nil after a failed CAS, then another
			// goroutine must have removed the last remaining value from the
			// queue.
			// q.showHeadAndTail("Dequeue POST")
			return nil, false
		}
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail)), unsafe.Pointer(head), nil) {
			// An enqueue operation might have already updated head pointer. Attempt
			// to update head, but only if it is still its previous value.
			_ = atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)), unsafe.Pointer(head), nil)
			// q.showHeadAndTail("Dequeue POST")
			return head.datum, true
		}
		next = (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next))))
	}
	// POST: The local head variable now points to the former head of the queue.

	// q.showHeadAndTail("Dequeue POST")
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
func (q *NonBlocking) Enqueue(datum interface{}) {
	if false {
		q.enqueueLessFast(datum)
		return
	}

	// q.showHeadAndTail("Enqueue PRE")
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
	// q.showHeadAndTail("Enqueue POST")
}

// enqueueLessFast appends datum to the tail of the queue. This version of
// enqueue is not as performant.
func (q *NonBlocking) enqueueLessFast(datum interface{}) {
	// q.showHeadAndTail("Enqueue PRE")
	n := &snode{datum: datum}

	// Handle empty list conditions, cautious that another goroutine could be
	// competing to create first node of the list. In most cases the queue is
	// not empty, so handle that case first.
	if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail)), nil, unsafe.Pointer(n)) {
		// When tail is not nil, there is at least one other item in the queue.
		tail := (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail))))

		// Insert node at tail.next, advancing tail as needed, because another
		// goroutine could have just added a node itself, but then lost CPU
		// time before it could update the queue's tail.
		for !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next)), nil, unsafe.Pointer(n)) {
			tail = (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail))))
		}

		// Attempt to update the tail to point to newest node.
		_ = atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail)), unsafe.Pointer(tail), unsafe.Pointer(n))
		// q.showHeadAndTail("Enqueue POST")
		return
	}

	// Whenever the queue is empty, only one goroutine can get here at a time
	// and append an item onto the previously empty queue. Because this go
	// routine was able to update the queue's nil tail, it now has the
	// responsibility to update the queue's head. No dequeue actions will return
	// values until the head is updated.
	//
	// NOTE: Once prove impossible to get here without head being nil, and safe,
	// then
	if false {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)), unsafe.Pointer(n))
	} else {
		if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)), nil, unsafe.Pointer(n)) {
			panic("goroutine which inserts node into empty queue should be able to update head")
		}
	}
	// q.showHeadAndTail("Enqueue POST")
}

// IsEmpty returns true when the queue is empty and false otherwise.
func (q *NonBlocking) IsEmpty() bool {
	// q.showHeadAndTail("IsEmpty")
	return (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail)))) == nil
}

func (q *NonBlocking) showHeadAndTail(op string) {
	fmt.Fprintf(os.Stderr, "nonblocking %s:\n\thead: %v\n\ttail: %v\n", op, (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.head)))), (*snode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.tail)))))
}
