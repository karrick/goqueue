# goqueue

goqueue is a lock-free first-in-first-out (FIFO) data structure.

I believe this library is correct, and prevents go-routine thread
starvation when under heavy concurrency, but is not as performant as
using a simple mutex to provide concurrency protection around another
data structure as the `LockingList` data structure from
[https://github.com/karrick/godeque](https://github.com/karrick/godeque).

I view this library as an experiment to figure out how to provide a
more performant lock-free queue than a locking queue. I think that the
Enqueue operation is implemented as wait-free, but the Dequeue
operation remains simply lock-free. One of these days I hope to figure
it out.

There are several data structures in this library that behave slightly
differently. All of them are lock-free implementations, however the
difference is how they behave when a client attempts to Dequeue from
an empty queue. The `Blocking` queue will block waiting for the next
item to be available, while the `NonBlocking` queue returns
immediately with the second return value set to false to indicate an
empty queue. Even in the `Blocking` queue, the library avoids
spinning, and uses sync.Cond to provide notification primitives to
signal when items are available.

## Overview [![GoDoc](https://godoc.org/github.com/karrick/goqueue?status.svg)](https://godoc.org/github.com/karrick/goqueue)

### Queue whose Dequeue method waits for an item to be available.

```Go
package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/karrick/goqueue"
)

func main() {
	const oneMillion = 1000000

	values := rand.Perm(oneMillion)
	q := goqueue.NewQueue()

	for _, v := range values {
		q.Enqueue(v)
	}

	for i := 0; i < len(values); i++ {
		v := q.Dequeue()
		if got, want := v, values[i]; got != want {
			fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", got, want)
		}
	}
}
```

### Queue whose Dequeue method does not wait for an item to be available.

```Go
package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/karrick/goqueue"
)

func main() {
	const oneMillion = 1000000

	values := rand.Perm(oneMillion)
	q := goqueue.NewQueueNoWait()

	for _, v := range values {
		q.Enqueue(v)
	}

	for i := 0; i < len(values); i++ {
		v, ok := q.Dequeue()
		if !ok {
			fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", ok, true)
			os.Exit(1)
		}
		if got, want := v, values[i]; got != want {
			fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", got, want)
		}
	}
}
```

## Install

```
go get github.com/karrick/goqueue
```

## License

MIT.
