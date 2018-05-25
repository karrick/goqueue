# goqueue : Queue is a lock-free first-in-first-out (FIFO) data structure

## Overview [![GoDoc](https://godoc.org/github.com/karrick/goqueue?status.svg)](https://godoc.org/github.com/karrick/goqueue)

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
	q := new(goqueue.Queue)

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
