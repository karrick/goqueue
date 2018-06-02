package goqueue

import (
	"testing"
)

func TestNonBlocking(t *testing.T) {
	t.Run("fills and drains", func(t *testing.T) {
		testNonBlockingFillsAndDrains(t, func() nonBlockingQueue { return NewNonBlocking() })
	})
	t.Run("interleaved", func(t *testing.T) {
		testNonBlockingInterleaved(t, func() nonBlockingQueue { return NewNonBlocking() })
	})
	t.Run("parallel", func(t *testing.T) {
		combinedNonBlockingParallel(t, func() nonBlockingQueue { return NewNonBlocking() })
	})
}

func BenchmarkNonBlockingParallel(b *testing.B) {
	combinedNonBlockingParallel(b, func() nonBlockingQueue { return NewNonBlocking() })
}
