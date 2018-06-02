package goqueue

import (
	"testing"

	"github.com/karrick/godeque"
)

func TestLockingList(t *testing.T) {
	t.Run("fills and drains", func(t *testing.T) {
		testNonBlockingFillsAndDrains(t, func() nonBlockingQueue { return godeque.NewLockingList() })
	})
	t.Run("interleaved", func(t *testing.T) {
		testNonBlockingInterleaved(t, func() nonBlockingQueue { return godeque.NewLockingList() })
	})
	t.Run("parallel", func(t *testing.T) {
		combinedNonBlockingParallel(t, func() nonBlockingQueue { return godeque.NewLockingList() })
	})
}

func BenchmarkLockingListParallel(b *testing.B) {
	combinedNonBlockingParallel(b, func() nonBlockingQueue { return godeque.NewLockingList() })
}
