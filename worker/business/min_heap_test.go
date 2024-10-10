package business_test

import (
	"container/heap"
	"middleware/worker/business"
	"testing"
)

func TestMinHeap_Push(t *testing.T) {
	h := business.NewHeap()

	heap.Push(h, business.ReviewWithSource{Review: business.NamedReviewCounter{Count: 5}})
	heap.Push(h, business.ReviewWithSource{Review: business.NamedReviewCounter{Count: 3}})
	heap.Push(h, business.ReviewWithSource{Review: business.NamedReviewCounter{Count: 8}})

	if h.Len() != 3 {
		t.Fatalf("Expected 3, got %d elements pushed", h.Len())
	}

	if (*h)[0].Review.Count != 3 {
		t.Fatalf("Expected 3, got %d", (*h)[0].Review.Count)
	}
}

func TestMinHeap_Pop(t *testing.T) {
	h := business.NewHeap()

	heap.Push(h, business.ReviewWithSource{Review: business.NamedReviewCounter{Count: 5}})
	heap.Push(h, business.ReviewWithSource{Review: business.NamedReviewCounter{Count: 3}})
	heap.Push(h, business.ReviewWithSource{Review: business.NamedReviewCounter{Count: 8}})

	min := heap.Pop(h).(business.ReviewWithSource)
	if min.Review.Count != 3 {
		t.Fatalf("Expected 3, got %d", min.Review.Count)
	}

	min = heap.Pop(h).(business.ReviewWithSource)
	if min.Review.Count != 5 {
		t.Fatalf("Expected 5, got %d", min.Review.Count)
	}

	min = heap.Pop(h).(business.ReviewWithSource)
	if min.Review.Count != 8 {
		t.Fatalf("Expected 8, got %d", min.Review.Count)
	}
}
