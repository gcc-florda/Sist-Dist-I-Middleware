package business

import (
	"container/heap"
	"middleware/worker/schema"
)

type ReviewWithSource struct {
	Review schema.NamedReviewCounter
	Index  int
}

type MinHeap []ReviewWithSource

func NewHeap() *MinHeap {
	h := &MinHeap{}
	heap.Init(h)
	return h
}

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool {
	return h[i].Review.Count < h[j].Review.Count
}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(ReviewWithSource))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
