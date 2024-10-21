package common

import "sync"

type WaitGroup struct {
	n     int
	group *sync.WaitGroup
}

func NewWaitGroup(n int) *WaitGroup {
	var wg sync.WaitGroup
	wg.Add(n)

	return &WaitGroup{
		n:     n,
		group: &wg,
	}
}

func (wg *WaitGroup) Wait() {
	wg.group.Wait()
}

func (wg *WaitGroup) Done() {
	wg.group.Done()
}
