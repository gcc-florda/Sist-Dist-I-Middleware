package main

import (
	"middleware/common"
	"net"
	"strings"
	"sync"
)

type WorkerStatus struct {
	mutex      sync.RWMutex
	Name       string
	Alive      bool
	Connection net.Conn
}

func NewWorkerStatus(name string) *WorkerStatus {
	return &WorkerStatus{
		mutex: sync.RWMutex{},
		Name:  name,
		Alive: false,
	}
}

func (w *WorkerStatus) Send(message string) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return common.Send("HCK\n", w.Connection)
}

func (w *WorkerStatus) Receive() (string, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return common.Receive(w.Connection)
}

func (w *WorkerStatus) UpdateWorkerStatus(alive bool, conn net.Conn) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.Alive = alive
	if conn == nil && w.Connection != nil {
		w.Connection.Close()
	}
	w.Connection = conn
}

func (w *WorkerStatus) Revive() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	// dockr-in-docker revive
}

type WorkerStatusManager struct {
	mutex   sync.RWMutex
	Workers []*WorkerStatus
}

func NewWorkerStatusManager() *WorkerStatusManager {
	return &WorkerStatusManager{
		mutex:   sync.RWMutex{},
		Workers: make([]*WorkerStatus, 0),
	}
}

func (wsm *WorkerStatusManager) AddWorker(workerName string) {
	wsm.mutex.Lock()
	defer wsm.mutex.Unlock()
	wsm.Workers = append(wsm.Workers, NewWorkerStatus(workerName))
}

func (wsm *WorkerStatusManager) GetWorkerStatusByName(name string) *WorkerStatus {
	wsm.mutex.RLock()
	defer wsm.mutex.RUnlock()
	for i := range wsm.Workers {
		archName := strings.Split(wsm.Workers[i].Name, "_")[0]
		if archName == name {
			return wsm.Workers[i]
		}
	}
	return nil
}

func (wsm *WorkerStatusManager) GetDeadWorker() *WorkerStatus {
	wsm.mutex.RLock()
	defer wsm.mutex.RUnlock()
	for i := range wsm.Workers {
		worker := wsm.Workers[i]
		worker.mutex.RLock()
		if !worker.Alive {
			worker.mutex.RUnlock()
			return wsm.Workers[i]
		}
		worker.mutex.RUnlock()
	}
	return nil
}
