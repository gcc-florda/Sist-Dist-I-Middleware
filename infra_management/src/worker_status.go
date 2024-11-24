package src

import (
	"fmt"
	"middleware/common"
	"net"
	"strings"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type WorkerStatus struct {
	Name       string
	Alive      bool
	Connection net.Conn
}

func NewWorkerStatus(name string) *WorkerStatus {
	return &WorkerStatus{
		Name:  name,
		Alive: false,
	}
}

func (w *WorkerStatus) Send(message string) error {
	return common.Send(message, w.Connection)
}

func (w *WorkerStatus) Receive() (string, error) {
	return common.Receive(w.Connection)
}

func (w *WorkerStatus) UpdateWorkerStatus(alive bool, conn net.Conn) {
	w.Alive = alive
	if conn == nil && w.Connection != nil {
		w.Connection.Close()
	}
	w.Connection = conn
}

func (w *WorkerStatus) Revive() {
	// dockr-in-docker revive, kill before
}

func (w *WorkerStatus) EstablishConnection(port string) {
	log.Debugf("Connecting manager to worker", w.Name)
	const maxRetries = 3

	for i := 0; i < maxRetries; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", w.Name, port))
		if err == nil {
			log.Debugf("Manager connected to worker: ", w.Name)
			w.UpdateWorkerStatus(true, conn)
			return
		}
		if i == maxRetries-1 {
			// Asumed dead
			log.Criticalf("Failed to connect to worker %s", w.Name)
			w.Receive()
			i = 0
		}

		time.Sleep(5 * time.Second)
	}
}

func (w *WorkerStatus) Handle() error {
	defer w.Connection.Close()

	w.Watch()

	return nil
}

func (w *WorkerStatus) Watch() {
	for {
		if w.Send("HCK") != nil {
			w.ReviveWorker()
			continue
		}

		message, err := w.Receive()

		if err != nil {
			w.ReviveWorker()
			continue
		} else {
			messageAlive := common.ManagementMessage{Content: message}

			if !messageAlive.IsAlive() {
				w.ReviveWorker()
				continue
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func (w *WorkerStatus) ReviveWorker() {
	w.UpdateWorkerStatus(false, nil)
	w.Revive()
}

type WorkerStatusManager struct {
	Workers []*WorkerStatus
}

func NewWorkerStatusManager() *WorkerStatusManager {
	return &WorkerStatusManager{
		Workers: make([]*WorkerStatus, 0),
	}
}

func (wsm *WorkerStatusManager) AddWorker(workerName string) {
	wsm.Workers = append(wsm.Workers, NewWorkerStatus(workerName))
}

func (wsm *WorkerStatusManager) GetWorkerStatusByName(name string) *WorkerStatus {
	workerName := fmt.Sprintf("node_%s", strings.ToLower(name))
	log.Debugf("Searching for worker %s", workerName)

	for i := range wsm.Workers {
		if wsm.Workers[i].Name == workerName {
			return wsm.Workers[i]
		}
	}
	return nil
}
