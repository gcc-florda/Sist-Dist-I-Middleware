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
	i := 0
	for {
		if i == 0 {
			log.Debugf("Reviving worker: %s", w.Name)
			i++
		}
	}
	log.Debugf("Worker revived: %s", w.Name)
}

func (w *WorkerStatus) EstablishConnection(port string) {
	log.Debugf("Connecting manager to worker: %s", w.Name)
	const maxRetries = 3

	for i := 0; i < maxRetries; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", w.Name, port))
		if err == nil {
			log.Debugf("Manager connected to worker: %s", w.Name)
			w.UpdateWorkerStatus(true, conn)
			return
		}
		if i == maxRetries-1 {
			// Asumed dead
			log.Criticalf("Failed to connect to worker %s", w.Name)
			w.Revive()
			i = 0
		}
		log.Debugf("Manager failed to connect to worker, sleeping: %s", w.Name)
		time.Sleep(20 * time.Second)
	}
}

func (w *WorkerStatus) Handle() error {
	defer w.Connection.Close()

	log.Debugf("Watching worker: %s", w.Name)
	w.Watch()
	log.Debugf("Stop Watching worker: %s", w.Name)

	return nil
}

func (w *WorkerStatus) Watch() {
	for {
		log.Debugf("Sending HCK to worker %s", w.Name)
		if w.Send("HCK") != nil {
			log.Debugf("Error sending HCK to worker %s", w.Name)
			w.SetDeadWorkerNRevive()
			continue
		}

		log.Debugf("Receiving message from worker %s", w.Name)
		message, err := w.Receive()

		if err != nil {
			log.Debugf("Error receiving message from worker %s", w.Name)
			w.SetDeadWorkerNRevive()
			continue
		} else {
			messageAlive := common.ManagementMessage{Content: message}

			if !messageAlive.IsAlive() {
				log.Debugf("Expecting alive message from worker %s, got %s", w.Name, message)
				w.SetDeadWorkerNRevive()
				continue
			}

			log.Debugf("Worker %s is alive", w.Name)
		}

		log.Debugf("Sleeping for 10 seconds before next check")

		time.Sleep(10 * time.Second)
	}
}

func (w *WorkerStatus) SetDeadWorkerNRevive() {
	log.Debugf("Worker %s is dead, update status and revive", w.Name)
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
