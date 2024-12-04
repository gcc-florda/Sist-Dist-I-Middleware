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
	name       string
	alive      bool
	connection net.Conn
	port       string
	CoordNews  chan bool
}

func NewWorkerStatus(name string) *WorkerStatus {
	return &WorkerStatus{
		name:      name,
		alive:     false,
		CoordNews: make(chan bool, 2),
	}
}

func (w *WorkerStatus) Send(message string) error {
	return common.Send(message, w.connection)
}

func (w *WorkerStatus) Receive() (string, error) {
	return common.Receive(w.connection)
}

func (w *WorkerStatus) UpdateWorkerStatus(alive bool, conn net.Conn) {
	w.alive = alive
	if conn == nil && w.connection != nil {
		w.connection.Close()
	}
	w.connection = conn
}

func (w *WorkerStatus) Revive() {
	log.Debugf("Reviving worker: %s", w.name)
	if common.ReviveContainer(w.name, 3) != nil {
		log.Criticalf("Failed to revive worker: %s", w.name)
		return
	}
	log.Debugf("Worker revived: %s, establishing connection", w.name)
}

func (w *WorkerStatus) EstablishConnection() {
	log.Debugf("Connecting manager to worker: %s", w.name)
	for common.DoWithRetry(func() error {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", w.name, w.port))
		if err == nil {
			log.Debugf("Manager connected to worker: %s", w.name)
			w.UpdateWorkerStatus(true, conn)
			return nil
		}
		return err
	}, 3) != nil {
		w.Revive()
	}
}

func (w *WorkerStatus) Handle() error {
	defer w.connection.Close()

	log.Debugf("Watching worker: %s", w.name)
	w.Watch()
	log.Debugf("Stop Watching worker: %s", w.name)

	return nil
}

func (w *WorkerStatus) Watch() {
	for {
		select {
		case msg := <-w.CoordNews:
			if !msg {
				log.Infof("Finish watching worker %s", w.name)
				return
			}
		default:
			log.Debugf("Sending HCK to worker %s", w.name)
			if w.Send("HCK") != nil {
				log.Debugf("Error sending HCK to worker %s", w.name)
				w.SetDeadWorkerNRevive()
				continue
			}

			log.Debugf("Receiving message from worker %s", w.name)
			message, err := w.Receive()

			if err != nil {
				log.Debugf("Error receiving message from worker %s", w.name)
				w.SetDeadWorkerNRevive()
				continue
			} else {
				messageAlive := common.ManagementMessage{Content: message}

				if !messageAlive.IsAlive() {
					log.Debugf("Expecting alive message from worker %s, got %s", w.name, message)
					w.SetDeadWorkerNRevive()
					continue
				}

				log.Debugf("Worker %s is alive", w.name)
			}

			log.Debugf("Sleeping for 10 seconds before next check")

			time.Sleep(10 * time.Second)
		}
	}
}

func (w *WorkerStatus) SetDeadWorkerNRevive() {
	log.Debugf("Worker %s is dead, update status and revive", w.name)
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
		if wsm.Workers[i].name == workerName {
			return wsm.Workers[i]
		}
	}
	return nil
}

func (wsm *WorkerStatusManager) HandleShutdown() {
	for _, worker := range wsm.Workers {
		worker.connection.Close()
	}
}
