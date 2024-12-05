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
	Name      string
	conn      net.Conn
	port      string
	CoordNews chan bool
}

func NewWorkerStatus(name string) *WorkerStatus {
	return &WorkerStatus{
		Name:      name,
		CoordNews: make(chan bool, 2),
	}
}

func (w *WorkerStatus) Send(message string) error {
	return common.Send(message, w.conn)
}

func (w *WorkerStatus) Receive() (string, error) {
	return common.Receive(w.conn)
}

func (w *WorkerStatus) Revive() {
	log.Infof("WORKER-REVIVING: %s", w.Name)
	if common.ReviveContainer(w.Name, 3) != nil {
		log.Criticalf("WORKER-REVIVING-FAILED: %s", w.Name)
		return
	}
	log.Infof("WORKER-REVIVED: %s", w.Name)
	w.EstablishConnection()
}

func (w *WorkerStatus) EstablishConnection() {
	log.Debugf("Connecting manager to worker: %s", w.Name)
	const maxRetries = 3

	for i := 1; i <= maxRetries; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", w.Name, w.port))
		if err == nil {
			log.Infof("WORKER-CONNECTED: %s", w.Name)
			w.conn = conn
			return
		}
		time.Sleep(1 * time.Second)
	}
	w.Revive()
}

func (w *WorkerStatus) Watch() {
	w.EstablishConnection()

	for {
		select {
		case msg := <-w.CoordNews:
			if !msg {
				log.Infof("Finish watching worker %s", w.Name)
				return
			}
		default:
			if w.Send("HCK") != nil {
				log.Debugf("Error sending HCK to worker %s", w.Name)
				w.Revive()
				continue
			}

			message, err := w.Receive()

			if err != nil {
				log.Debugf("Error receiving message from worker %s", w.Name)
				w.Revive()
				continue
			} else {
				messageAlive := common.ManagementMessage{Content: message}

				if !messageAlive.IsAlive() {
					log.Criticalf("Expecting alive message from worker %s, got %s", w.Name, message)
					w.Revive()
					continue
				}
			}

			time.Sleep(2 * time.Second)
		}
	}
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

	for i := range wsm.Workers {
		if wsm.Workers[i].Name == workerName {
			return wsm.Workers[i]
		}
	}
	return nil
}
