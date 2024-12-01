package src

import (
	"fmt"
	"middleware/common"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ReplicaManager struct {
	id                    int
	coordinatorId         int
	replicasAmount        int
	port                  string
	listener              net.Listener
	postNeighbourMessages chan RingMessage
}

func NewReplicaManager(id int, replicasAmount int, port string) *ReplicaManager {
	return &ReplicaManager{
		id:                    id,
		coordinatorId:         0,
		replicasAmount:        replicasAmount,
		port:                  port,
		postNeighbourMessages: make(chan RingMessage),
	}
}

func (rm *ReplicaManager) Start() error {
	var wg sync.WaitGroup

	wg.Add(2)

	go rm.ListenPreNeighbour()

	go rm.ConnectPostNeighbour()

	if rm.id == 1 {
		rm.StartElection()
	}

	wg.Wait()

	return nil
}

func (rm *ReplicaManager) ListenPreNeighbour() error {
	var err error
	rm.listener, err = net.Listen("tcp", rm.port)
	if err != nil {
		log.Errorf("Failed to start listening on port %s: %s", rm.port, err)
		return err
	}
	defer rm.listener.Close()

	for {
		conn, err := rm.listener.Accept()
		if err != nil {
			log.Errorf("Failed to accept connection: %s", err)
			continue
		}

		go rm.HandlePreNeighbour(conn)
	}
}

func (rm *ReplicaManager) ConnectPostNeighbour() {
	postNeighId := GetPostNeighbourId(rm.replicasAmount, rm.id)
	var conn net.Conn
	var connLock sync.Mutex
	finishConnection := make(chan struct{})

	for {
		newConn, err := rm.EstablishConnection(postNeighId)

		if err != nil {
			go func(neighId int) {
				if rm.Revive(neighId) != nil {
					log.Errorf("Failed to revive replica %d", neighId)
					return
				}

				log.Debugf("Revived replica %d", neighId)

				connLock.Lock()
				defer connLock.Unlock()

				if conn != nil {
					conn.Close()
					conn = nil
					log.Debug("Closed existing connection")
				}

				postNeighId = neighId
				close(finishConnection)
			}(postNeighId)

			postNeighId = GetPostNeighbourId(rm.replicasAmount, postNeighId)
			continue
		}

		connLock.Lock()
		if conn != nil {
			conn.Close()
			conn = nil
			log.Debug("Closed existing connection")
		}
		conn = newConn
		connLock.Unlock()

		go rm.TalkPostNeighbour(postNeighId, conn, finishConnection)
		<-finishConnection
		log.Debugf("Post neighbour connection closed, reconnecting to %d", postNeighId)
	}
}

func (rm *ReplicaManager) EstablishConnection(id int) (net.Conn, error) {
	log.Debugf("Establishing connection with replica %d", id)
	conn, err := net.Dial("tcp", fmt.Sprintf("manager_%d:%s", id, rm.port))
	if err != nil {
		log.Errorf("Failed to establish connection with replica %d: %s", id, err)
		return nil, err
	}
	return conn, nil
}

func (rm *ReplicaManager) TalkPostNeighbour(id int, conn net.Conn, finishConnection chan struct{}) {
	var wg sync.WaitGroup
	finishHealthCheck := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-finishHealthCheck:
				log.Debugf("Health check done for replica %d", rm.id)
				return
			case rm.postNeighbourMessages <- *NewRingMessage(HEALTHCHECK, ""):
				log.Debugf("Sent health check message to replica %d, sleeping", rm.id)
				time.Sleep(10 * time.Second)
			}
		}
	}()

	for msg := range rm.postNeighbourMessages {
		log.Debugf("Sending message to post neighbour: %s", msg)
		err := common.SendWithRetry(msg.Serialize(), conn, 3)
		if err != nil {
			log.Errorf("Error sending message to post neighbour: %s", err)
			break
		}

		if msg.IsHealthCheck() {
			log.Debugf("Waiting for alive message from post neighbour")
			recv, err := common.Receive(conn)
			if err != nil {
				log.Errorf("Error receiving alive message from post neighbour: %s", err)
				if rm.coordinatorId == id {
					log.Errorf("Coordinator replica %d is down", id)
					rm.coordinatorId = 0
					rm.StartElection()
				}
				break
			}

			aliveMessage, err := Deserialize(recv)
			if err != nil {
				log.Errorf("Error deserializing alive message: %s", err)
				if rm.coordinatorId == id {
					log.Errorf("Coordinator replica %d is down", id)
					rm.coordinatorId = 0
					rm.StartElection()
				}
				break
			}

			if !aliveMessage.IsAlive() {
				log.Errorf("Expecting alive message but received: %s", aliveMessage)
			}

			log.Debugf("Received alive message from post neighbour")
		}
	}

	close(finishHealthCheck)
	log.Debugf("Closed health check channel for replica %d", rm.id)
	wg.Wait()
	close(finishConnection)
	log.Debugf("Closed done channel for replica %d", rm.id)
}

func (rm *ReplicaManager) Revive(id int) error {
	log.Debugf("Reviving replica %d", id)

	return common.ReviveContainer(fmt.Sprintf("manager_%d", id), 10)
}

func (rm *ReplicaManager) StartElection() {
	log.Debugf("Starting election")
	msg := NewRingMessage(ELECTION, strconv.Itoa(rm.id))
	log.Debugf("Sending election message: %s", msg)
	rm.postNeighbourMessages <- *msg
}

func (rm *ReplicaManager) HandlePreNeighbour(conn net.Conn) {
	for {
		message, err := common.Receive(conn)

		if err != nil {
			log.Criticalf("Error receiving pre neighbour message: %s", err)
			return
		}

		replicaMessage, err := Deserialize(message)

		if err != nil {
			log.Criticalf("Error deserializing pre neighbour message: %s", err)
			continue
		}

		if replicaMessage.IsElection() {
			rm.ManageElectionMessage(replicaMessage)
		} else if replicaMessage.IsCoordinator() {
			rm.ManageCoordinatorMessage(replicaMessage)
		} else if replicaMessage.IsHealthCheck() {
			if rm.ManageHealthCheckMessage(conn) != nil {
				log.Criticalf("Error receiving pre neighbour alive message: %s", err)
				return
			}
		} else {
			log.Criticalf("Unknown pre neighbour message: %s", message)
		}

	}
}

func (rm *ReplicaManager) ManageElectionMessage(msg *RingMessage) {
	log.Debugf("Received election message: %s", msg)
	if strings.Contains(msg.Content, strconv.Itoa(rm.id)) || rm.coordinatorId == rm.id {
		rm.coordinatorId = rm.id
		log.Debugf("Election message already visited replica %d", rm.id)
		coordMessage := NewRingMessage(COORDINATOR, strconv.Itoa(rm.id))
		log.Debugf("Sending coordinator message: %s", coordMessage)
		rm.postNeighbourMessages <- *coordMessage
	} else {
		log.Debugf("Passing election message to post neighbour")
		msg.Content = fmt.Sprintf("%s,%s", msg.Content, strconv.Itoa(rm.id))
		log.Debugf("Updated election message: %s", msg)
		rm.postNeighbourMessages <- *msg
	}
}

func (rm *ReplicaManager) ManageCoordinatorMessage(msg *RingMessage) error {
	log.Debugf("Received coordinator message: %s", msg)
	var err error
	newCoord, err := strconv.Atoi(msg.Content)
	if err != nil {
		log.Errorf("Error parsing coordinator message: %s", err)
		return err
	}

	if rm.coordinatorId == newCoord {
		log.Debugf("Coordinator replica %d already set", newCoord)
		return nil
	}

	rm.coordinatorId = newCoord

	log.Debugf("Sending coordinator message to post neighbour")
	rm.postNeighbourMessages <- *msg

	return nil
}

func (rm *ReplicaManager) ManageHealthCheckMessage(conn net.Conn) error {
	log.Debugf("Received health check message")
	aliveMessage := NewRingMessage(ALIVE, "")

	err := common.SendWithRetry(aliveMessage.Serialize(), conn, 3)
	if err != nil {
		log.Errorf("Error sending alive message: %s", err)
		return err
	}

	log.Debugf("Sent alive message")

	return nil
}

func GetPostNeighbourId(replicasAmount int, id int) int {
	log.Debugf("Getting post neighbour id for replica %d", id)
	var postNeighId int

	if id == replicasAmount {
		postNeighId = 1
	} else {
		postNeighId = id + 1
	}

	return postNeighId
}
