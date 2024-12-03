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
	id             int
	coordinatorId  int
	replicasAmount int
	ip             string
	port           string
	send           chan *RingMessage
	neighbours     []*ReplicaNeighbour
}

type ReplicaNeighbour struct {
	id   int
	conn net.Conn
}

func NewReplicaManager(id int, replicasAmount int, ip string, port string) *ReplicaManager {
	return &ReplicaManager{
		id:             id,
		coordinatorId:  0,
		replicasAmount: replicasAmount,
		ip:             ip,
		port:           port,
		send:           make(chan *RingMessage),
		neighbours:     make([]*ReplicaNeighbour, replicasAmount-1),
	}
}

func (rm *ReplicaManager) Start() error {
	var wg sync.WaitGroup

	wg.Add(3)

	go rm.ListenNeighbours()

	rm.InitNetwork()

	go rm.HealthCheck()

	go rm.TalkNeighbour()

	rm.StartElection()

	wg.Wait()

	return nil
}

func (rm *ReplicaManager) InitNetwork() {
	log.Debugf("Initializing network")

	replicaPosition := 0

	for i := 1; i <= rm.replicasAmount; i++ {
		if i == rm.id {
			continue
		}

		conn, err := rm.EstablishConnection(i)
		if err != nil {
			log.Errorf("Failed to establish connection with replica %d: %s", i, err)
			continue
		}
		rm.neighbours[replicaPosition] = &ReplicaNeighbour{id: i, conn: conn}
		replicaPosition++
	}
}

func (rm *ReplicaManager) EstablishConnection(id int) (net.Conn, error) {
	var conn net.Conn
	var err error

	err = common.DoWithRetry(func() error {
		log.Debugf("Establishing connection with replica %d", id)
		conn, err = net.Dial("tcp", fmt.Sprintf("manager_%d:%s", id, rm.port))
		if err != nil {
			log.Errorf("Failed to establish connection with replica %d: %s", id, err)
			return err
		}
		return nil
	}, 3)

	return conn, err
}

func (rm *ReplicaManager) ListenNeighbours() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", rm.port))
	if err != nil {
		log.Errorf("Failed to start listening on port %s: %s", rm.port, err)
		return err
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Errorf("Failed to accept connection: %s", err)
			continue
		}

		go rm.HandleNeighbour(conn)
	}
}

func (rm *ReplicaManager) HealthCheck() {
	for {
		msg := NewRingMessage(HEALTHCHECK, "")
		rm.send <- msg
		time.Sleep(5 * time.Second)
	}
}

func (rm *ReplicaManager) TalkNeighbour() {
	log.Debugf("Talking to post neighbour")
	var err error

	reviver := make(chan int)

	neigh := rm.GetPostNeighbour(rm.id)
	for {
		select {
		case id := <-reviver:
			log.Debugf("Revived replica %d", neigh.id)
			neigh = rm.GetNeighbour(id)
			neigh.conn, err = rm.EstablishConnection(neigh.id)
			if err != nil {
				log.Criticalf("Failed to establish connection with replica %d after reviving: %s", neigh.id, err)
				continue
			}
		case msg := <-rm.send:
			log.Debugf("Sending message to post neighbour: %s", msg)
			if common.SendWithRetry(msg.Serialize(), neigh.conn, 3) != nil {
				neigh := rm.GetPostNeighbour(neigh.id)
				go func() {
					log.Debugf("Reviving replica %d", neigh.id)
					rm.Revive(neigh.id)
					reviver <- neigh.id
				}()
			}
			recv, err := common.Receive(neigh.conn)

			if err != nil {
				log.Criticalf("Error receiving message from post neighbour: %s", err)
				continue
			}

			log.Debug("Received message from post neighbour: %s", recv)

			replicaMessage, err := Deserialize(recv)
			if err != nil {
				log.Criticalf("Error deserializing message: %s", err)
				continue
			}

			if !replicaMessage.IsAlive() {
				log.Criticalf("Post neighbour is dead")
				reviver <- neigh.id
			}
		}
	}
}

func (rm *ReplicaManager) Revive(id int) error {
	log.Debugf("Reviving replica %d", id)

	return common.ReviveContainer(fmt.Sprintf("manager_%d", id), 10)
}

func (rm *ReplicaManager) StartElection() {
	log.Debugf("Starting election")
	msg := NewRingMessage(ELECTION, strconv.Itoa(rm.id))
	log.Debugf("Sending election message: %s", msg)
	rm.send <- msg
}

func (rm *ReplicaManager) HandleNeighbour(conn net.Conn) {
	log.Debugf("Handling neighbour connection")
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
		rm.send <- coordMessage
	} else {
		log.Debugf("Passing election message to post neighbour")
		msg.Content = fmt.Sprintf("%s,%s", msg.Content, strconv.Itoa(rm.id))
		log.Debugf("Updated election message: %s", msg)
		rm.send <- msg
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

	// if I am the coordinator and the new coordinator is smaller than me, I should not change
	if rm.coordinatorId == rm.id && newCoord != rm.id && newCoord < rm.id {
		log.Debugf("Arrived smaller cooridnator: %d but I am greater", newCoord)
		return nil
	}

	rm.coordinatorId = newCoord

	log.Debugf("Sending coordinator message to post neighbour")
	rm.send <- msg

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

func (rm *ReplicaManager) GetNeighbour(id int) *ReplicaNeighbour {
	for _, neigh := range rm.neighbours {
		if neigh.id == id {
			return neigh
		}
	}
	return nil
}

func (rm *ReplicaManager) GetPostNeighbour(id int) *ReplicaNeighbour {
	log.Debugf("Getting post neighbour id for replica %d", id)

	ids := make([]int, rm.replicasAmount)
	for i := range rm.replicasAmount {
		ids[i] = i + 1
	}

	var postNeighId int
	for i, val := range ids {
		if val == id {
			postNeighId = ids[(i+1)%len(ids)]
		}
	}

	log.Debugf("Post neighbour id for replica %d is %d", id, postNeighId)

	for _, neigh := range rm.neighbours {
		if neigh.id == postNeighId {
			return neigh
		}
	}

	return nil
}
