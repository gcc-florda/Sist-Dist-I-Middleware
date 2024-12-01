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
	id              int
	coordinatorId   int
	replicasAmount  int
	ip              string
	port            string
	listener        net.Listener
	activeNeighbour *ReplicaNeighbour
	neighbours      []*ReplicaNeighbour
}

type ReplicaNeighbour struct {
	id      int
	conn    net.Conn
	channel chan RingMessage
}

func NewReplicaManager(id int, replicasAmount int, ip string, port string) *ReplicaManager {
	return &ReplicaManager{
		id:             id,
		coordinatorId:  0,
		replicasAmount: replicasAmount,
		ip:             ip,
		port:           port,
		neighbours:     make([]*ReplicaNeighbour, 0),
	}
}

func (rm *ReplicaManager) Start() error {
	var wg sync.WaitGroup

	wg.Add(2)

	go rm.ListenPreNeighbour()

	go rm.CheckPostNeighbour()

	if rm.id == 1 {
		rm.StartElection()
	}

	wg.Wait()

	return nil
}

func (rm *ReplicaManager) ListenPreNeighbour() error {
	var err error
	rm.listener, err = net.Listen("tcp", fmt.Sprintf(":%s", rm.port))
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

func (rm *ReplicaManager) CheckPostNeighbour() {
	go func() {
		for {
			rm.SendMessageToPostNeighbour(NewRingMessage(HEALTHCHECK, ""))
			time.Sleep(10 * time.Second)
		}
	}()

	postNeigh := rm.GetPostNeighbour(rm.id)
	for {
		if rm.activeNeighbour != nil {
			for msg := range rm.activeNeighbour.channel {
				if common.SendWithRetry(msg.Serialize(), rm.activeNeighbour.conn, 3) != nil {
					log.Errorf("Failed to send message to post neighbour %d", rm.activeNeighbour.id)
					rm.activeNeighbour = nil
					break
				}

				if msg.IsHealthCheck() {
					log.Debugf("Waiting for alive message from post neighbour")
					recv, err := common.Receive(rm.activeNeighbour.conn)
					if err != nil {
						log.Errorf("Error receiving alive message from post neighbour: %s", err)
						if rm.coordinatorId == rm.activeNeighbour.id {
							log.Errorf("Coordinator replica %d is down", rm.coordinatorId)
							rm.coordinatorId = 0
							rm.StartElection()
						}
						rm.activeNeighbour = nil
						break
					}

					aliveMessage, err := Deserialize(recv)
					if err != nil {
						log.Errorf("Error deserializing alive message: %s", err)
						if rm.coordinatorId == rm.activeNeighbour.id {
							log.Errorf("Coordinator replica %d is down", rm.coordinatorId)
							rm.coordinatorId = 0
							rm.StartElection()
						}
						rm.activeNeighbour = nil
						break
					}

					if !aliveMessage.IsAlive() {
						log.Errorf("Expecting alive message but received: %s", aliveMessage)
					}

					log.Debugf("Received alive message from post neighbour")
				}
			}
		} else {
			if postNeigh.conn == nil {
				conn, err := rm.EstablishConnection(postNeigh.id)
				if err != nil {
					log.Errorf("Failed to establish connection with post neighbour %d: %s", postNeigh.id, err)
					go func() {
						err = rm.Revive(postNeigh.id)

						if err != nil {
							log.Criticalf("Failed to revive replica %d: %s", postNeigh.id, err)
						}

						conn, err := rm.EstablishConnection(postNeigh.id)

						if err != nil {
							log.Criticalf("Failed to establish connection with post neighbour %d: %s", postNeigh.id, err)
							return
						}

						postNeigh.conn = conn

						rm.activeNeighbour = postNeigh
					}()

					postNeigh = rm.GetPostNeighbour(postNeigh.id)
					continue
				}

				postNeigh.conn = conn
			}

			if rm.activeNeighbour == nil {
				rm.activeNeighbour = postNeigh
			}
		}
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

func (rm *ReplicaManager) Revive(id int) error {
	log.Debugf("Reviving replica %d", id)

	return common.ReviveContainer(fmt.Sprintf("manager_%d", id), 10)
}

func (rm *ReplicaManager) StartElection() {
	log.Debugf("Starting election")
	msg := NewRingMessage(ELECTION, strconv.Itoa(rm.id))
	log.Debugf("Sending election message: %s", msg)
	rm.SendMessageToPostNeighbour(msg)
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
		rm.SendMessageToPostNeighbour(coordMessage)
	} else {
		log.Debugf("Passing election message to post neighbour")
		msg.Content = fmt.Sprintf("%s,%s", msg.Content, strconv.Itoa(rm.id))
		log.Debugf("Updated election message: %s", msg)
		rm.SendMessageToPostNeighbour(msg)
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
	rm.SendMessageToPostNeighbour(msg)

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

func (rm *ReplicaManager) SendMessageToPostNeighbour(msg *RingMessage) {
	for {
		if rm.activeNeighbour != nil {
			rm.activeNeighbour.channel <- *msg
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func (rm *ReplicaManager) GetPostNeighbour(id int) *ReplicaNeighbour {
	log.Debugf("Getting post neighbour id for replica %d", id)

	ids := make([]int, rm.replicasAmount)
	for i := range ids {
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

	neigh := &ReplicaNeighbour{
		id:      postNeighId,
		conn:    nil,
		channel: make(chan RingMessage),
	}

	log.Debugf("Appending post neighbour %d to neighbours", postNeighId)

	rm.neighbours = append(rm.neighbours, neigh)

	return neigh
}
