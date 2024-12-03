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
		send:           make(chan *RingMessage, 10),
		neighbours:     make([]*ReplicaNeighbour, replicasAmount-1),
	}
}

func (rm *ReplicaManager) Start() error {
	var wg sync.WaitGroup

	wg.Add(1)
	go rm.ListenNeighbours()

	rm.InitNetwork()

	wg.Add(1)
	go rm.HealthCheck()

	wg.Add(1)
	go rm.TalkNeighbour()

	rm.StartElection()

	wg.Wait()

	return nil
}

func (rm *ReplicaManager) InitNetwork() {
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
		log.Infof("Establishing connection with replica %d", id)
		conn, err = net.Dial("tcp", fmt.Sprintf("manager_%d:%s", id, rm.port))
		if err != nil {
			log.Errorf("Failed to establish connection with replica %d: %s", id, err)
			return err
		}
		return nil
	}, 3)

	return conn, err
}

func (rm *ReplicaManager) HealthCheck() {
	for {
		msg := NewRingMessage(HEALTHCHECK, "")
		log.Infof("Pushing to channel: %s", msg.Serialize())
		rm.send <- msg
		time.Sleep(10 * time.Second)
	}
}

func (rm *ReplicaManager) ReviveAndGetPostNeighbour(id int, reviver chan int) *ReplicaNeighbour {
	go rm.Revive(id, reviver)
	if id == rm.coordinatorId {
		log.Criticalf("Coordinator replica %d is dead", id)
		rm.coordinatorId = 0
		rm.StartElection()
	}
	neigh := rm.GetPostNeighbour(id)

	return neigh
}

func (rm *ReplicaManager) TalkNeighbour() {
	var err error

	reviver := make(chan int, rm.replicasAmount-1)

	neigh := rm.GetPostNeighbour(rm.id)

mainloop:
	for {
		select {
		case id := <-reviver:
			log.Debugf("Reviver channel got revived replica = %d", id)
			neigh = rm.GetNeighbour(id)
			neigh.conn, err = rm.EstablishConnection(neigh.id)
			if err != nil {
				log.Criticalf("Failed to establish connection with replica %d after reviving: %s", neigh.id, err)
				continue mainloop
			}
		case msg := <-rm.send:
			log.Infof("Pulling from channel: %s", msg.Serialize())

			err = common.DoWithRetry(func() error {
				return common.Send(msg.Serialize(), neigh.conn)
			}, 3)

			if err != nil {
				log.Criticalf("Error sending message to neighbour: %s, %s", msg.Serialize(), err)
				neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
				continue mainloop
			}

			if msg.IsHealthCheck() {
				recv, err := common.Receive(neigh.conn)

				if err != nil {
					log.Criticalf("Error receiving message from neighbour: %s", err)
					neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
					continue mainloop
				}

				replicaMessage, err := Deserialize(recv)
				if err != nil {
					log.Criticalf("Error deserializing message: %s", err)
					neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
					continue mainloop
				}

				if !replicaMessage.IsAlive() {
					log.Criticalf("Expecting alive, got: %s", replicaMessage.Serialize())
					neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
					continue mainloop
				}
			}
		}
	}
}

func (rm *ReplicaManager) Revive(id int, reviver chan int) error {
	time.Sleep(10 * time.Second)

	err := common.ReviveContainer(fmt.Sprintf("manager_%d", id), 5)

	if err != nil {
		log.Errorf("Failed to revive replica %d: %s", id, err)
		return err
	}

	reviver <- id

	log.Infof("Replica %d revived", id)

	return nil
}

func (rm *ReplicaManager) StartElection() {
	msg := NewRingMessage(ELECTION, strconv.Itoa(rm.id))
	log.Infof("Pushing to channel: %s", msg.Serialize())
	rm.send <- msg
}

func (rm *ReplicaManager) ListenNeighbours() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", rm.port))
	if err != nil {
		log.Errorf("Failed to start listening on port %s: %s", rm.port, err)
		return err
	}
	defer listener.Close()
	log.Infof("Listening on port %s", rm.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Errorf("Failed to accept connection: %s", err)
			continue
		}

		go rm.HandleNeighbour(conn)
	}
}

func (rm *ReplicaManager) HandleNeighbour(conn net.Conn) {
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
	if strings.Contains(msg.Content, strconv.Itoa(rm.id)) || rm.coordinatorId == rm.id {
		rm.coordinatorId = rm.id
		coordMessage := NewRingMessage(COORDINATOR, strconv.Itoa(rm.id))
		log.Infof("Pushing to channel: %s", msg.Serialize())
		rm.send <- coordMessage
	} else {
		msg.Content = fmt.Sprintf("%s,%s", msg.Content, strconv.Itoa(rm.id))
		log.Infof("Pushing to channel: %s", msg.Serialize())
		rm.send <- msg
	}
}

func (rm *ReplicaManager) ManageCoordinatorMessage(msg *RingMessage) error {
	var err error
	newCoord, err := strconv.Atoi(msg.Content)
	if err != nil {
		log.Errorf("Error parsing coordinator message: %s", err)
		return err
	}

	// If I am the coordinator and the new coordinator is the same as me, I ignore the message
	if rm.coordinatorId == newCoord && rm.coordinatorId == rm.id {
		log.Infof("Coordinator message closed the ring, ignore")
		return nil
	}

	// if I am the coordinator and the new coordinator is smaller than me, I should not change
	if rm.coordinatorId == rm.id && newCoord != rm.id && newCoord < rm.id {
		log.Infof("Smaller replica %d wants to be coordinator, ignore", newCoord)
		return nil
	}

	rm.coordinatorId = newCoord

	log.Infof("Pushing to channel: %s", msg.Serialize())
	rm.send <- msg

	return nil
}

func (rm *ReplicaManager) ManageHealthCheckMessage(conn net.Conn) error {
	aliveMessage := NewRingMessage(ALIVE, "")

	err := common.DoWithRetry(func() error {
		return common.Send(aliveMessage.Serialize(), conn)
	}, 3)
	if err != nil {
		log.Errorf("Error sending alive message: %s", err)
		return err
	}

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

	log.Debugf("Post neighbour for %d is %d", id, postNeighId)

	for _, neigh := range rm.neighbours {
		if neigh.id == postNeighId {
			return neigh
		}
	}

	return nil
}
