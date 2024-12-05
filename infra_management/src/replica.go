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
	CoordNews      chan bool
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
		CoordNews:      make(chan bool, 2),
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

	for rm.coordinatorId == 0 {
		rm.StartElection()
		time.Sleep(5 * time.Second)
	}

	wg.Wait()

	return nil
}

func (rm *ReplicaManager) InitNetwork() {
	replicaPosition := 0

	for i := 1; i <= rm.replicasAmount; i++ {
		if i == rm.id {
			continue
		}

		rm.neighbours[replicaPosition] = &ReplicaNeighbour{id: i}
		replicaPosition++
	}
}

func (rm *ReplicaManager) EstablishConnection(id int) (net.Conn, error) {
	var conn net.Conn
	var err error

	err = common.DoWithRetry(func() error {
		log.Infof("[COOR = %d] - Establishing connection with replica %d", rm.coordinatorId, id)
		conn, err = net.Dial("tcp", fmt.Sprintf("manager_%d:%s", id, rm.port))
		if err != nil {
			log.Errorf("[COOR = %d] - Failed to establish connection with replica %d: %s", rm.coordinatorId, id, err)
			return err
		}
		return nil
	}, 3, 2)

	return conn, err
}

func (rm *ReplicaManager) HealthCheck() {
	for {
		msg := NewRingMessage(HEALTHCHECK, "")
		log.Infof("[COOR = %d] - Pushing to channel: %s", rm.coordinatorId, msg.Serialize())
		rm.send <- msg
		time.Sleep(10 * time.Second)
	}
}

func (rm *ReplicaManager) ReviveAndGetPostNeighbour(id int, reviver chan int) *ReplicaNeighbour {
	go rm.Revive(id, reviver)
	if id == rm.coordinatorId {
		log.Criticalf("[COOR = %d] - Coordinator replica %d is dead", rm.coordinatorId, id)
		rm.coordinatorId = 0
		rm.StartElection()
	}
	neigh := rm.GetPostNeighbour(id)
	neigh.conn = nil

	return neigh
}

func (rm *ReplicaManager) TalkNeighbour() {
	var err error

	reviver := make(chan int, rm.replicasAmount-1)

	neigh := rm.GetPostNeighbour(rm.id)

mainloop:
	for {

		if neigh.conn == nil {
			neigh.conn, err = rm.EstablishConnection(neigh.id)
			if err != nil {
				log.Criticalf("[COOR = %d] - Failed to establish connection with replica %d: %s", rm.coordinatorId, neigh.id, err)
				neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
				continue mainloop
			}
		}

		select {
		case id := <-reviver:
			log.Debugf("[COOR = %d] - Reviver channel got revived replica = %d", rm.coordinatorId, id)
			neigh = rm.GetNeighbour(id)
			neigh.conn = nil
			continue mainloop
		case msg := <-rm.send:
			log.Infof("[COOR = %d] - Pulling from channel: %s", rm.coordinatorId, msg.Serialize())

			err = common.DoWithRetry(func() error {
				return common.Send(msg.Serialize(), neigh.conn)
			}, 3, 2)

			if err != nil {
				log.Criticalf("[COOR = %d] - Error sending message to neighbour: %s, %s", rm.coordinatorId, msg.Serialize(), err)
				neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
				if !msg.IsHealthCheck() {
					rm.send <- msg
				}
				continue mainloop
			}

			if msg.IsHealthCheck() {
				recv, err := common.Receive(neigh.conn)

				if err != nil {
					log.Criticalf("[COOR = %d] - Error receiving message from neighbour: %s", rm.coordinatorId, err)
					neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
					continue mainloop
				}

				replicaMessage, err := Deserialize(recv)
				if err != nil {
					log.Criticalf("[COOR = %d] - Error deserializing message: %s", rm.coordinatorId, err)
					neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
					continue mainloop
				}

				if !replicaMessage.IsAlive() {
					log.Criticalf("[COOR = %d] - Expecting alive, got: %s", rm.coordinatorId, replicaMessage.Serialize())
					neigh = rm.ReviveAndGetPostNeighbour(neigh.id, reviver)
					continue mainloop
				}
			}
		}
	}
}

func (rm *ReplicaManager) Revive(id int, reviver chan int) error {
	time.Sleep(2 * time.Second)
	err := common.ReviveContainer(fmt.Sprintf("manager_%d", id), 5)

	if err != nil {
		log.Errorf("[COOR = %d] - Failed to revive replica %d: %s", rm.coordinatorId, id, err)
		return err
	}

	reviver <- id

	log.Infof("[COOR = %d] - Replica %d revived", rm.coordinatorId, id)

	return nil
}

func (rm *ReplicaManager) StartElection() {
	msg := NewRingMessage(ELECTION, strconv.Itoa(rm.id))
	log.Infof("[COOR = %d] - Pushing to channel: %s", rm.coordinatorId, msg.Serialize())
	rm.send <- msg
}

func (rm *ReplicaManager) ListenNeighbours() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", rm.port))
	if err != nil {
		log.Errorf("[COOR = %d] - Failed to start listening on port %s: %s", rm.coordinatorId, rm.port, err)
		return err
	}
	defer listener.Close()
	log.Infof("[COOR = %d] - Listening on port %s", rm.coordinatorId, rm.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Errorf("[COOR = %d] - Failed to accept connection: %s", rm.coordinatorId, err)
			continue
		}

		go rm.HandleNeighbour(conn)
	}
}

func (rm *ReplicaManager) HandleNeighbour(conn net.Conn) {
	for {
		message, err := common.Receive(conn)

		if err != nil {
			log.Criticalf("[COOR = %d] - Error receiving pre neighbour message: %s", rm.coordinatorId, err)
			return
		}

		replicaMessage, err := Deserialize(message)

		if err != nil {
			log.Criticalf("[COOR = %d] - Error deserializing pre neighbour message: %s", rm.coordinatorId, err)
			continue
		}

		if replicaMessage.IsElection() {
			rm.ManageElectionMessage(replicaMessage)
		} else if replicaMessage.IsCoordinator() {
			rm.ManageCoordinatorMessage(replicaMessage)
		} else if replicaMessage.IsHealthCheck() {
			if rm.ManageHealthCheckMessage(conn) != nil {
				log.Criticalf("[COOR = %d] - Error receiving pre neighbour alive message: %s", rm.coordinatorId, err)
				return
			}
		} else {
			log.Criticalf("[COOR = %d] - Unknown pre neighbour message: %s", rm.coordinatorId, message)
		}
	}
}

func (rm *ReplicaManager) ManageElectionMessage(msg *RingMessage) {
	if strings.Contains(msg.Content, strconv.Itoa(rm.id)) || rm.coordinatorId == rm.id {
		rm.coordinatorId = rm.id
		coordMessage := NewRingMessage(COORDINATOR, strconv.Itoa(rm.id))
		log.Infof("[COOR = %d] - Pushing to channel: %s", rm.coordinatorId, msg.Serialize())
		rm.send <- coordMessage
		rm.CoordNews <- true
	} else {
		msg.Content = fmt.Sprintf("%s,%s", msg.Content, strconv.Itoa(rm.id))
		log.Infof("[COOR = %d] - Pushing to channel: %s", rm.coordinatorId, msg.Serialize())
		rm.send <- msg
	}
}

func (rm *ReplicaManager) ManageCoordinatorMessage(msg *RingMessage) error {
	var err error
	newCoord, err := strconv.Atoi(msg.Content)
	if err != nil {
		log.Errorf("[COOR = %d] - Error parsing coordinator message: %s", rm.coordinatorId, err)
		return err
	}

	// If I am the coordinator and the new coordinator is the same as me, I ignore the message
	if rm.coordinatorId == newCoord && rm.coordinatorId == rm.id {
		log.Infof("[COOR = %d] - Coordinator message closed the ring, ignore", rm.coordinatorId)
		return nil
	}

	if rm.coordinatorId == rm.id && newCoord != rm.id && newCoord < rm.id {
		// if I am the coordinator and the new coordinator is smaller than me, I should not change
		if newCoord < rm.id {
			log.Infof("[COOR = %d] - Smaller replica %d wants to be coordinator, ignore", rm.coordinatorId, newCoord)
			return nil
		} else {
			rm.CoordNews <- false
		}
	}

	rm.coordinatorId = newCoord

	log.Infof("[COOR = %d] - Pushing to channel: %s", rm.coordinatorId, msg.Serialize())
	rm.send <- msg

	return nil
}

func (rm *ReplicaManager) ManageHealthCheckMessage(conn net.Conn) error {
	aliveMessage := NewRingMessage(ALIVE, "")

	err := common.DoWithRetry(func() error {
		return common.Send(aliveMessage.Serialize(), conn)
	}, 3, 2)
	if err != nil {
		log.Errorf("[COOR = %d] - Error sending alive message: %s", rm.coordinatorId, err)
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

	log.Debugf("[COOR = %d] - Post neighbour for %d is %d", rm.coordinatorId, id, postNeighId)

	for _, neigh := range rm.neighbours {
		if neigh.id == postNeighId {
			return neigh
		}
	}

	log.Fatalf("HOPING FOR UNREACHABLE")
	return nil
}

func (rm *ReplicaManager) HandleShutdown() {
	for _, neigh := range rm.neighbours {
		neigh.conn.Close()
	}
}
