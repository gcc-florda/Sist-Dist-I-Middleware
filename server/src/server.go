package src

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"middleware/common"
	"middleware/rabbitmq"
	"middleware/worker/schema"
	"net"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Server struct {
	Address         string
	Port            int
	Listener        net.Listener
	Term            chan os.Signal
	Clients         []*Client
	arc             *rabbitmq.Architecture
	rand            *rand.Rand
	ExchangeGames   *rabbitmq.Exchange
	ExchangeReviews *rabbitmq.Exchange
	Results         *rabbitmq.Results
	ResultStores    map[common.JobID]*ResultStore
	storeMu         sync.Mutex
}

func NewServer(ip string, port int) *Server {
	arc := rabbitmq.CreateArchitecture(common.LoadArchitectureConfig("./architecture.yaml"))

	server := &Server{
		Address:         fmt.Sprintf("%s:%d", ip, port),
		Port:            port,
		Term:            make(chan os.Signal, 1),
		Clients:         []*Client{},
		arc:             arc,
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
		ExchangeGames:   arc.MapFilter.Games.GetExchange(),
		ExchangeReviews: arc.MapFilter.Reviews.GetExchange(),
		Results:         arc.Results,
		ResultStores:    make(map[uuid.UUID]*ResultStore),
		storeMu:         sync.Mutex{},
	}

	signal.Notify(server.Term, syscall.SIGTERM)

	return server
}

func (s *Server) Start() error {
	go s.HandleShutdown()

	var err error
	s.Listener, err = net.Listen("tcp", s.Address)
	common.FailOnError(err, "Failed to start server")
	defer s.Listener.Close()

	log.Infof("Server listening on %s", s.Address)

	s.ConsumeResults()
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			log.Errorf("Action: Accept connection | Result: Error | Error: %s", err)
			break
		}
		client := NewClient(conn)
		s.Clients = append(s.Clients, client)

		go s.HandleConnection(client)
	}

	return nil
}

func (s *Server) HandleConnection(client *Client) {
	defer client.Close()

	log.Infof("Client connected: %s", client.Id)

	err := client.SendId()

	if err != nil {
		log.Infof("Client disconnected: %s", client.Id)
		s.RemoveClient(client)
		return
	}
	gamec := 1
	reviewc := 1
	for {
		message, err := client.Recv()

		if err != nil {
			log.Errorf("Action: Receive Message from Client | Result: Error | Error: %s", err)
			break
		}

		messageDeserialized, err := common.DeserializeClientMessage(message)

		common.FailOnError(err, "Failed to deserialize message") // UNREACHABLE

		switch messageDeserialized.Type {
		case common.Type_GAMES:
			s.Broadcast(client, messageDeserialized, &common.IdempotencyID{Origin: "SV", Sequence: uint32(gamec)})
			gamec++

		case common.Type_REVIEWS:
			s.Broadcast(client, messageDeserialized, &common.IdempotencyID{Origin: "SV", Sequence: uint32(reviewc)})
			reviewc++

		case common.Type_AskForResults:
			log.Debugf("message is Type_AskForResults")
			s.SendResults(client, messageDeserialized)

		case common.Type_CloseConnection:
			log.Infof("Action: Received Close Connection for Client | Result: Closing_Connection")
			s.RemoveClient(client)
			return

		default:
			log.Error("Received an UNKNOWN Type message --> no broadcast")
		}
	}
}

func (s *Server) Broadcast(client *Client, message common.ClientMessage, idemId *common.IdempotencyID) {

	ser := common.NewSerializer()

	if message.IsEOF() {
		var eoftt uint32 = 0
		if message.Type == common.Type_REVIEWS {
			log.Debugf("About to broadcast EOF REVIEWS %s", client.Id)
			eoftt = 1
		} else {
			log.Debugf("About to broadcast EOF GAMES %s", client.Id)
			eoftt = 0
		}

		content := make([]byte, 4)
		binary.BigEndian.PutUint32(content, eoftt)
		s.BroadcastData(message.Type, common.NewMessage(client.Id, idemId, common.ProtocolMessage_Control, content), true)
	} else {
		s.BroadcastData(message.Type, common.NewMessage(client.Id, idemId, common.ProtocolMessage_Data, ser.WriteUint8(uint8(message.Type)).WriteString(message.Content).ToBytes()), false)
	}
}

func (s *Server) BroadcastData(exType int, ser common.Serializable, fanout bool) {
	var partitionedExchange *rabbitmq.PartitionedExchange

	switch exType {
	case common.Type_GAMES:
		partitionedExchange = s.arc.MapFilter.Games
	case common.Type_REVIEWS:
		partitionedExchange = s.arc.MapFilter.Reviews
	default:
		log.Error("Unknown type reached BroadcastData")
		return
	}

	ex := partitionedExchange.GetExchange()
	for _, key := range partitionedExchange.GetChannels() {
		cl := partitionedExchange.GetChannelSize(key)
		if fanout {
			for i := 1; i <= cl; i++ {
				ex.Publish(fmt.Sprintf("%s_%d", key, i), ser)
			}
		} else {
			ex.Publish(fmt.Sprintf("%s_%d", key, s.rand.Intn(cl)+1), ser)
		}
	}
}

func (s *Server) SendResults(client *Client, message common.ClientMessage) {
	log.Debugf("Sending results to client: %s", client.Id)
	jobId, err := uuid.Parse(message.Content)
	if err != nil {
		log.Errorf("Invalid JobID: %s", jobId)
		return
	}

	store, err := s.GetDataStore(jobId)

	if err != nil {
		log.Errorf("Error getting ResultStore: %s", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(5)

	log.Debug("About to send Results for query")
	go client.SendResultsQ1(store.QueryOne, &wg)
	go client.SendResultsQ2(store.QueryTwo, &wg)
	go client.SendResultsQ3(store.QueryThree, &wg)
	go client.SendResultsQ4(store.QueryFour, &wg)
	go client.SendResultsQ5(store.QueryFive, &wg)

	wg.Wait()

	log.Debugf("Finished sending results to client: %s", client.Id)

	client.SendEndWithResults()
}

func (s *Server) GetDataStore(j common.JobID) (*ResultStore, error) {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	store, ok := s.ResultStores[j]
	if !ok {
		store, err := NewResultStore(j)
		if err != nil {
			return nil, err
		}
		s.ResultStores[j] = store
		return store, nil
	}
	return store, nil
}

func (s *Server) HandleShutdown() {
	<-s.Term
	log.Criticalf("Received SIGTERM")

	if s.Listener != nil {
		s.Listener.Close()
	}

	for _, client := range s.Clients {
		client.Close()
		log.Infof("Closed connection for client: %s", client.Id)
	}
}

func (s *Server) ConsumeResults() {
	go s.ConsumeResultsQ1()
	go s.ConsumeResultsQ2()
	go s.ConsumeResultsQ3()
	go s.ConsumeResultsQ4()
	go s.ConsumeResultsQ5()
}

func (s *Server) ConsumeResultsQ1() {
	q := s.Results.QueryOne.GetQueueSingle(1)
	chq := q.Consume()

	for delivery := range chq {
		m, err := common.MessageFromBytes(delivery.Body)
		if err != nil {
			log.Errorf("Action: Deserialize %s | Result: Error | Error: %s", q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		s, err := s.GetDataStore(m.JobID())
		if err != nil {
			log.Errorf("Action: Get Result Store %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		if m.IsEOF() {
			log.Infof("Action: Received EOF %s - %s", m.JobID(), q.ExternalName)
			s.QueryOne.Finished = true
			delivery.Ack(false)
			continue
		}

		msg, err := schema.UnmarshalMessage(m.Content)

		if err != nil {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}

		if reflect.TypeOf(msg) != reflect.TypeOf(&schema.SOCounter{}) {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: Unknown Message Type %s", m.JobID(), q.ExternalName, reflect.TypeOf(msg))
			delivery.Nack(false, true)
			continue
		}

		s.QueryOne.AddResult(msg.(*schema.SOCounter), m.IdempotencyID)
		delivery.Ack(false)
	}
}

func (s *Server) ConsumeResultsQ2() {
	q := s.Results.QueryTwo.GetQueueSingle(1)
	chq := q.Consume()

	for delivery := range chq {
		m, err := common.MessageFromBytes(delivery.Body)
		if err != nil {
			log.Errorf("Action: Deserialize %s | Result: Error | Error: %s", q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		s, err := s.GetDataStore(m.JobID())
		if err != nil {
			log.Errorf("Action: Get Result Store %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		if m.IsEOF() {
			log.Infof("Action: Received EOF %s - %s", m.JobID(), q.ExternalName)
			s.QueryTwo.Finished = true
			delivery.Ack(false)
			continue
		}

		msg, err := schema.UnmarshalMessage(m.Content)

		if err != nil {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}

		if reflect.TypeOf(msg) != reflect.TypeOf(&schema.PlayedTime{}) {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: Unknown Message Type %s", m.JobID(), q.ExternalName, reflect.TypeOf(msg))
			delivery.Nack(false, true)
			continue
		}

		s.QueryTwo.AddResult(msg.(*schema.PlayedTime), m.IdempotencyID)
		delivery.Ack(false)
	}
}

func (s *Server) ConsumeResultsQ3() {
	q := s.Results.QueryThree.GetQueueSingle(1)
	chq := q.Consume()

	for delivery := range chq {
		m, err := common.MessageFromBytes(delivery.Body)
		if err != nil {
			log.Errorf("Action: Deserialize %s | Result: Error | Error: %s", q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		s, err := s.GetDataStore(m.JobID())
		if err != nil {
			log.Errorf("Action: Get Result Store %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		if m.IsEOF() {
			log.Infof("Action: Received EOF %s - %s", m.JobID(), q.ExternalName)
			s.QueryThree.Finished = true
			delivery.Ack(false)
			continue
		}

		msg, err := schema.UnmarshalMessage(m.Content)

		if err != nil {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}

		if reflect.TypeOf(msg) != reflect.TypeOf(&schema.NamedReviewCounter{}) {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: Unknown Message Type %s", m.JobID(), q.ExternalName, reflect.TypeOf(msg))
			delivery.Nack(false, true)
			continue
		}

		s.QueryThree.AddResult(msg.(*schema.NamedReviewCounter), m.IdempotencyID)
		delivery.Ack(false)
	}
}

func (s *Server) ConsumeResultsQ4() {
	q := s.Results.QueryFour.GetQueueSingle(1)
	chq := q.Consume()

	for delivery := range chq {
		m, err := common.MessageFromBytes(delivery.Body)
		if err != nil {
			log.Errorf("Action: Deserialize %s | Result: Error | Error: %s", q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		s, err := s.GetDataStore(m.JobID())
		if err != nil {
			log.Errorf("Action: Get Result Store %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		if m.IsEOF() {
			log.Infof("Action: Received EOF %s - %s", m.JobID(), q.ExternalName)
			s.QueryFour.Finished = true
			delivery.Ack(false)
			continue
		}

		msg, err := schema.UnmarshalMessage(m.Content)

		if err != nil {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}

		if reflect.TypeOf(msg) != reflect.TypeOf(&schema.NamedReviewCounter{}) {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: Unknown Message Type %s", m.JobID(), q.ExternalName, reflect.TypeOf(msg))
			delivery.Nack(false, true)
			continue
		}

		s.QueryFour.AddResult(msg.(*schema.NamedReviewCounter), m.IdempotencyID)
		delivery.Ack(false)
	}
}

func (s *Server) ConsumeResultsQ5() {
	q := s.Results.QueryFive.GetQueueSingle(1)
	chq := q.Consume()

	for delivery := range chq {
		m, err := common.MessageFromBytes(delivery.Body)
		if err != nil {
			log.Errorf("Action: Deserialize %s | Result: Error | Error: %s", q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		s, err := s.GetDataStore(m.JobID())
		if err != nil {
			log.Errorf("Action: Get Result Store %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}
		if m.IsEOF() {
			log.Infof("Action: Received EOF %s - %s", m.JobID(), q.ExternalName)
			s.QueryFive.Finished = true
			delivery.Ack(false)
			continue
		}

		msg, err := schema.UnmarshalMessage(m.Content)

		if err != nil {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: %s", m.JobID(), q.ExternalName, err)
			delivery.Nack(false, true)
			continue
		}

		if reflect.TypeOf(msg) != reflect.TypeOf(&schema.NamedReviewCounter{}) {
			log.Errorf("Action: Demarshal Result %s - %s | Result: Error | Error: Unknown Message Type %s", m.JobID(), q.ExternalName, reflect.TypeOf(msg))
			delivery.Nack(false, true)
			continue
		}

		s.QueryFive.AddResult(msg.(*schema.NamedReviewCounter), m.IdempotencyID)
		delivery.Ack(false)
	}
}

func (s *Server) RemoveClient(client *Client) {
	for i, c := range s.Clients {
		if c == client {
			s.Clients = append(s.Clients[:i], s.Clients[i+1:]...)
			return
		}
	}
}
