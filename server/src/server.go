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
}

func NewServer(ip string, port int) *Server {
	arc := rabbitmq.CreateArchitecture(rabbitmq.LoadConfig("./architecture.yaml"))

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
		client := NewClient(conn)
		s.Clients = append(s.Clients, client)
		if err != nil {
			log.Errorf("Failed to accept connection: %s", err)
			continue
		}

		go s.HandleConnection(client)

	}
}

func (s *Server) HandleConnection(client *Client) {
	defer client.Close()

	log.Infof("Client connected: %s", client.Id)

	for {
		message, err := client.Recv()

		if err != nil {
			log.Errorf("Action: Receive Message from Client | Result: Error | Error: %s", err)
			break
		}

		if message == common.END {
			log.Infof("Action: Received END for Client | Result: Closing_Connection")
			break
		}

		rk := common.GetRoutingKey(message)

		ser := common.NewSerializer()

		if rk == common.RoutingGames {
			var eoftt uint32 = 0
			a := make([]byte, 4)
			binary.BigEndian.PutUint32(a, eoftt)
			if message == "1,EOF" {
				log.Debugf("Sending EOF to Games")
				s.broadcastData("GAMES", common.NewMessage(client.Id, common.ProtocolMessage_Control, a))
			} else {
				s.sendData("GAMES", common.NewMessage(client.Id, common.ProtocolMessage_Data, ser.WriteUint8(common.Type_Game).WriteString(message[2:]).ToBytes()))
			}
		} else if rk == common.RoutingReviews {
			var eoftt uint32 = 1
			a := make([]byte, 4)
			binary.BigEndian.PutUint32(a, eoftt)
			if message == "2,EOF" {
				log.Debugf("Sending EOF to Reviews")
				s.broadcastData("REVIEWS", common.NewMessage(client.Id, common.ProtocolMessage_Control, a))
			} else {
				s.sendData("REVIEWS", common.NewMessage(client.Id, common.ProtocolMessage_Data, ser.WriteUint8(common.Type_Review).WriteString(message[2:]).ToBytes()))
			}
		}
	}
}

func (s *Server) sendData(to string, ser common.Serializable) {
	if to == "GAMES" {
		ex := s.arc.MapFilter.Games.GetExchange()
		for _, key := range s.arc.MapFilter.Games.GetChannels() {
			cl := s.arc.MapFilter.Games.GetChannelSize(key)

			ex.Publish(fmt.Sprintf("%s_%d", key, s.rand.Intn(cl)+1), ser)
		}
	} else if to == "REVIEWS" {
		ex := s.arc.MapFilter.Reviews.GetExchange()
		for _, key := range s.arc.MapFilter.Reviews.GetChannels() {
			cl := s.arc.MapFilter.Reviews.GetChannelSize(key)

			ex.Publish(fmt.Sprintf("%s_%d", key, s.rand.Intn(cl)+1), ser)
		}
	}
}

func (s *Server) broadcastData(to string, ser common.Serializable) {
	if to == "GAMES" {
		ex := s.arc.MapFilter.Games.GetExchange()
		for _, key := range s.arc.MapFilter.Games.GetChannels() {
			cl := s.arc.MapFilter.Games.GetChannelSize(key)
			for i := 1; i <= cl; i++ {
				ex.Publish(fmt.Sprintf("%s_%d", key, i), ser)
			}
		}
	} else if to == "REVIEWS" {
		ex := s.arc.MapFilter.Reviews.GetExchange()
		for _, key := range s.arc.MapFilter.Reviews.GetChannels() {
			cl := s.arc.MapFilter.Reviews.GetChannelSize(key)
			for i := 1; i <= cl; i++ {
				ex.Publish(fmt.Sprintf("%s_%d", key, i), ser)
			}
		}
	}
}

func (s *Server) getDataStore(j common.JobID) (*ResultStore, error) {
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
		s, err := s.getDataStore(m.JobID())
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

		s.QueryOne.AddResult(msg.(*schema.SOCounter))
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
		s, err := s.getDataStore(m.JobID())
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

		s.QueryTwo.AddResult(msg.(*schema.PlayedTime))
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
		s, err := s.getDataStore(m.JobID())
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

		s.QueryThree.AddResult(msg.(*schema.NamedReviewCounter))
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
		s, err := s.getDataStore(m.JobID())
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

		s.QueryFour.AddResult(msg.(*schema.NamedReviewCounter))
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
		s, err := s.getDataStore(m.JobID())
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

		s.QueryFive.AddResult(msg.(*schema.NamedReviewCounter))
		delivery.Ack(false)
	}
}
