package src

import (
	"fmt"
	"middleware/common"
	"middleware/rabbitmq"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Server struct {
	Address  string
	Port     int
	Listener net.Listener
	Term     chan os.Signal
	Clients  []*Client
	Rabbit   *rabbitmq.Rabbit
}

func NewServer(ip string, port int) *Server {
	server := &Server{
		Address: fmt.Sprintf("%s:%d", ip, port),
		Port:    port,
		Term:    make(chan os.Signal, 1),
		Clients: []*Client{},
		Rabbit:  rabbitmq.NewRabbit(),
	}

	signal.Notify(server.Term, syscall.SIGTERM)

	server.InitRabbit()

	return server
}

func (s *Server) InitRabbit() {
	exG := s.Rabbit.NewExchange(common.ExchangeNameGames, common.ExchangeFanout)
	exR := s.Rabbit.NewExchange(common.ExchangeNameReviews, common.ExchangeFanout)

	MFG_Q1 := s.Rabbit.NewQueue("MFG_Q1")
	MFG_Q2 := s.Rabbit.NewQueue("MFG_Q2")
	MFG_Q3 := s.Rabbit.NewQueue("MFG_Q3")
	MFG_Q4 := s.Rabbit.NewQueue("MFG_Q4")
	MFG_Q5 := s.Rabbit.NewQueue("MFG_Q5")

	MFR_Q3 := s.Rabbit.NewQueue("MFR_Q3")
	MFR_Q4 := s.Rabbit.NewQueue("MFR_Q4")
	MFR_Q5 := s.Rabbit.NewQueue("MFR_Q5")

	MFG_Q1.Bind(exG, "")
	MFG_Q2.Bind(exG, "")
	MFG_Q3.Bind(exG, "")
	MFG_Q4.Bind(exG, "")
	MFG_Q5.Bind(exG, "")

	MFR_Q3.Bind(exR, "")
	MFR_Q4.Bind(exR, "")
	MFR_Q5.Bind(exR, "")
}

func (s *Server) Start() error {
	go s.HandleShutdown()

	var err error
	s.Listener, err = net.Listen("tcp", s.Address)
	common.FailOnError(err, "Failed to start server")
	defer s.Listener.Close()

	log.Infof("Server listening on %s", s.Address)

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
		message := client.Recv()

		if message == common.END {
			break
		}

		rk := common.GetRoutingKey(message)

		mb := []byte(message[2:])
		send := make([]byte, len(mb)+1)
		copy(send[1:], mb)

		if rk == common.RoutingGames {
			send[0] = common.Type_Game
			s.Rabbit.Publish(common.ExchangeNameGames, "", common.NewMessage(client.Id, common.ProtocolMessage_Data, send))
		} else if rk == common.RoutingReviews {
			send[0] = common.Type_Review
			s.Rabbit.Publish(common.ExchangeNameReviews, "", common.NewMessage(client.Id, common.ProtocolMessage_Data, send))
		}

	}
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

	s.Rabbit.Close()
}
