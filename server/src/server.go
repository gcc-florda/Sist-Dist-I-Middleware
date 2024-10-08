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
	ex := s.Rabbit.NewExchange(common.ExchangeNameRawData, common.ExchangeDirect)

	qG := s.Rabbit.NewQueue(common.RoutingGames)
	qR := s.Rabbit.NewQueue(common.RoutingReviews)
	qP := s.Rabbit.NewQueue(common.RoutingProtocol)

	qG.Bind(ex, common.RoutingGames)
	qR.Bind(ex, common.RoutingReviews)
	qP.Bind(ex, common.RoutingProtocol)
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

		s.Rabbit.Publish(common.ExchangeNameRawData, common.GetRoutingKey(message), common.NewMessage(client.Id, message))
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
