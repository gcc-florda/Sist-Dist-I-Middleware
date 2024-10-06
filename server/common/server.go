package common

import (
	"fmt"
	"middleware/common"
	"middleware/common/utils"
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
}

func NewServer(ip string, port int) *Server {
	server := &Server{
		Address: fmt.Sprintf("%s:%d", ip, port),
		Port:    port,
		Term:    make(chan os.Signal, 1),
		Clients: []*Client{},
	}

	signal.Notify(server.Term, syscall.SIGTERM)

	return server
}

func (s *Server) Start() error {
	var err error
	s.Listener, err = net.Listen("tcp", s.Address)
	utils.FailOnError(err, "Failed to start server")
	defer s.Listener.Close()

	log.Infof("Server listening on %s", s.Address)

	go s.HandleShutdown()

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

		log.Infof("Received message: %s", message)

		if message == common.END {
			break
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
}
