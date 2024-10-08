package src

import (
	"fmt"
	"middleware/common"

	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Server struct {
	address  string
	port     int
	listener net.Listener
	term     chan os.Signal
}

func NewServer(ip string, port int) *Server {
	server := &Server{
		address: fmt.Sprintf("%s:%d", ip, port),
		port:    port,
		term:    make(chan os.Signal, 1),
	}

	signal.Notify(server.term, syscall.SIGTERM)

	return server
}

func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.address)
	common.FailOnError(err, "Failed to start server")
	defer s.listener.Close()

	log.Infof("Server listening on %s", s.address)

	go s.HandleShutdown()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("Failed to accept connection: %s", err)
			continue
		}

		go s.HandleConnection(conn)
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()

	log.Infof("Client connected: %s", conn.RemoteAddr().String())

	for {
		message := common.Receive(conn)

		log.Infof("Received message: %s", message)

		if message == common.END {
			break
		}
	}
}

func (s *Server) HandleShutdown() {
	<-s.term
	log.Criticalf("Received SIGTERM")
	if s.listener != nil {
		s.listener.Close()
	}
}
