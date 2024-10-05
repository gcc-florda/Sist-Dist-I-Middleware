package common

import (
	"encoding/binary"
	"fmt"
	"io"
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

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
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
	FailOnError(err, "Failed to start server")
	defer s.listener.Close()

	log.Infof("Server listening on %s", s.address)

	go s.HandleShutdown()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("Failed to accept connection: %s", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Infof("Client connected: %s", conn.RemoteAddr().String())

	for {
		message := s.ReceiveMessage(conn)

		log.Infof("Received message: %s", string(message))
	}
}

func (s *Server) HandleShutdown() {
	<-s.term
	log.Criticalf("Received SIGTERM")
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) ReceiveMessage(conn net.Conn) []byte {
	var length uint32
	err := binary.Read(conn, binary.BigEndian, &length)
	FailOnError(err, "Failed to read message length")

	message := make([]byte, length)
	_, err = io.ReadFull(conn, message)
	FailOnError(err, "Failed to read message")

	return message
}
