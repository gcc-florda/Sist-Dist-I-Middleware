package src

import (
	"encoding/binary"
	"fmt"
	"middleware/common"
	"middleware/rabbitmq"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Server struct {
	Address         string
	Port            int
	Listener        net.Listener
	Term            chan os.Signal
	Clients         []*Client
	ExchangeGames   *rabbitmq.Exchange
	ExchangeReviews *rabbitmq.Exchange
}

func NewServer(ip string, port int) *Server {

	arc := rabbitmq.CreateArchitecture(rabbitmq.LoadConfig("./architecture.yaml"))

	server := &Server{
		Address:         fmt.Sprintf("%s:%d", ip, port),
		Port:            port,
		Term:            make(chan os.Signal, 1),
		Clients:         []*Client{},
		ExchangeGames:   arc.MapFilter.Games.GetExchange(),
		ExchangeReviews: arc.MapFilter.Reviews.GetExchange(),
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
			var eoftt uint32 = 0
			a := make([]byte, 4)
			binary.BigEndian.PutUint32(a, eoftt)
			send[0] = common.Type_Game
			if strings.Contains(message, "EOF") {
				log.Debugf("Received a GAMES type message - EOF")
				s.ExchangeGames.Publish("1", common.NewMessage(client.Id, common.ProtocolMessage_Control, a))
			} else {
				log.Debugf("Received a GAMES type message - DATA")
				s.ExchangeGames.Publish("1", common.NewMessage(client.Id, common.ProtocolMessage_Data, send))
			}
			log.Debugf("Forwarded to ExchangeNameGames")
		} else if rk == common.RoutingReviews {
			var eoftt uint32 = 1
			a := make([]byte, 4)
			binary.BigEndian.PutUint32(a, eoftt)
			send[0] = common.Type_Review
			if strings.Contains(message, "EOF") {
				log.Debugf("Recieved a REVIEWS type message - EOF")
				s.ExchangeReviews.Publish("1", common.NewMessage(client.Id, common.ProtocolMessage_Control, a))
			} else {
				log.Debugf("Recieved a REVIEWS type message - DATA")
				s.ExchangeReviews.Publish("1", common.NewMessage(client.Id, common.ProtocolMessage_Data, send))
			}
			log.Debugf("Forwarded to ExchangeNameReviews")
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
