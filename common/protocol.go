package common

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"time"
)

const (
	GAMES           = "GAM"
	REVIEWS         = "REV"
	AskForResults   = "RES"
	Results_Q1      = "Q1"
	Results_Q2      = "Q2"
	Results_Q3      = "Q3"
	Results_Q4      = "Q4"
	Results_Q5      = "Q5"
	CloseConnection = "CLC"
	EndWithResults  = "EWR"
	EOF             = "EOF"
)

const (
	Type_GAMES = iota
	Type_REVIEWS
	Type_AskForResults
	Type_Results_Q1
	Type_Results_Q2
	Type_Results_Q3
	Type_Results_Q4
	Type_Results_Q5
	Type_CloseConnection
	Type_EndWithResults
	Type_EOF
)

type ClientMessage struct {
	Content string
	Type    int
}

func (cm ClientMessage) IsEOF() bool {
	return cm.Content == EOF
}

func (cm ClientMessage) IsEndWithResults() bool {
	return cm.Type == Type_EndWithResults
}

func (cm ClientMessage) IsQueryResult() bool {
	return cm.Type == Type_Results_Q1 || cm.Type == Type_Results_Q2 || cm.Type == Type_Results_Q3 || cm.Type == Type_Results_Q4 || cm.Type == Type_Results_Q5
}

func (cm ClientMessage) SerializeClientMessage() (string, error) {
	switch cm.Type {
	case Type_GAMES:
		return GAMES + "|" + cm.Content + "\n", nil
	case Type_REVIEWS:
		return REVIEWS + "|" + cm.Content + "\n", nil
	case Type_AskForResults:
		return AskForResults + "|" + cm.Content + "\n", nil
	case Type_Results_Q1:
		return Results_Q1 + "|" + cm.Content + "\n", nil
	case Type_Results_Q2:
		return Results_Q2 + "|" + cm.Content + "\n", nil
	case Type_Results_Q3:
		return Results_Q3 + "|" + cm.Content + "\n", nil
	case Type_Results_Q4:
		return Results_Q4 + "|" + cm.Content + "\n", nil
	case Type_Results_Q5:
		return Results_Q5 + "|" + cm.Content + "\n", nil
	case Type_CloseConnection:
		return CloseConnection + "|" + cm.Content + "\n", nil
	case Type_EndWithResults:
		return EndWithResults + "|" + cm.Content + "\n", nil
	}

	return "", errors.New("invalid message type")
}

func DeserializeClientMessage(message string) (ClientMessage, error) {
	msg_splitted := strings.SplitN(message, "|", 2)

	msg_type := msg_splitted[0]
	msg_content := msg_splitted[1]

	switch msg_type {
	case GAMES:
		return ClientMessage{msg_content, Type_GAMES}, nil
	case REVIEWS:
		return ClientMessage{msg_content, Type_REVIEWS}, nil
	case AskForResults:
		return ClientMessage{msg_content, Type_AskForResults}, nil
	case Results_Q1:
		return ClientMessage{msg_content, Type_Results_Q1}, nil
	case Results_Q2:
		return ClientMessage{msg_content, Type_Results_Q2}, nil
	case Results_Q3:
		return ClientMessage{msg_content, Type_Results_Q3}, nil
	case Results_Q4:
		return ClientMessage{msg_content, Type_Results_Q4}, nil
	case Results_Q5:
		return ClientMessage{msg_content, Type_Results_Q5}, nil
	case CloseConnection:
		return ClientMessage{msg_content, Type_CloseConnection}, nil
	case EndWithResults:
		return ClientMessage{msg_content, Type_EndWithResults}, nil
	}
	return ClientMessage{}, errors.New("invalid message type")
}

func Send(message string, conn net.Conn) error {
	messageBytes := []byte(message)

	buffer := new(bytes.Buffer)

	err := binary.Write(buffer, binary.BigEndian, uint32(len(messageBytes)))

	if err != nil {
		log.Errorf("Failed to write message length to buffer %s", err)
		return err
	}

	err = binary.Write(buffer, binary.BigEndian, messageBytes)

	if err != nil {
		log.Errorf("Failed to write message to buffer %s", err)
		return err
	}

	messageLength := buffer.Len()
	bytesSent := 0

	for bytesSent < messageLength {
		n, err := conn.Write(buffer.Bytes())
		if err != nil {
			log.Errorf("Failed to send bytes to %s: %s", conn.LocalAddr().String(), err)
			return err
		}
		bytesSent += n
	}

	return nil
}

func SendWithRetry(message string, conn net.Conn, retries int) error {
	for i := 0; i < retries; i++ {
		err := Send(message, conn)

		if err == nil {
			return nil
		}

		if i == retries-1 {
			return err
		}

		time.Sleep(5 * time.Second)
	}

	return nil
}

func Receive(conn net.Conn) (string, error) {
	lengthBuffer := make([]byte, 4)
	_, err := io.ReadFull(conn, lengthBuffer)
	if err != nil {
		log.Errorf("Failed to read message %s", err)
		return "", err
	}

	messageLength := binary.BigEndian.Uint32(lengthBuffer)

	messageBytes := make([]byte, messageLength)
	_, err = io.ReadFull(conn, messageBytes)
	if err != nil {
		log.Errorf("Failed to read message %s", err)
		return "", err
	}

	messageString := strings.Trim(string(messageBytes), "\n")

	return messageString, nil
}

func GetRoutingKey(line string) string {
	lineType := int(line[0] - '0')

	if lineType == TypeGame {
		return RoutingGames
	} else if lineType == TypeReview {
		return RoutingReviews
	}

	panic("ni idea man")
}

type ManagementMessage struct {
	Content string
}

func (mm ManagementMessage) IsName() bool {
	return !mm.IsAlive() && !mm.IsHealthCheck()
}

func (mm ManagementMessage) IsAlive() bool {
	return mm.Content == "ALV"
}

func (mm ManagementMessage) IsHealthCheck() bool {
	return mm.Content == "HCK"
}
