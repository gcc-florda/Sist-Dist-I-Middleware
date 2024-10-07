package common

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
)

func Send(message string, conn net.Conn) {
	messageBytes := []byte(message)

	buffer := new(bytes.Buffer)

	err := binary.Write(buffer, binary.BigEndian, uint32(len(messageBytes)))
	FailOnError(err, "Failed to write message length to buffer")

	err = binary.Write(buffer, binary.BigEndian, messageBytes)
	FailOnError(err, "Failed to write message to buffer")

	messageLength := buffer.Len()
	bytesSent := 0

	for bytesSent < messageLength {
		n, err := conn.Write(buffer.Bytes())
		FailOnError(err, "Failed to send bytes to server")
		bytesSent += n
	}

	log.Debugf("Sent message via TCP: %s", message)
}

func Receive(conn net.Conn) string {
	lengthBuffer := make([]byte, 4)
	_, err := io.ReadFull(conn, lengthBuffer)
	FailOnError(err, "Failed to read message length")

	messageLength := binary.BigEndian.Uint32(lengthBuffer)

	message := make([]byte, messageLength)
	_, err = io.ReadFull(conn, message)
	FailOnError(err, "Failed to read message")

	log.Debugf("Received message via TCP: %s", message)

	return string(message)
}

func GetRoutingKey(line string) string {
	lineType := int(line[0] - '0')

	log.Debugf("Routing key for line[0]: %s", line[0])
	log.Debugf("Routing key for line: %v", lineType)

	if lineType == TypeGame {
		return RoutingGames
	} else if lineType == TypeReview {
		return RoutingReviews
	}

	return RoutingProtocol
}
