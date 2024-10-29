package common

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
)

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
		FailOnError(err, "Failed to send bytes to server")
		bytesSent += n
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

	message := make([]byte, messageLength)
	_, err = io.ReadFull(conn, message)
	if err != nil {
		log.Errorf("Failed to read message %s", err)
		return "", err
	}

	return string(message), nil
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
