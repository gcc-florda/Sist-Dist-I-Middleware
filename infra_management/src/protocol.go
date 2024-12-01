package src

import (
	"fmt"
	"strings"
)

type RingMessageType string

const (
	ELECTION    RingMessageType = "ELE"
	COORDINATOR RingMessageType = "COO"
	HEALTHCHECK RingMessageType = "HCK"
	ALIVE       RingMessageType = "ALV"
)

type RingMessage struct {
	Type    RingMessageType
	Content string
}

func NewRingMessage(t RingMessageType, content string) *RingMessage {
	return &RingMessage{
		Type:    t,
		Content: content,
	}
}

func (rm *RingMessage) IsElection() bool {
	return rm.Type == ELECTION
}

func (rm *RingMessage) IsCoordinator() bool {
	return rm.Type == COORDINATOR
}

func (rm *RingMessage) IsHealthCheck() bool {
	return rm.Type == HEALTHCHECK
}

func (rm *RingMessage) IsAlive() bool {
	return rm.Type == ALIVE
}

func (rm *RingMessage) Serialize() string {
	log.Debugf("Serializing message: %s|%s", rm.Type, rm.Content)
	switch rm.Type {
	case ELECTION:
		return fmt.Sprintf("%s|%s", rm.Type, rm.Content)
	case COORDINATOR:
		return fmt.Sprintf("%s|%s", rm.Type, rm.Content)
	case HEALTHCHECK:
		return string(rm.Type)
	case ALIVE:
		return string(rm.Type)
	default:
		return ""
	}
}

func Deserialize(message string) (*RingMessage, error) {
	log.Debugf("Deserializing message: %s", message)
	parts := strings.Split(message, "|")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Failed deserializing message: %s", message)
	}

	switch parts[0] {
	case string(ELECTION):
		return NewRingMessage(ELECTION, parts[1]), nil
	case string(COORDINATOR):
		return NewRingMessage(COORDINATOR, parts[1]), nil
	case string(HEALTHCHECK):
		return NewRingMessage(HEALTHCHECK, ""), nil
	case string(ALIVE):
		return NewRingMessage(ALIVE, ""), nil
	default:
		return nil, fmt.Errorf("Failed deserializing message, invalid type: %s", message)
	}
}
