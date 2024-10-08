package common

import "github.com/google/uuid"

type Message struct {
	JobId   uuid.UUID
	Content string
}

func NewMessage(jobId uuid.UUID, content string) Message {
	return Message{
		JobId:   jobId,
		Content: content,
	}
}

func (m Message) Serialize() []byte {
	s := NewSerializer()
	return s.
		WriteUUID(m.JobId).
		WriteString(m.Content).
		ToBytes()
}
