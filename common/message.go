package common

import (
	"errors"

	"github.com/google/uuid"
)

type JobID = uuid.UUID

const (
	ProtocolMessage_Data uint8 = iota
	ProtocolMessage_Control
)

type Message struct {
	JobId         uuid.UUID
	_type         uint8
	IdempotencyID *IdempotencyID
	Content       []byte
}

func NewMessage(jobId uuid.UUID, idempotencyID *IdempotencyID, t uint8, content []byte) *Message {
	return &Message{
		JobId:         jobId,
		_type:         t,
		IdempotencyID: idempotencyID,
		Content:       content,
	}
}

func (m *Message) Serialize() []byte {
	s := NewSerializer()
	return s.WriteUUID(m.JobId).WriteUint8(m._type).WriteBytes(m.IdempotencyID.Serialize()).WriteBytes(m.Content).ToBytes()
}

func MessageFromBytes(raw []byte) (*Message, error) {
	d := NewDeserializer(raw)
	id, t, idemId, err := messageDeserialize(&d)
	if err != nil {
		return nil, err
	}

	return &Message{
		JobId:         id,
		_type:         t,
		IdempotencyID: idemId,
		Content:       raw[len(raw)-d.Buf.Len():],
	}, nil
}

func messageDeserialize(d *Deserializer) (uuid.UUID, uint8, *IdempotencyID, error) {
	id, err := d.ReadUUID()
	if err != nil {
		return id, 0, nil, err
	}

	t, err := d.ReadUint8()
	if err != nil {
		return id, 0, nil, err
	}

	if t != ProtocolMessage_Data && t != ProtocolMessage_Control {
		return id, t, nil, errors.New("the read message from the protocol is not of a known type")
	}

	idemId, err := IdempotencyIDDeserialize(d)
	if err != nil {
		return id, t, nil, err
	}

	return id, t, idemId, nil
}

func (pm *Message) JobID() JobID {
	return pm.JobId
}

func (pm *Message) IdemID() *IdempotencyID {
	return pm.IdempotencyID
}

func (pm *Message) IsEOF() bool {
	return pm._type == ProtocolMessage_Control
}

func (pm *Message) Data() []byte {
	return pm.Content
}
