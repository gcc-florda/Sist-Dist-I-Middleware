package common

import "fmt"

type IdempotencyID struct {
	origin   string
	sequence uint32
}

func (id *IdempotencyID) String() string {
	return fmt.Sprintf("%s-%d", id.origin, id.sequence)
}

func (id *IdempotencyID) Serialize() []byte {
	s := NewSerializer()
	return s.WriteUint32(id.sequence).WriteString(id.origin).ToBytes()
}

func IdempotencyIDDeserialize(d *Deserializer) (*IdempotencyID, error) {
	id, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	o, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	return &IdempotencyID{
		origin:   o,
		sequence: id,
	}, nil
}
