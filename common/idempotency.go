package common

import "fmt"

type IdempotencyID struct {
	origin     string
	identifier uint32
}

func (id *IdempotencyID) String() string {
	return fmt.Sprintf("%s-%d", id.origin, id.identifier)
}

func (id *IdempotencyID) Serialize() []byte {
	s := NewSerializer()
	return s.WriteUint32(id.identifier).WriteString(id.origin).ToBytes()
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
		origin:     o,
		identifier: id,
	}, nil
}
