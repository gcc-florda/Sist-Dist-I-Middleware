package common

import "fmt"

type IdempotencyID struct {
	Origin   string
	Sequence uint32
}

func (id *IdempotencyID) String() string {
	return fmt.Sprintf("%s-%d", id.Origin, id.Sequence)
}

func (id *IdempotencyID) Serialize() []byte {
	s := NewSerializer()
	return s.WriteUint32(id.Sequence).WriteString(id.Origin).ToBytes()
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
		Origin:   o,
		Sequence: id,
	}, nil
}
