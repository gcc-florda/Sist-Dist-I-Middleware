package common

import (
	"bytes"
	"encoding/binary"
)

type Serializable interface {
	Serialize() []byte
}

type Deserialize[T any] func([]byte) (T, error)

type Serializer struct {
	buf bytes.Buffer
}

func NewSerializer() Serializer {
	var buf bytes.Buffer
	return Serializer{
		buf,
	}
}

func (s *Serializer) WriteString(str string) *Serializer {
	s.WriteUint32((uint32(len(str))))
	s.buf.WriteString(str)
	return s
}

func (s *Serializer) WriteInt32(n int32) *Serializer {
	binary.Write(&s.buf, binary.BigEndian, n)
	return s
}

func (s *Serializer) WriteUint32(n uint32) *Serializer {
	binary.Write(&s.buf, binary.BigEndian, n)
	return s
}

func (s *Serializer) WriteFloat64(n float64) *Serializer {
	binary.Write(&s.buf, binary.BigEndian, n)
	return s
}

func (s *Serializer) WriteUint8(n uint8) *Serializer {
	binary.Write(&s.buf, binary.BigEndian, n)
	return s
}

func (s *Serializer) WriteBytes(b []byte) *Serializer {
	s.buf.Write(b)
	return s
}

func (s *Serializer) WriteBool(b bool) *Serializer {
	if b {
		return s.WriteUint8(1)
	}
	return s.WriteUint8(0)
}

func (s *Serializer) ToBytes() []byte {
	return s.buf.Bytes()
}

func (s *Serializer) WriteArray(serializables []Serializable) *Serializer {
	l := uint32(len(serializables))
	s.WriteUint32(l)
	for _, ser := range serializables {
		s.WriteBytes(ser.Serialize())
	}
	return s
}

type Deserializer struct {
	buf *bytes.Buffer
}

func NewDeserializer(data []byte) Deserializer {
	return Deserializer{
		buf: bytes.NewBuffer(data),
	}
}

func (d *Deserializer) ReadString() (string, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return "", err
	}
	str := make([]byte, length)
	if _, err := d.buf.Read(str); err != nil {
		return "", err
	}
	return string(str), nil
}

func (d *Deserializer) ReadUint32() (uint32, error) {
	var n uint32
	if err := binary.Read(d.buf, binary.BigEndian, &n); err != nil {
		return 0, err
	}
	return n, nil
}

func (d *Deserializer) ReadInt32() (int32, error) {
	var n int32
	if err := binary.Read(d.buf, binary.BigEndian, &n); err != nil {
		return 0, err
	}
	return n, nil
}

func (d *Deserializer) ReadFloat64() (float64, error) {
	var n float64
	if err := binary.Read(d.buf, binary.BigEndian, &n); err != nil {
		return 0, err
	}
	return n, nil
}

func (d *Deserializer) ReadUint8() (uint8, error) {
	var n uint8
	if err := binary.Read(d.buf, binary.BigEndian, &n); err != nil {
		return 0, err
	}
	return n, nil
}

func (d *Deserializer) ReadBool() (bool, error) {
	b, err := d.ReadUint8()
	if err != nil {
		return false, err
	}
	return b > 0, err
}

func ReadArray[T any](d *Deserializer, f func(*Deserializer) (T, error)) ([]T, error) {
	l, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	r := make([]T, l)
	for i := uint32(0); i < l; i++ {
		o, err := f(d)
		if err != nil {
			return nil, err
		}
		r[i] = o
	}
	return r, nil
}

func (d *Deserializer) ReadArrayLen()
