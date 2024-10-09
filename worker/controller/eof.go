package controller

import (
	"encoding/json"
	"errors"
	"middleware/common"
	"middleware/worker/controller/enums"
	"path/filepath"
	"strconv"
)

type ControllerConfig struct {
	Type      string
	Partition *Partition
}

type Partition struct {
	Size int
}

type EOFMessage struct {
	TokenName enums.TokenName
}

func (m *EOFMessage) Serialize() []byte {
	s := common.NewSerializer()
	return s.WriteUint32(uint32(m.TokenName)).ToBytes()
}

func (m *EOFMessage) PartitionKey() string {
	return strconv.Itoa(int(m.TokenName))
}

func IsEOF(m any) bool {
	_, ok := m.(*EOFMessage)
	return ok
}

func EOFMessageFromBytes(b []byte) (*EOFMessage, error) {
	d := common.NewDeserializer(b)
	return EOFMessageDeserialize(&d)
}

func EOFMessageDeserialize(d *common.Deserializer) (*EOFMessage, error) {
	t, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	if !enums.IsValidTokenName(t) {
		return nil, errors.New("the given token name is invalid")
	}
	return &EOFMessage{
		TokenName: enums.TokenName(t),
	}, nil
}

type EOFState struct {
	Received map[enums.TokenName]uint
	storage  *common.TemporaryStorage
}

func NewEOFState(base string, id string) (*EOFState, error) {
	state := &EOFState{
		Received: make(map[enums.TokenName]uint),
	}
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, "eof_state", id, "eof.json"))
	if err != nil {
		return nil, err
	}

	b, err := s.ReadAll()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &state.Received)
	if err != nil {
		return nil, err
	}

	state.storage = s

	return state, nil
}

func (e *EOFState) Update(token enums.TokenName) error {
	e.Received[token]++
	return e.saveState()
}

func (e *EOFState) Read(token enums.TokenName) uint {
	return e.Received[token]
}

func (e *EOFState) saveState() error {
	data, err := json.MarshalIndent(e.Received, "", "  ")
	if err != nil {
		return err
	}

	_, err = e.storage.Overwrite(data)
	if err != nil {
		return err
	}

	return nil
}

type EOFChecker struct {
	Needed map[enums.TokenName]uint
	ToSend enums.TokenName
}

func matchLength[T any, S any](a []T, b []S) []S {
	lenA := len(a)
	lenB := len(b)

	if lenB > lenA {
		return b[:lenA]
	} else if lenB < lenA {
		lastElement := b[lenB-1]
		for lenB < lenA {
			b = append(b, lastElement)
			lenB++
		}
	}
	return b
}

func NewEOFChecker(controllerType string, partitionsBefore ...uint) *EOFChecker {
	n, ok := TokensNeeded[controllerType]
	if !ok {
		log.Fatalf("The Controller Type %s it's not known", controllerType)
	}
	amnt := matchLength(n, partitionsBefore)

	need := make(map[enums.TokenName]uint)
	for i, v := range n {
		need[v] = amnt[i]
	}

	s, ok := TokenToSend[controllerType]
	if !ok {
		log.Fatalf("The Controller Type %s it's not known", controllerType)
	}

	return &EOFChecker{
		Needed: need,
		ToSend: s,
	}
}

func (c *EOFChecker) AddCondition(s enums.TokenName, n uint) *EOFChecker {
	c.Needed[s] = n
	return c
}

func (c *EOFChecker) Finish(receivedEOFs map[enums.TokenName]uint) (*EOFMessage, bool) {
	for k := range c.Needed {
		if receivedEOFs[k] < c.Needed[k] {
			return nil, false
		}
	}
	return &EOFMessage{
		TokenName: c.ToSend,
	}, true
}
