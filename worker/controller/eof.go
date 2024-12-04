package controller

import (
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
	storage  *common.IdempotencyHandlerSingleFile[*EOFMessage]
}

func eofReadState(h *common.IdempotencyHandlerSingleFile[*EOFMessage]) (map[enums.TokenName]uint, error) {
	m := make(map[enums.TokenName]uint)
	_, err := h.LoadSequentialState(EOFMessageDeserialize, func(e1, e2 *EOFMessage) *EOFMessage {
		m[e2.TokenName]++
		return e2
	}, nil)
	return m, err
}

func NewEOFState(base string, id string) (*EOFState, error) {
	s, err := common.NewIdempotencyHandlerSingleFile[*EOFMessage](filepath.Join(".", base, "eof_state", id, "eof.json"))
	if err != nil {
		return nil, err
	}

	b, err := eofReadState(s)
	if err != nil {
		return nil, err
	}
	return &EOFState{
		Received: b,
		storage:  s,
	}, nil
}

func (e *EOFState) Update(token enums.TokenName, idempotencyId *common.IdempotencyID) bool {
	if e.storage.AlreadyProcessed(idempotencyId) {
		return false
	}
	e.Received[token]++
	return true
}

func (e *EOFState) Read(token enums.TokenName) uint {
	return e.Received[token]
}

func (e *EOFState) SaveState(token enums.TokenName, idempotencyId *common.IdempotencyID) error {
	return e.storage.SaveState(idempotencyId, &EOFMessage{TokenName: token})
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
