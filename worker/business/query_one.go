package business

import (
	"middleware/common"
	"path/filepath"
	"reflect"
)

func Q1Map(r *Game) *SOCounter {
	return &SOCounter{
		Windows: boolToCounter(r.Windows),
		Linux:   boolToCounter(r.Linux),
		Mac:     boolToCounter(r.Mac),
	}
}

func q1StateFromBytes(data []byte) (*SOCounter, error) {
	if len(data) == 0 {
		return &SOCounter{}, nil
	}

	d := common.NewDeserializer(data)

	return SOCounterDeserialize(&d)
}

type Q1 struct {
	state   *SOCounter
	storage *common.TemporaryStorage
}

func NewQ1(base string, id string, stage string) (*Q1, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_one", stage, id, "results"))

	if err != nil {
		return nil, err
	}

	diskState, err := s.ReadAll()

	if err != nil {
		return nil, err
	}

	state, err := q1StateFromBytes(diskState)

	if err != nil {
		return nil, err
	}

	return &Q1{
		state:   state,
		storage: s,
	}, nil
}

func (q *Q1) Count(r *SOCounter) error {
	q.state.Windows += r.Windows
	q.state.Linux += r.Linux
	q.state.Mac += r.Mac

	_, err := q.storage.SaveState(q.state)
	if err != nil {
		return err
	}
	return nil
}

func boolToCounter(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

func (q *Q1) NextStage() (<-chan *SOCounter, <-chan error) {
	ch := make(chan *SOCounter, 1) //Change this later
	ce := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(ce)

		ch <- q.state
	}()

	return ch, ce
}

func (q *Q1) Handle(protocolData []byte) error {
	p, err := UnmarshalMessage(protocolData)
	if err != nil {
		return err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&SOCounter{}) {
		return q.Count(p.(*SOCounter))
	}
	return &UnknownTypeError{}
}

func (q *Q1) Shutdown() {
	q.storage.Close()
}
