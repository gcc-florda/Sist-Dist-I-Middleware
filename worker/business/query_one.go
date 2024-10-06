package business

import (
	"middleware/common"
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

func NewQ1(path string) (*Q1, error) {
	s, err := common.NewTemporaryStorage(path)

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

func (q *Q1) ToStage3() *SOCounter {
	return q.state
}

func (q *Q1) ToResult() *SOCounter {
	return q.state
}

func boolToCounter(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
