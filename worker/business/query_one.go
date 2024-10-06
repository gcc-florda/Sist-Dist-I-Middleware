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

type Q1State struct {
	Windows uint32
	Linux   uint32
	Mac     uint32
}

func q1StateFromBytes(data []byte) (*Q1State, error) {
	if len(data) == 0 {
		return &Q1State{}, nil
	}

	d := common.NewDeserializer(data)

	windows, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	linux, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	mac, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	return &Q1State{
		Windows: windows,
		Linux:   linux,
		Mac:     mac,
	}, nil
}

func (s *Q1State) Serialize() []byte {
	se := common.NewSerializer()
	return se.WriteUint32(s.Windows).WriteUint32(s.Linux).WriteUint32(s.Mac).ToBytes()
}

type Q1 struct {
	state   *Q1State
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

func boolToCounter(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
