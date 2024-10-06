package business

import (
	"middleware/common"
	"middleware/common/utils"
	"sort"
)

func Q2Filter(r *Game, cat string) bool {
	return utils.Contains(r.Categories, cat)
}

func Q2Map(r *Game) *PlayedTime {
	return &PlayedTime{
		AveragePlaytimeForever: r.AveragePlaytimeForever,
		Name:                   r.Name,
	}
}

type Q2State struct {
	Top []*PlayedTime
	N   int
}

func (s *Q2State) Serialize() []byte {
	se := common.NewSerializer()
	serializables := make([]common.Serializable, len(s.Top))
	for i, pt := range s.Top {
		serializables[i] = pt
	}
	return se.WriteArray(serializables).ToBytes()
}

func q2StateFromBytes(data []byte, top int) (*Q2State, error) {
	if len(data) == 0 {
		return &Q2State{
			Top: make([]*PlayedTime, 0, top),
			N:   top,
		}, nil
	}

	d := common.NewDeserializer(data)

	res, err := common.ReadArray(&d, PlayedTimeDeserialize)

	if err != nil {
		return nil, err
	}

	return &Q2State{
		Top: res,
		N:   top,
	}, nil
}

type Q2 struct {
	state   *Q2State
	storage *common.TemporaryStorage
}

func NewQ2(path string, top int) (*Q2, error) {
	s, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	diskState, err := s.ReadAll()

	if err != nil {
		return nil, err
	}

	state, err := q2StateFromBytes(diskState, top)

	if err != nil {
		return nil, err
	}

	return &Q2{
		state:   state,
		storage: s,
	}, nil
}

func (q *Q2) Insert(games []*PlayedTime) error {
	q.state.Top = append(q.state.Top, games...)

	sort.Slice(q.state.Top, func(i, j int) bool {
		return q.state.Top[i].AveragePlaytimeForever > q.state.Top[j].AveragePlaytimeForever
	})

	if len(q.state.Top) > q.state.N {
		q.state.Top = q.state.Top[:q.state.N]
	}

	q.storage.SaveState(q.state)
	_, err := q.storage.SaveState(q.state)
	if err != nil {
		return err
	}

	return nil
}

func (q *Q2) ToStage3() []*PlayedTime {
	return q.state.Top
}

func (q *Q2) ToResult() []*PlayedTime {
	return q.state.Top
}
