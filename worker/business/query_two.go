package business

import (
	"middleware/common"
	"middleware/worker/controller"
	"path/filepath"
	"reflect"
	"sort"
)

func Q2Filter(r *Game) bool {
	return common.ContainsCaseInsensitive(r.Categories, common.Config.GetString("queries.2.category"))
}

func Q2Map(r *Game) controller.Partitionable {
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

func NewQ2(base string, stage string, id string, top int) (*Q2, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_two", stage, id, "results"))
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

func (q *Q2) Insert(games *PlayedTime) error {
	q.state.Top = append(q.state.Top, games)

	sort.Slice(q.state.Top, func(i, j int) bool {
		return q.state.Top[i].AveragePlaytimeForever > q.state.Top[j].AveragePlaytimeForever
	})

	if len(q.state.Top) > q.state.N {
		q.state.Top = q.state.Top[:q.state.N]
	}

	_, err := q.storage.SaveState(q.state)
	if err != nil {
		return err
	}

	return nil
}

func (q *Q2) NextStage() (<-chan *PlayedTime, <-chan error) {
	ch := make(chan *PlayedTime, q.state.N) //Change this later
	ce := make(chan error, 1)
	go func() {
		defer close(ch)
		defer close(ce)

		for _, pt := range q.state.Top {
			ch <- pt
		}
	}()

	return ch, ce
}

func (q *Q2) Handle(protocolData []byte) (controller.Partitionable, error) {
	p, err := UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&PlayedTime{}) {
		return nil, q.Insert(p.(*PlayedTime))
	}
	return nil, &UnknownTypeError{}
}

func (q *Q2) Shutdown() {
	q.storage.Close()
}
