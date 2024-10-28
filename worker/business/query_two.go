package business

import (
	"fmt"
	"middleware/common"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
	"sort"
	"time"
)

func extractDecade(s string) (int, error) {
	parsedDate, err := time.Parse("Jan 2, 2006", s)
	if err != nil {
		return 0, nil
	}

	// Extract the year
	year := parsedDate.Year()

	// Calculate the decade
	return year / 10 * 10, nil
}

func Q2Filter(r *schema.Game) bool {
	decade, err := extractDecade(r.ReleaseDate)
	if err != nil {
		log.Error("Can't extract decade from: %s", r.ReleaseDate)
		return false
	}
	return common.ContainsCaseInsensitive(r.Genres, common.Config.GetString("query.two.category")) && decade == common.Config.GetInt("query.two.decade")
}

func Q2Map(r *schema.Game) schema.Partitionable {
	return &schema.PlayedTime{
		AveragePlaytimeForever: r.AveragePlaytimeForever,
		Name:                   r.Name,
	}
}

type Q2State struct {
	Top []*schema.PlayedTime
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
			Top: make([]*schema.PlayedTime, 0, top),
			N:   top,
		}, nil
	}

	d := common.NewDeserializer(data)

	res, err := common.ReadArray(&d, schema.PlayedTimeDeserialize)

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

func NewQ2(base string, stage string, id string, partition int, top int) (*Q2, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, fmt.Sprintf("query_two_%d", partition), stage, id, "results"))
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

func (q *Q2) Insert(games *schema.PlayedTime) error {
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

func (q *Q2) NextStage() (<-chan schema.Partitionable, <-chan error) {
	ch := make(chan schema.Partitionable, q.state.N) //Change this later
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

func (q *Q2) Handle(protocolData []byte) (schema.Partitionable, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.PlayedTime{}) {
		return nil, q.Insert(p.(*schema.PlayedTime))
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Q2) Shutdown(delete bool) {
	q.storage.Close()
	if delete {
		err := q.storage.Delete()
		if err != nil {
			log.Errorf("Action: Deleting JOIN Game File | Result: Error | Error: %s", err)
		}
	}
}
