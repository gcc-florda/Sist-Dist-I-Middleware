package business

import (
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

func Q3FilterGames(r *schema.Game) bool {
	decade, err := extractDecade(r.ReleaseDate)
	if err != nil {
		log.Error("Can't extract decade from: %s", r.ReleaseDate)
		return false
	}
	return common.ContainsCaseInsensitive(r.Categories, common.Config.GetString("query.three.category")) && decade == common.Config.GetInt("query.three.decade")
}

func Q3FilterReviews(r *schema.Review) bool {
	if common.Config.GetBool("query.three.positive") {
		return r.ReviewScore > 0
	}
	return r.ReviewScore < 0
}

func Q3MapGames(r *schema.Game) schema.Partitionable {
	return &schema.GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q3MapReviews(r *schema.Review) schema.Partitionable {
	return &schema.ValidReview{
		AppID: r.AppID,
	}
}

type Q3State struct {
	Top []*schema.NamedReviewCounter
	N   int
}

func (s *Q3State) Serialize() []byte {
	se := common.NewSerializer()
	serializables := make([]common.Serializable, len(s.Top))
	for i, pt := range s.Top {
		serializables[i] = pt
	}
	return se.WriteArray(serializables).ToBytes()
}

type Q3 struct {
	state   *Q3State
	storage *common.TemporaryStorage
}

func NewQ3(base string, id string, top int) (*Q3, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_three", id, "results"))
	if err != nil {
		return nil, err
	}

	diskState, err := s.ReadAll()

	if err != nil {
		return nil, err
	}

	state, err := q3StateFromBytes(diskState)

	if err != nil {
		return nil, err
	}

	return &Q3{
		state: &Q3State{
			Top: state,
			N:   top,
		},
		storage: s,
	}, nil
}

func q3StateFromBytes(data []byte) ([]*schema.NamedReviewCounter, error) {
	if len(data) == 0 {
		return make([]*schema.NamedReviewCounter, 0), nil
	}

	d := common.NewDeserializer(data)

	return common.ReadArray(&d, schema.NamedReviewCounterDeserialize)
}

func (q *Q3) Insert(rc *schema.NamedReviewCounter) error {
	q.state.Top = append(q.state.Top, rc)

	sort.Slice(q.state.Top, func(i, j int) bool {
		return q.state.Top[i].Count > q.state.Top[j].Count
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

func (q *Q3) NextStage() (<-chan schema.Partitionable, <-chan error) {
	ch := make(chan schema.Partitionable, q.state.N)
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

func (q *Q3) Handle(protocolData []byte) (schema.Partitionable, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.NamedReviewCounter{}) {
		return nil, q.Insert(p.(*schema.NamedReviewCounter))
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Q3) Shutdown(delete bool) {
	q.storage.Close()
	if delete {
		err := q.storage.Delete()
		if err != nil {
			log.Errorf("Error while deleting the file: %s", err)
		}
	}
}
