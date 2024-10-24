package business

import (
	"middleware/common"
	"middleware/worker/controller"
	"path/filepath"
	"reflect"
)

type DetectLanguage func(string) bool

func Q4FilterGames(r *Game) bool {
	return common.ContainsCaseInsensitive(r.Categories, common.Config.GetString("query.four.category"))
}

func Q4FilterReviewsBuilder(isLanguage DetectLanguage) FilterReview {
	return func(r *Review) bool {
		if common.Config.GetBool("query.four.positive") {
			return r.ReviewScore > 0 && isLanguage(r.ReviewText)
		}
		return r.ReviewScore < 0 && isLanguage(r.ReviewText)
	}

}

func Q4MapGames(r *Game) controller.Partitionable {
	return &GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q4MapReviews(r *Review) controller.Partitionable {
	return &ValidReview{
		AppID: r.AppID,
	}
}

type Q4State struct {
	Over    uint32
	bufSize int
}

type Q4 struct {
	state   *Q4State
	storage *common.TemporaryStorage
}

func NewQ4(base string, id string, over int, bufSize int) (*Q4, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_four", id, "results"))
	if err != nil {
		return nil, err
	}

	return &Q4{
		state: &Q4State{
			Over:    uint32(over),
			bufSize: bufSize,
		},
		storage: s,
	}, nil
}

func (q *Q4) Insert(rc *NamedReviewCounter) error {
	if rc.Count > uint32(q.state.Over) {
		_, err := q.storage.Append(rc.Serialize())
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Q4) NextStage() (<-chan controller.Partitionable, <-chan error) {
	cr := make(chan controller.Partitionable, q.state.bufSize)
	ce := make(chan error, 1)

	go func() {
		defer close(cr)
		defer close(ce)

		q.storage.Reset()

		s, err := q.storage.Scanner()
		if err != nil {
			ce <- err
			return
		}

		for s.Scan() {
			b := s.Bytes()
			d := common.NewDeserializer(b)
			nrc, err := NamedReviewCounterDeserialize(&d)
			if err != nil {
				ce <- err
				return
			}

			cr <- nrc
		}

		if err := s.Err(); err != nil {
			ce <- err
			return
		}
	}()

	return cr, ce
}

func (q *Q4) Handle(protocolData []byte) (controller.Partitionable, error) {
	p, err := UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&NamedReviewCounter{}) {
		return nil, q.Insert(p.(*NamedReviewCounter))
	}
	return nil, &UnknownTypeError{}
}

func (q *Q4) Shutdown(delete bool) {
	q.storage.Close()
}
