package business

import (
	"fmt"
	"middleware/common"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
)

type DetectLanguage func(string) bool

func Q4FilterGames(r *schema.Game) bool {
	return common.ContainsCaseInsensitive(r.Genres, common.Config.GetString("query.four.category"))
}

func Q4FilterReviewsBuilder(isLanguage DetectLanguage) FilterReview {
	return func(r *schema.Review) bool {
		if common.Config.GetBool("query.four.positive") {
			return r.ReviewScore > 0 && isLanguage(r.ReviewText)
		}
		return r.ReviewScore < 0 && isLanguage(r.ReviewText)
	}

}

func Q4MapGames(r *schema.Game) schema.Partitionable {
	return &schema.GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q4MapReviews(r *schema.Review) schema.Partitionable {
	return &schema.ValidReview{
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

func NewQ4(base string, id string, partition int, over int, bufSize int) (*Q4, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, fmt.Sprintf("query_four_%d", partition), id, "results"))
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

func (q *Q4) Insert(rc *schema.NamedReviewCounter) error {
	if rc.Count > uint32(q.state.Over) {
		_, err := q.storage.Append(rc.Serialize())
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Q4) NextStage() (<-chan schema.Partitionable, <-chan error) {
	cr := make(chan schema.Partitionable, q.state.bufSize)
	ce := make(chan error, 1)

	go func() {
		defer close(cr)
		defer close(ce)

		q.storage.Reset()

		s, err := q.storage.ScannerDeserialize(func(d *common.Deserializer) error {
			_, err := schema.NamedReviewCounterDeserialize(d)
			return err
		})
		if err != nil {
			ce <- err
			return
		}

		for s.Scan() {
			b := s.Bytes()
			d := common.NewDeserializer(b)
			nrc, err := schema.NamedReviewCounterDeserialize(&d)
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

func (q *Q4) Handle(protocolData []byte) (schema.Partitionable, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.NamedReviewCounter{}) {
		return nil, q.Insert(p.(*schema.NamedReviewCounter))
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Q4) Shutdown(delete bool) {
	q.storage.Close()
}
