package business

import (
	"middleware/common"
	"middleware/common/utils"
	"path/filepath"
	"reflect"
)

func Q5FilterGames(r *Game, cat string) bool {
	return utils.Contains(r.Categories, cat)
}

func Q5FilterReviews(r *Review, pos bool) bool {
	if pos {
		return r.ReviewScore > 0
	}
	return r.ReviewScore < 0
}

func Q5MapGames(r *Game) *GameName {
	return &GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q5MapReviews(r *Review) *ValidReview {
	return &ValidReview{
		AppID: r.AppID,
	}
}

type Q5State struct {
	PercentileOver uint32
	bufSize        int
}

type Q5 struct {
	state   *Q5State
	storage *common.TemporaryStorage
}

func NewQ5(base string, id string, pctOver int, bufSize int) (*Q5, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_five", id, "results"))
	if err != nil {
		return nil, err
	}

	return &Q5{
		state: &Q5State{
			PercentileOver: uint32(pctOver),
			bufSize:        bufSize,
		},
		storage: s,
	}, nil
}

func (q *Q5) Insert(rc *NamedReviewCounter) error {
	_, err := q.storage.Append(rc.Serialize())
	if err != nil {
		return err
	}

	return nil
}

func (q *Q5) NextStage() (chan *NamedReviewCounter, chan error) {
	cr := make(chan *NamedReviewCounter, q.state.bufSize)
	ce := make(chan error, 1)

	go func() {
		defer close(cr)
		defer close(ce)

		// q.storage.Reset()

		// s, err := q.storage.Scanner()
		// if err != nil {
		// 	ce <- err
		// 	return
		// }

		// for s.Scan() {
		// 	b := s.Bytes()
		// 	d := common.NewDeserializer(b)
		// 	nrc, err := NamedReviewCounterDeserialize(&d)
		// 	if err != nil {
		// 		ce <- err
		// 		return
		// 	}

		// 	cr <- nrc
		// }

		// if err := s.Err(); err != nil {
		// 	ce <- err
		// 	return
		// }
	}()

	return cr, ce
}

func (q *Q5) Handle(protocolData []byte) error {
	p, err := UnmarshalMessage(protocolData)
	if err != nil {
		return err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&NamedReviewCounter{}) {
		return q.Insert(p.(*NamedReviewCounter))
	}
	return &UnknownTypeError{}
}

func (q *Q5) Shutdown() {
	q.storage.Close()
}
