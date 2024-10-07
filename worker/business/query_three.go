package business

import (
	"middleware/common"
	"middleware/common/utils"
	"sort"
)

func Q3FilterGames(r *Game, cat string) bool {
	return utils.Contains(r.Categories, cat)
}

func Q3FilterReviews(r *Review, pos bool) bool {
	if pos {
		return r.ReviewScore > 0
	}
	return r.ReviewScore < 0
}

func Q3MapGames(r *Game) *GameName {
	return &GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q3MapReviews(r *Review) *ValidReview {
	return &ValidReview{
		AppID: r.AppID,
	}
}

type Q3State struct {
	Top []*NamedReviewCounter
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

func NewQ3(path string, top int) (*Q3, error) {
	s, err := common.NewTemporaryStorage(path)
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

func q3StateFromBytes(data []byte) ([]*NamedReviewCounter, error) {
	if len(data) == 0 {
		return make([]*NamedReviewCounter, 0), nil
	}

	d := common.NewDeserializer(data)

	return common.ReadArray(&d, NamedReviewCounterDeserialize)
}

func (q *Q3) Insert(rc *NamedReviewCounter) error {
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

func (q *Q3) ToResult() []*NamedReviewCounter {
	return q.state.Top
}
