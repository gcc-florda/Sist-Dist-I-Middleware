package business

import (
	"middleware/common"
	"middleware/common/utils"
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
	PercentilOver uint32
	bufSize       int
}

type Q5 struct {
	state   *Q5State
	storage *common.TemporaryStorage
}

func NewQ5(path string, pctOver int, bufSize int) (*Q5, error) {
	s, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	return &Q5{
		state: &Q5State{
			PercentilOver: uint32(pctOver),
			bufSize:       bufSize,
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
