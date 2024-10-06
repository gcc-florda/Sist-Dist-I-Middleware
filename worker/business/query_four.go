package business

import (
	"middleware/common"
	"middleware/common/utils"
)

func Q4FilterGames(r *Game, cat string) bool {
	return utils.Contains(r.Categories, cat)
}

func Q4FilterReviews(r *Review, pos bool) bool {
	if pos {
		return r.ReviewScore > 0
	}
	return r.ReviewScore < 0
}

func Q4MapGames(r *Game) *GameName {
	return &GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q4MapReviews(r *Review) *ValidReview {
	return &ValidReview{
		AppID: r.AppID,
	}
}

type Q4State struct {
	N       int
	bufSize int
}

type Q4 struct {
	state         *Q4State
	reviewStorage *common.TemporaryStorage
	gameStorage   *common.TemporaryStorage
}

func NewQ4(path string, top int, bufSize int) (*Q4, error) {
	r, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	g, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	return &Q4{
		state: &Q4State{
			N:       top,
			bufSize: bufSize,
		},
		reviewStorage: r,
		gameStorage:   g,
	}, nil
}

func (q *Q4) AddReview(r *ValidReview) error {
	err := q.addReview(r.AppID)
	if err != nil {
		return err
	}
	return nil
}

func (q *Q4) AddGame(r *GameName) error {
	_, err := q.gameStorage.Append(r.Serialize())
	if err != nil {
		return err
	}
	return nil
}

func (q *Q4) addReview(appId string) error {
	q.reviewStorage.Reset()
	var offset int64 = 0
	file, err := q.reviewStorage.File()
	if err != nil {
		return err
	}
	scanner, err := q.reviewStorage.Scanner()
	if err != nil {
		return err
	}
	for scanner.Scan() {
		b := scanner.Bytes()
		lb := len(b) + 1
		d := common.NewDeserializer(b)
		l, err := ReviewCounterDeserialize(&d)
		if err != nil {
			return err
		}

		if l.AppID != appId {
			offset += int64(lb)
			continue
		}

		l.Count += 1

		nb := append(l.Serialize(), '\n')

		_, err = file.Seek(offset, 0)
		if err != nil {
			return err
		}

		_, err = file.Write(nb)
		if err != nil {
			return err
		}

		return nil
	}

	// Check for any error during scanning
	if err := scanner.Err(); err != nil {
		return err
	}

	// We got here because we didn't find the review already written
	c := &ReviewCounter{
		AppID: appId,
		Count: 1,
	}
	b := c.Serialize()

	_, err = file.Write(append(b, '\n'))
	if err != nil {
		return err
	}

	return nil
}

func (q *Q4) ToStage3() (chan *NamedReviewCounter, chan error) {

	cache := common.NewJoinCache[string, *ReviewCounter](q.state.bufSize)
	cr := make(chan *NamedReviewCounter, q.state.bufSize)
	ce := make(chan error, 1)
	go func() {
		q.gameStorage.Reset()
		q.reviewStorage.Reset()

		gss, err := q.gameStorage.Scanner()
		if err != nil {
			ce <- err
			return
		}
		rss, err := q.reviewStorage.Scanner()
		if err != nil {
			ce <- err
			return
		}

		nextReview := func() (*ReviewCounter, error) {
			if rss.Scan() {
				b := rss.Bytes()
				d := common.NewDeserializer(b)
				r, err := ReviewCounterDeserialize(&d)
				if err != nil {
					return nil, err
				}
				return r, nil
			}
			return nil, nil
		}

		tryJoin := func(g *GameName) error {
			v, ok := cache.Get(g.AppID)
			if ok {
				cache.Remove(g.AppID)
				nrc := &NamedReviewCounter{
					Name:  g.Name,
					Count: v.Count,
				}
				cr <- nrc
				return nil
			}
			for {
				r, err := nextReview()
				if err != nil {
					return err
				}
				if r == nil {
					break
				}

				if r.AppID == g.AppID {
					nrc := &NamedReviewCounter{
						Name:  g.Name,
						Count: r.Count,
					}
					cr <- nrc
					return nil
				} else {
					cache.TryPut(r.AppID, r)
				}
			}
			return nil
		}

		for gss.Scan() {
			q.reviewStorage.Reset()

			b := gss.Bytes()
			d := common.NewDeserializer(b)
			gn, err := GameNameDeserialize(&d)
			if err != nil {
				ce <- err
				return
			}
			err = tryJoin(gn)
			if err != nil {
				ce <- err
				return
			}
		}

	}()

	return cr, ce
}

type Q4AfterJoinState struct {
	Over    uint32
	bufSize int
}

type Q4AfterJoin struct {
	state   *Q4AfterJoinState
	storage *common.TemporaryStorage
}

func NewQ4AfterJoin(path string, over int, bufSize int) (*Q4AfterJoin, error) {
	s, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	return &Q4AfterJoin{
		state: &Q4AfterJoinState{
			Over:    uint32(over),
			bufSize: bufSize,
		},
		storage: s,
	}, nil
}

func (q *Q4AfterJoin) Insert(rc *NamedReviewCounter) error {
	if rc.Count > uint32(q.state.Over) {
		_, err := q.storage.Append(rc.Serialize())
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Q4AfterJoin) ToResult() (chan *NamedReviewCounter, chan error) {
	cr := make(chan *NamedReviewCounter, q.state.bufSize)
	ce := make(chan error, 1)

	go func() {
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
