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
	N       int
	bufSize int
}

type Q3 struct {
	state         *Q3State
	reviewStorage *common.TemporaryStorage
	gameStorage   *common.TemporaryStorage
}

func NewQ3(path string, top int, bufSize int) (*Q3, error) {
	r, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	g, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	return &Q3{
		state: &Q3State{
			N:       top,
			bufSize: bufSize,
		},
		reviewStorage: r,
		gameStorage:   g,
	}, nil
}

func (q *Q3) AddReview(r *ValidReview) error {
	err := q.addReview(r.AppID)
	if err != nil {
		return err
	}
	return nil
}

func (q *Q3) AddGame(r *GameName) error {
	_, err := q.gameStorage.Append(r.Serialize())
	if err != nil {
		return err
	}
	return nil
}

func (q *Q3) addReview(appId string) error {
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

func (q *Q3) ToStage3() (chan *NamedReviewCounter, chan error) {

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

type Q3AfterJoinState struct {
	Top []*NamedReviewCounter
	N   int
}

func (s *Q3AfterJoinState) Serialize() []byte {
	se := common.NewSerializer()
	serializables := make([]common.Serializable, len(s.Top))
	for i, pt := range s.Top {
		serializables[i] = pt
	}
	return se.WriteArray(serializables).ToBytes()
}

type Q3AfterJoin struct {
	state   *Q3AfterJoinState
	storage *common.TemporaryStorage
}

func NewQ3AfterJoin(path string, top int) (*Q3AfterJoin, error) {
	s, err := common.NewTemporaryStorage(path)
	if err != nil {
		return nil, err
	}

	diskState, err := s.ReadAll()

	if err != nil {
		return nil, err
	}

	state, err := q3AfterJoinStateFromBytes(diskState)

	if err != nil {
		return nil, err
	}

	return &Q3AfterJoin{
		state: &Q3AfterJoinState{
			Top: state,
			N:   top,
		},
		storage: s,
	}, nil
}

func q3AfterJoinStateFromBytes(data []byte) ([]*NamedReviewCounter, error) {
	if len(data) == 0 {
		return make([]*NamedReviewCounter, 0), nil
	}

	d := common.NewDeserializer(data)

	return common.ReadArray(&d, NamedReviewCounterDeserialize)
}

func (q *Q3AfterJoin) Insert(rc *NamedReviewCounter) error {
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

func (q *Q3AfterJoin) ToResult() []*NamedReviewCounter {
	return q.state.Top
}
