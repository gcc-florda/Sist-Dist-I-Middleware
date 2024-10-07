package business

import "middleware/common"

type JoinState struct {
	bufSize int
}

type Join struct {
	state         *JoinState
	reviewStorage *common.TemporaryStorage
	gameStorage   *common.TemporaryStorage
}

func NewJoin(reviewPath string, gamePath string, bufSize int) (*Join, error) {
	r, err := common.NewTemporaryStorage(reviewPath)
	if err != nil {
		return nil, err
	}

	g, err := common.NewTemporaryStorage(gamePath)
	if err != nil {
		return nil, err
	}

	return &Join{
		state: &JoinState{
			bufSize: bufSize,
		},
		reviewStorage: r,
		gameStorage:   g,
	}, nil
}

func (q *Join) AddReview(r *ValidReview) error {
	err := q.addReview(r.AppID)
	if err != nil {
		return err
	}
	return nil
}

func (q *Join) AddGame(r *GameName) error {
	_, err := q.gameStorage.Append(r.Serialize())
	if err != nil {
		return err
	}
	return nil
}

func (q *Join) addReview(appId string) error {
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

func (q *Join) ToStage3() (chan *NamedReviewCounter, chan error) {
	cache := common.NewJoinCache[string, *ReviewCounter](q.state.bufSize)
	cr := make(chan *NamedReviewCounter, q.state.bufSize)
	ce := make(chan error, 1)
	go func() {
		q.gameStorage.Reset()
		q.reviewStorage.Reset()

		gss, err := q.gameStorage.Scanner()
		if err != nil {
			ce <- err
			close(ce)
			return
		}
		rss, err := q.reviewStorage.Scanner()
		if err != nil {
			ce <- err
			close(ce)
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
				close(ce)
				return
			}
		}

		close(cr)
	}()

	return cr, ce
}
