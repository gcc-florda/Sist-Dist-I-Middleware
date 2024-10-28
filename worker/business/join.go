package business

import (
	"fmt"
	"io"
	"middleware/common"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
)

type JoinState struct {
	bufSize int
}

type Join struct {
	state         *JoinState
	reviewStorage *common.TemporaryStorage
	gameStorage   *common.TemporaryStorage
}

func NewJoin(base string, query string, id string, partition int, bufSize int) (*Join, error) {
	r, err := common.NewTemporaryStorage(filepath.Join(".", base, fmt.Sprintf("%s_%d", query, partition), "join", id, "review.results"))
	if err != nil {
		return nil, err
	}

	g, err := common.NewTemporaryStorage(filepath.Join(".", base, fmt.Sprintf("%s_%d", query, partition), "join", id, "game.results"))
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

func (q *Join) AddReview(r *schema.ValidReview) error {
	err := q.addReview(r.AppID)
	if err != nil {
		log.Debugf("Action: Saving Review to Join | Result: Error | Error: %s", err)
		return err
	}
	return nil
}

func (q *Join) AddGame(r *schema.GameName) error {
	_, err := q.gameStorage.Append(r.Serialize())
	if err != nil {
		log.Debugf("Action: Saving Game to Join | Result: Error | Error: %s", err)
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
	scanner, err := q.reviewStorage.ScannerDeserialize(func(d *common.Deserializer) error {
		_, err := schema.ReviewCounterDeserialize(d)
		return err
	})
	if err != nil {
		return err
	}
	for scanner.Scan() {
		b := scanner.Bytes()
		lb := len(b)
		d := common.NewDeserializer(b)
		l, err := schema.ReviewCounterDeserialize(&d)
		if err != nil {
			log.Debugf("Action: Deserialize from file | Result: Error | Error: %s | Data: %v", err, b)
			continue
		}

		if l.AppID != appId {
			offset += int64(lb)
			continue
		}

		l.Count += 1
		nb := l.Serialize()

		_, err = file.Seek(offset, 0)
		if err != nil {
			log.Debugf("Action: Moving Offset | Result: Error | Error: %s", err)
			return err
		}

		_, err = file.Write(nb)
		if err != nil {
			log.Debugf("Action: Writing | Result: Error | Error: %s", err)
			return err
		}

		return nil
	}

	// Check for any error during scanning
	if err := scanner.Err(); err != nil {
		log.Debugf("Action: Reading from file | Result: Error | Error: %s", err)
		return err
	}

	// We got here because we didn't find the review already written
	c := &schema.ReviewCounter{
		AppID: appId,
		Count: 1,
	}
	b := c.Serialize()
	file.Seek(0, io.SeekEnd)
	_, err = file.Write(b)
	if err != nil {
		return err
	}

	return nil
}

func (q *Join) NextStage() (<-chan schema.Partitionable, <-chan error) {
	cache := common.NewJoinCache[string, *schema.ReviewCounter](1)
	cr := make(chan schema.Partitionable, q.state.bufSize)
	ce := make(chan error, 1)
	go func() {
		defer close(cr)
		defer close(ce)
		tryJoin := func(g *schema.GameName) error {
			v, ok := cache.Get(g.AppID)
			if ok {
				cache.Remove(g.AppID)
				nrc := &schema.NamedReviewCounter{
					Name:  g.Name,
					Count: v.Count,
				}
				cr <- nrc
				return nil
			}

			q.reviewStorage.Reset()
			rss, err := q.reviewStorage.ScannerDeserialize(func(d *common.Deserializer) error {
				_, err := schema.ReviewCounterDeserialize(d)
				return err
			})
			if err != nil {
				return err
			}

			for rss.Scan() {
				b := rss.Bytes()
				d := common.NewDeserializer(b)
				r, err := schema.ReviewCounterDeserialize(&d)
				if err != nil {
					return err
				}
				if r.AppID == g.AppID {
					nrc := &schema.NamedReviewCounter{
						Name:  g.Name,
						Count: r.Count,
					}
					cr <- nrc
					return nil
				} else {
					cache.TryPut(r.AppID, r)
				}
			}

			cr <- &schema.NamedReviewCounter{
				Name:  g.Name,
				Count: 0,
			}
			return nil
		}

		q.gameStorage.Reset()
		gss, err := q.gameStorage.ScannerDeserialize(func(d *common.Deserializer) error {
			_, err := schema.GameNameDeserialize(d)
			return err
		})
		if err != nil {
			ce <- err
			return
		}
		for gss.Scan() {
			b := gss.Bytes()
			d := common.NewDeserializer(b)
			gn, err := schema.GameNameDeserialize(&d)
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

func (q *Join) Handle(protocolData []byte) (schema.Partitionable, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.ValidReview{}) {
		return nil, q.AddReview(p.(*schema.ValidReview))
	}

	if reflect.TypeOf(p) == reflect.TypeOf(&schema.GameName{}) {
		return nil, q.AddGame(p.(*schema.GameName))
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Join) Shutdown(delete bool) {
	q.gameStorage.Close()
	q.reviewStorage.Close()
	if delete {
		err := q.gameStorage.Delete()
		if err != nil {
			log.Errorf("Action: Deleting JOIN Game File | Result: Error | Error: %s", err)
		}

		err = q.reviewStorage.Delete()
		if err != nil {
			log.Errorf("Action: Deleting JOIN Game File | Result: Error | Error: %s", err)
		}
	}
}
