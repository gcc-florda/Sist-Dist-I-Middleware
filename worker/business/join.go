package business

import (
	"fmt"
	"middleware/common"
	"middleware/worker/controller"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
)

type Join struct {
	reviewStorage *common.IdempotencyHandlerMultipleFiles[*CountState]
	gameStorage   *common.IdempotencyHandlerSingleFile[*schema.GameName]
	basefiles     string
}

func NewJoin(base string, query string, id string, partition int, bufSize int) (*Join, error) {
	basefiles := filepath.Join(".", base, fmt.Sprintf("%s_%d", query, partition), "join", id)

	r, err := common.NewIdempotencyHandlerMultipleFiles[*CountState](
		filepath.Join(basefiles, "reviews"),
	)

	if err != nil {
		return nil, err
	}

	if err = r.LoadState(CountStateDeserialize); err != nil {
		return nil, err
	}

	g, err := common.NewIdempotencyHandlerSingleFile[*schema.GameName](
		filepath.Join(basefiles, "games"),
	)

	if err != nil {
		return nil, err
	}

	// We don't care about the games for now, only for the IdempotencyID
	if _, err = g.LoadOverwriteState(schema.GameNameDeserialize); err != nil {
		return nil, err
	}

	return &Join{
		reviewStorage: r,
		gameStorage:   g,
		basefiles:     basefiles,
	}, nil
}

func (q *Join) AddReview(r *schema.ValidReview, idempotencyID *common.IdempotencyID) error {
	if q.reviewStorage.AlreadyProcessed(idempotencyID) {
		log.Debugf("Action: Saving Review to Join | Result: Already processed | IdempotencyID: %s", idempotencyID)
		return nil
	}

	err := q.reviewStorage.SaveState(idempotencyID, &CountState{count: 1}, r.AppID)
	if err != nil {
		log.Debugf("Action: Saving Review to Join | Result: Error | Error: %s", err)
		return err
	}

	return nil
}

func (q *Join) AddGame(r *schema.GameName, idempotencyID *common.IdempotencyID) error {
	if q.gameStorage.AlreadyProcessed(idempotencyID) {
		log.Debugf("Action: Saving Game to Join | Result: Already processed | IdempotencyID: %s", idempotencyID)
		return nil
	}
	err := q.gameStorage.SaveState(idempotencyID, r)
	if err != nil {
		log.Debugf("Action: Saving Game to Join | Result: Error | Error: %s", err)
		return err
	}
	return nil
}

func (q *Join) NextStage() (<-chan *controller.NextStageMessage, <-chan error) {
	cr := make(chan *controller.NextStageMessage)
	ce := make(chan error, 1)
	go func() {
		defer close(cr)
		defer close(ce)

		games, err := q.gameStorage.ReadState(schema.GameNameDeserialize)
		if err != nil {
			ce <- err
			return
		}
		fs, err := NewFileSequence(filepath.Join(q.basefiles, "sent_lines"))
		if err != nil {
			ce <- err
			return
		}

		var line uint32 = 1
		for game := range games {
			if line < fs.LastConfirmedSent() {
				continue
			}
			reviews, err := q.reviewStorage.ReadSerialState(game.AppID, CountStateDeserialize, CountStateAggregate, &CountState{count: 0})
			if err != nil {
				ce <- err
				return
			}
			cr <- &controller.NextStageMessage{
				Message: &schema.NamedReviewCounter{
					Name:  game.Name,
					Count: reviews.count,
				},
				Sequence:     line,
				SentCallback: fs.Sent,
			}
			line++
		}

		cr <- &controller.NextStageMessage{
			Message:      nil,
			Sequence:     line + 1,
			SentCallback: nil,
		}
	}()

	return cr, ce
}

func (q *Join) Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.ValidReview{}) {
		return nil, q.AddReview(p.(*schema.ValidReview), idempotencyID)
	}

	if reflect.TypeOf(p) == reflect.TypeOf(&schema.GameName{}) {
		return nil, q.AddGame(p.(*schema.GameName), idempotencyID)
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
