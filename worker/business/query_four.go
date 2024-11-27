package business

import (
	"fmt"
	"middleware/common"
	"middleware/worker/controller"
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
	state     *Q4State
	storage   *common.IdempotencyHandlerSingleFile[*schema.NamedReviewCounter]
	basefiles string
}

func NewQ4(base string, id string, partition int, over int, bufSize int) (*Q4, error) {
	basefiles := filepath.Join(".", base, fmt.Sprintf("query_four_%d", partition), id)

	s, err := common.NewIdempotencyHandlerSingleFile[*schema.NamedReviewCounter](
		filepath.Join(basefiles, "results"),
	)
	if err != nil {
		return nil, err
	}
	_, err = s.LoadOverwriteState(schema.NamedReviewCounterDeserialize)

	if err != nil {
		return nil, err
	}

	return &Q4{
		state: &Q4State{
			Over:    uint32(over),
			bufSize: bufSize,
		},
		storage:   s,
		basefiles: basefiles,
	}, nil
}

func (q *Q4) Insert(rc *schema.NamedReviewCounter, idempotencyID *common.IdempotencyID) error {
	if rc.Count > uint32(q.state.Over) {
		// TODO: We are not really storing these IDs, but do we really care?
		// They are not modifying state, so they can come as much as they want.
		err := q.storage.SaveState(idempotencyID, rc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Q4) NextStage() (<-chan *controller.NextStageMessage, <-chan error) {
	cr := make(chan *controller.NextStageMessage, q.state.bufSize)
	ce := make(chan error, 1)

	go func() {
		defer close(cr)
		defer close(ce)

		s, err := q.storage.ReadState(schema.NamedReviewCounterDeserialize)
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
		for rc := range s {
			if line < fs.LastSent() {
				continue
			}
			cr <- &controller.NextStageMessage{
				Message:      rc,
				Sequence:     line,
				SentCallback: fs.Sent,
			}
			line++
		}
	}()

	return cr, ce
}

func (q *Q4) Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	if q.storage.AlreadyProcessed(idempotencyID) {
		log.Debugf("Action: Saving Game Over %d | Result: Already processed | IdempotencyID: %s", q.state.Over, idempotencyID)
		return nil, nil
	}

	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.NamedReviewCounter{}) {
		return nil, q.Insert(p.(*schema.NamedReviewCounter), idempotencyID)
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Q4) Shutdown(delete bool) {
	q.storage.Close()
}
