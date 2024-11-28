package business

import (
	"fmt"
	"middleware/common"
	"middleware/worker/controller"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
	"sort"
)

func Q3FilterGames(r *schema.Game) bool {
	return common.ContainsCaseInsensitive(r.Genres, common.Config.GetString("query.three.category"))
}

func Q3FilterReviews(r *schema.Review) bool {
	if common.Config.GetBool("query.three.positive") {
		return r.ReviewScore > 0
	}
	return r.ReviewScore < 0
}

func Q3MapGames(r *schema.Game) schema.Partitionable {
	return &schema.GameName{
		AppID: r.AppID,
		Name:  r.Name,
	}
}

func Q3MapReviews(r *schema.Review) schema.Partitionable {
	return &schema.ValidReview{
		AppID: r.AppID,
	}
}

type Q3State struct {
	Top []*schema.NamedReviewCounter
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
	state     *Q3State
	storage   *common.IdempotencyHandlerSingleFile[*common.ArraySerialize[*schema.NamedReviewCounter]]
	basefiles string
}

func NewQ3(base string, id string, partition int, top int) (*Q3, error) {
	basefiles := filepath.Join(".", base, fmt.Sprintf("query_three_%d", partition), id)

	s, err := common.NewIdempotencyHandlerSingleFile[*common.ArraySerialize[*schema.NamedReviewCounter]](
		filepath.Join(basefiles, "results"),
	)
	if err != nil {
		return nil, err
	}

	state, err := s.LoadOverwriteState(common.ReadArray(schema.NamedReviewCounterDeserialize))

	if err != nil {
		return nil, err
	}

	return &Q3{
		state: &Q3State{
			Top: state.Arr,
			N:   top,
		},
		storage: s,
	}, nil
}

func (q *Q3) Insert(rc *schema.NamedReviewCounter, idempotencyID *common.IdempotencyID) error {
	q.state.Top = append(q.state.Top, rc)

	sort.Slice(q.state.Top, func(i, j int) bool {
		return q.state.Top[i].Count > q.state.Top[j].Count
	})

	if len(q.state.Top) > q.state.N {
		q.state.Top = q.state.Top[:q.state.N]
	}

	err := q.storage.SaveState(idempotencyID, &common.ArraySerialize[*schema.NamedReviewCounter]{
		Arr: q.state.Top,
	})
	if err != nil {
		return err
	}

	return nil
}

func (q *Q3) NextStage() (<-chan *controller.NextStageMessage, <-chan error) {
	ch := make(chan *controller.NextStageMessage, q.state.N)
	ce := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(ce)

		fs, err := NewFileSequence(filepath.Join(q.basefiles, "sent_lines"))
		if err != nil {
			ce <- err
			return
		}
		var line uint32 = 1
		for _, pt := range q.state.Top {
			if line < fs.LastSent() {
				continue
			}
			ch <- &controller.NextStageMessage{
				Message:      pt,
				Sequence:     line,
				SentCallback: fs.Sent,
			}
			line++
		}

		ch <- &controller.NextStageMessage{
			Message:      nil,
			Sequence:     line + 1,
			SentCallback: nil,
		}
	}()

	return ch, ce
}

func (q *Q3) Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	if q.storage.AlreadyProcessed(idempotencyID) {
		log.Debugf("Action: Saving Game Top %d | Result: Already processed | IdempotencyID: %s", q.state.N, idempotencyID)
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

func (q *Q3) Shutdown(delete bool) {
	q.storage.Close()
	if delete {
		err := q.storage.Delete()
		if err != nil {
			log.Errorf("Action: Deleting JOIN Game File | Result: Error | Error: %s", err)
		}
	}
}
