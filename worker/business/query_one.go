package business

import (
	"fmt"
	"middleware/common"
	"middleware/worker/controller"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
)

func boolToCounter(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

func Q1Map(r *schema.Game) schema.Partitionable {
	return &schema.SOCounter{
		Windows: boolToCounter(r.Windows),
		Linux:   boolToCounter(r.Linux),
		Mac:     boolToCounter(r.Mac),
	}
}

type Q1 struct {
	state   *schema.SOCounter
	storage *common.IdempotencyHandlerSingleFile[*schema.SOCounter]
}

func NewQ1(base string, id string, partition int, stage string) (*Q1, error) {
	basefiles := filepath.Join(".", base, fmt.Sprintf("query_one_%d", partition), stage, id)

	s, err := common.NewIdempotencyHandlerSingleFile[*schema.SOCounter](
		filepath.Join(basefiles, "results"),
	)

	if err != nil {
		return nil, err
	}

	state, err := s.LoadSequentialState(schema.SOCounterDeserialize, schema.SOCounterAggregate, &schema.SOCounter{})

	if err != nil {
		return nil, err
	}

	return &Q1{
		state:   state,
		storage: s,
	}, nil
}

func (q *Q1) Count(r *schema.SOCounter, idempotencyID *common.IdempotencyID) error {
	q.state.Windows += r.Windows
	q.state.Linux += r.Linux
	q.state.Mac += r.Mac

	err := q.storage.SaveState(idempotencyID, q.state)
	if err != nil {
		return err
	}
	return nil
}

func (q *Q1) NextStage() (<-chan *controller.NextStageMessage, <-chan error) {
	ch := make(chan *controller.NextStageMessage, 1) //Change this later
	ce := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(ce)

		ch <- &controller.NextStageMessage{
			Message:      q.state,
			Sequence:     1,
			SentCallback: nil,
		}

		ch <- &controller.NextStageMessage{
			Message:      nil,
			Sequence:     2,
			SentCallback: nil,
		}
	}()

	return ch, ce
}

func (q *Q1) Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		log.Debugf("Error marshalling Q1")

		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.SOCounter{}) {
		return nil, q.Count(p.(*schema.SOCounter), idempotencyID)
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Q1) Shutdown(delete bool) {
	q.storage.Close()
	if delete {
		err := q.storage.Delete()
		if err != nil {
			log.Errorf("Action: Deleting JOIN Game File | Result: Error | Error: %s", err)
		}
	}
}
