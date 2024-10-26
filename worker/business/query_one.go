package business

import (
	"middleware/common"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
)

func Q1Map(r *schema.Game) schema.Partitionable {
	return &schema.SOCounter{
		Windows: boolToCounter(r.Windows),
		Linux:   boolToCounter(r.Linux),
		Mac:     boolToCounter(r.Mac),
	}
}

func q1StateFromBytes(data []byte) (*schema.SOCounter, error) {
	if len(data) == 0 {
		return &schema.SOCounter{}, nil
	}

	d := common.NewDeserializer(data)

	return schema.SOCounterDeserialize(&d)
}

type Q1 struct {
	state   *schema.SOCounter
	storage *common.TemporaryStorage
}

func NewQ1(base string, id string, stage string) (*Q1, error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", base, "query_one", stage, id, "results"))

	if err != nil {
		return nil, err
	}

	diskState, err := s.ReadAll()

	if err != nil {
		return nil, err
	}

	state, err := q1StateFromBytes(diskState)

	if err != nil {
		return nil, err
	}

	return &Q1{
		state:   state,
		storage: s,
	}, nil
}

func (q *Q1) Count(r *schema.SOCounter) error {
	q.state.Windows += r.Windows
	q.state.Linux += r.Linux
	q.state.Mac += r.Mac

	log.Debugf("Saving state")
	_, err := q.storage.SaveState(q.state)
	if err != nil {
		log.Debugf("Error saving state")
		return err
	}
	return nil
}

func boolToCounter(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

func (q *Q1) NextStage() (<-chan schema.Partitionable, <-chan error) {
	ch := make(chan schema.Partitionable, 1) //Change this later
	ce := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(ce)

		ch <- q.state
	}()

	return ch, ce
}

func (q *Q1) Handle(protocolData []byte) (schema.Partitionable, error) {
	log.Debugf("Handling some message in Q1")
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		log.Debugf("Error marshalling Q1")

		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.SOCounter{}) {
		return nil, q.Count(p.(*schema.SOCounter))
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Q1) Shutdown(delete bool) {
	q.storage.Close()
	if delete {
		err := q.storage.Delete()
		if err != nil {
			log.Errorf("Error while deleting the file: %s", err)
		}
	}
}
