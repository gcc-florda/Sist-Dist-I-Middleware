package business

import (
	"fmt"
	"middleware/common"
	"middleware/worker/controller"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
	"sort"
	"time"
)

func extractDecade(s string) (int, error) {
	parsedDate, err := time.Parse("Jan 2, 2006", s)
	if err != nil {
		return 0, nil
	}

	// Extract the year
	year := parsedDate.Year()

	// Calculate the decade
	return year / 10 * 10, nil
}

func Q2Filter(r *schema.Game) bool {
	decade, err := extractDecade(r.ReleaseDate)
	if err != nil {
		log.Error("Can't extract decade from: %s", r.ReleaseDate)
		return false
	}
	return common.ContainsCaseInsensitive(r.Genres, common.Config.GetString("query.two.category")) && decade == common.Config.GetInt("query.two.decade")
}

func Q2Map(r *schema.Game) schema.Partitionable {
	return &schema.PlayedTime{
		AveragePlaytimeForever: r.AveragePlaytimeForever,
		Name:                   r.Name,
	}
}

type Q2State struct {
	Top []*schema.PlayedTime
	N   int
}

type Q2 struct {
	state     *Q2State
	storage   *common.IdempotencyHandlerSingleFile[*common.ArraySerialize[*schema.PlayedTime]]
	basefiles string
}

func NewQ2(base string, stage string, id string, partition int, top int) (*Q2, error) {
	basefiles := filepath.Join(".", base, fmt.Sprintf("query_two_%d", partition), stage, id)

	s, err := common.NewIdempotencyHandlerSingleFile[*common.ArraySerialize[*schema.PlayedTime]](
		filepath.Join(basefiles, "results"),
	)
	if err != nil {
		return nil, err
	}

	state, err := s.LoadOverwriteState(common.ReadArray(schema.PlayedTimeDeserialize))

	if err != nil {
		return nil, err
	}

	return &Q2{
		state: &Q2State{
			Top: state.Arr,
			N:   top,
		},
		storage:   s,
		basefiles: basefiles,
	}, nil
}

func (q *Q2) Insert(games *schema.PlayedTime, idempotencyID *common.IdempotencyID) error {
	q.state.Top = append(q.state.Top, games)

	sort.Slice(q.state.Top, func(i, j int) bool {
		return q.state.Top[i].AveragePlaytimeForever > q.state.Top[j].AveragePlaytimeForever
	})

	if len(q.state.Top) > q.state.N {
		q.state.Top = q.state.Top[:q.state.N]
	}
	err := q.storage.SaveState(idempotencyID, &common.ArraySerialize[*schema.PlayedTime]{
		Arr: q.state.Top,
	})

	if err != nil {
		return err
	}

	return nil
}

func (q *Q2) NextStage() (<-chan *controller.NextStageMessage, <-chan error) {
	ch := make(chan *controller.NextStageMessage, q.state.N) //Change this later
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
	}()

	return ch, ce
}

func (q *Q2) Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	if q.storage.AlreadyProcessed(idempotencyID) {
		log.Debugf("Action: Saving Game Top %d | Result: Already processed | IdempotencyID: %s", q.state.N, idempotencyID)
		return nil, nil
	}
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.PlayedTime{}) {
		return nil, q.Insert(p.(*schema.PlayedTime), idempotencyID)
	}
	return nil, &schema.UnknownTypeError{}
}

func (q *Q2) Shutdown(delete bool) {
	q.storage.Close()
	if delete {
		err := q.storage.Delete()
		if err != nil {
			log.Errorf("Action: Deleting JOIN Game File | Result: Error | Error: %s", err)
		}
	}
}
