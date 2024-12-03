package business

import (
	"fmt"
	"middleware/common"
	"middleware/worker/controller"
	"middleware/worker/schema"
	"path/filepath"
	"reflect"
)

type FilterGame func(*schema.Game) bool
type MapGame func(*schema.Game) schema.Partitionable

type FilterReview func(*schema.Review) bool
type MapReview func(*schema.Review) schema.Partitionable

type MapFilterGames struct {
	Filter    FilterGame
	Mapper    MapGame
	basefiles string
	state     *common.IdempotencyHandlerSingleFile[*NullState]
}

func NewMapFilterGames(base string, id string, query string, partition int, mapper MapGame, filter FilterGame) (*MapFilterGames, error) {
	basefiles := filepath.Join(".", base, fmt.Sprintf("map_filter_%s_%d", query, partition), id)

	s, err := common.NewIdempotencyHandlerSingleFile[*NullState](filepath.Join(basefiles, "done"))

	if err != nil {
		return nil, err
	}

	_, err = s.LoadOverwriteState(NullStateDeserialize)

	if err != nil {
		return nil, err
	}

	return &MapFilterGames{
		Filter:    filter,
		Mapper:    mapper,
		basefiles: basefiles,
		state:     s,
	}, nil
}

func (mf *MapFilterGames) Do(g *schema.Game, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	if mf.Filter == nil {
		return &controller.NextStageMessage{
			Message:  mf.Mapper(g),
			Sequence: idempotencyID.Sequence,
		}, nil
	}
	ok := mf.Filter(g)
	if !ok {
		return nil, nil
	}
	return &controller.NextStageMessage{
		Message:  mf.Mapper(g),
		Sequence: idempotencyID.Sequence,
	}, nil
}

func (mf *MapFilterGames) Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.Game{}) {
		return mf.Do(p.(*schema.Game), idempotencyID)
	}
	return nil, &schema.UnknownTypeError{}
}

func (mf *MapFilterGames) NextStage() (<-chan *controller.NextStageMessage, <-chan error) {
	cr := make(chan *controller.NextStageMessage, 1)
	ce := make(chan error, 1)
	go func() {
		defer close(cr)
		defer close(ce)
	}()

	return cr, ce
}

func (mf *MapFilterGames) Shutdown(delete bool) {
	mf.state.Close()
	if delete {
		mf.state.Delete()
	}
}

type MapFilterReviews struct {
	Filter    FilterReview
	Mapper    MapReview
	basefiles string
	state     *common.IdempotencyHandlerSingleFile[*NullState]
}

func NewMapFilterReviews(base string, id string, query string, partition int, mapper MapReview, filter FilterReview) (*MapFilterReviews, error) {
	basefiles := filepath.Join(".", base, fmt.Sprintf("map_filter_%s_%d", query, partition), id)

	s, err := common.NewIdempotencyHandlerSingleFile[*NullState](filepath.Join(basefiles, "done"))

	if err != nil {
		return nil, err
	}

	_, err = s.LoadOverwriteState(NullStateDeserialize)

	if err != nil {
		return nil, err
	}

	return &MapFilterReviews{
		Filter:    filter,
		Mapper:    mapper,
		basefiles: basefiles,
		state:     s,
	}, nil
}

func (mf *MapFilterReviews) Do(r *schema.Review, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	if mf.Filter == nil {
		return &controller.NextStageMessage{
			Message:  mf.Mapper(r),
			Sequence: idempotencyID.Sequence,
		}, nil
	}
	ok := mf.Filter(r)
	if !ok {
		return nil, nil
	}
	return &controller.NextStageMessage{
		Message:  mf.Mapper(r),
		Sequence: idempotencyID.Sequence,
	}, nil
}

func (mf *MapFilterReviews) Handle(protocolData []byte, idempotencyID *common.IdempotencyID) (*controller.NextStageMessage, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.Review{}) {
		return mf.Do(p.(*schema.Review), idempotencyID)
	}
	return nil, &schema.UnknownTypeError{}
}

func (mf *MapFilterReviews) NextStage() (<-chan *controller.NextStageMessage, <-chan error) {
	cr := make(chan *controller.NextStageMessage, 1)
	ce := make(chan error, 1)
	go func() {
		defer close(cr)
		defer close(ce)
	}()

	return cr, ce
}

func (mf *MapFilterReviews) Shutdown(delete bool) {
	mf.state.Close()
	if delete {
		mf.state.Delete()
	}
}
