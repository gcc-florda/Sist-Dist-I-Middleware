package business

import (
	"middleware/worker/schema"
	"reflect"
)

type FilterGame func(*schema.Game) bool
type MapGame func(*schema.Game) schema.Partitionable

type FilterReview func(*schema.Review) bool
type MapReview func(*schema.Review) schema.Partitionable

type MapFilterGames struct {
	Filter FilterGame
	Mapper MapGame
}

func (mf *MapFilterGames) Do(g *schema.Game) (schema.Partitionable, error) {
	if mf.Filter == nil {
		return mf.Mapper(g), nil
	}
	ok := mf.Filter(g)
	if !ok {
		return nil, nil
	}
	return mf.Mapper(g), nil
}

func (mf *MapFilterGames) Handle(protocolData []byte) (schema.Partitionable, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.Game{}) {
		return mf.Do(p.(*schema.Game))
	}
	return nil, &schema.UnknownTypeError{}
}

func (mf *MapFilterGames) NextStage() (<-chan schema.Partitionable, <-chan error) {
	cr := make(chan schema.Partitionable, 1)
	ce := make(chan error, 1)
	go func() {
		defer close(cr)
		defer close(ce)
	}()

	return cr, ce
}

func (mf *MapFilterGames) Shutdown(delete bool) {

}

type MapFilterReviews struct {
	Filter FilterReview
	Mapper MapReview
}

func (mf *MapFilterReviews) Do(r *schema.Review) (schema.Partitionable, error) {
	ok := mf.Filter(r)
	if !ok {
		return nil, nil
	}
	return mf.Mapper(r), nil
}

func (mf *MapFilterReviews) Handle(protocolData []byte) (schema.Partitionable, error) {
	p, err := schema.UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&schema.Review{}) {
		return mf.Do(p.(*schema.Review))
	}
	return nil, &schema.UnknownTypeError{}
}

func (mf *MapFilterReviews) NextStage() (<-chan schema.Partitionable, <-chan error) {
	cr := make(chan schema.Partitionable, 1)
	ce := make(chan error, 1)
	go func() {
		defer close(cr)
		defer close(ce)
	}()

	return cr, ce
}

func (mf *MapFilterReviews) Shutdown(delete bool) {

}
