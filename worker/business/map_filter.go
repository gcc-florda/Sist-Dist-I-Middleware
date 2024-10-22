package business

import (
	"middleware/worker/controller"
	"reflect"
)

type FilterGame func(*Game) bool
type MapGame func(*Game) controller.Partitionable

type FilterReview func(*Review) bool
type MapReview func(*Review) controller.Partitionable

type MapFilterGames struct {
	Filter FilterGame
	Mapper MapGame
}

func (mf *MapFilterGames) Do(g *Game) (controller.Partitionable, error) {
	if mf.Filter != nil {
		return mf.Mapper(g), nil
	}
	ok := mf.Filter(g)
	if !ok {
		return nil, nil
	}
	return mf.Mapper(g), nil
}

func (mf *MapFilterGames) Handle(protocolData []byte) (controller.Partitionable, error) {
	p, err := UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&Game{}) {
		return mf.Do(p.(*Game))
	}
	return nil, &UnknownTypeError{}
}

func (mf *MapFilterGames) NextStage() (<-chan controller.Partitionable, <-chan error) {
	cr := make(chan controller.Partitionable, 1)
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

func (mf *MapFilterReviews) Do(r *Review) (controller.Partitionable, error) {
	ok := mf.Filter(r)
	if !ok {
		return nil, nil
	}
	return mf.Mapper(r), nil
}

func (mf *MapFilterReviews) Handle(protocolData []byte) (controller.Partitionable, error) {
	p, err := UnmarshalMessage(protocolData)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(p) == reflect.TypeOf(&Review{}) {
		return mf.Do(p.(*Review))
	}
	return nil, &UnknownTypeError{}
}

func (mf *MapFilterReviews) NextStage() (<-chan controller.Partitionable, <-chan error) {
	cr := make(chan controller.Partitionable, 1)
	ce := make(chan error, 1)
	go func() {
		defer close(cr)
		defer close(ce)
	}()

	return cr, ce
}

func (mf *MapFilterReviews) Shutdown(delete bool) {

}
