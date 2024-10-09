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
	filter FilterGame
	mapper MapGame
}

func (mf *MapFilterGames) Do(g *Game) (controller.Partitionable, error) {
	ok := mf.filter(g)
	if !ok {
		return nil, nil
	}
	return mf.mapper(g), nil
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

func (mf *MapFilterGames) Shutdown() {

}

type MapFilterReviews struct {
	filter FilterReview
	mapper MapReview
}

func (mf *MapFilterReviews) Do(r *Review) (controller.Partitionable, error) {
	ok := mf.filter(r)
	if !ok {
		return nil, nil
	}
	return mf.mapper(r), nil
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

func (mf *MapFilterReviews) Shutdown() {

}
