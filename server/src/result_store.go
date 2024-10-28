package src

import (
	"encoding/csv"
	"fmt"
	"middleware/common"
	"middleware/worker/schema"
	"path/filepath"
)

type QueryResultStore[T schema.ToCSV] struct {
	Finished  bool
	store     *common.TemporaryStorage
	csvWriter *csv.Writer
}

func NewQueryResultStore[T schema.ToCSV](id string, name string) (*QueryResultStore[T], error) {
	s, err := common.NewTemporaryStorage(filepath.Join(".", "data", "results", id, fmt.Sprintf("%s.csv", name)))
	if err != nil {
		return nil, err
	}
	f, err := s.File()
	if err != nil {
		return nil, err
	}
	return &QueryResultStore[T]{store: s, csvWriter: csv.NewWriter(f)}, nil
}

func (q *QueryResultStore[T]) AddResult(msg T) error {
	defer q.csvWriter.Flush()

	record := msg.ToCSV()
	log.Debugf("Received %v", record)
	if err := q.csvWriter.Write(record); err != nil {
		return err
	}

	return nil
}

type ResultStore struct {
	jobID      common.JobID
	QueryOne   *QueryResultStore[*schema.SOCounter]
	QueryTwo   *QueryResultStore[*schema.PlayedTime]
	QueryThree *QueryResultStore[*schema.NamedReviewCounter]
	QueryFour  *QueryResultStore[*schema.NamedReviewCounter]
	QueryFive  *QueryResultStore[*schema.NamedReviewCounter]
}

func NewResultStore(f common.JobID) (*ResultStore, error) {
	qs1, err := NewQueryResultStore[*schema.SOCounter](f.String(), "query_one")
	if err != nil {
		return nil, err
	}
	qs2, err := NewQueryResultStore[*schema.PlayedTime](f.String(), "query_two")
	if err != nil {
		return nil, err
	}
	qs3, err := NewQueryResultStore[*schema.NamedReviewCounter](f.String(), "query_three")
	if err != nil {
		return nil, err
	}
	qs4, err := NewQueryResultStore[*schema.NamedReviewCounter](f.String(), "query_four")
	if err != nil {
		return nil, err
	}
	qs5, err := NewQueryResultStore[*schema.NamedReviewCounter](f.String(), "query_five")
	if err != nil {
		return nil, err
	}

	return &ResultStore{
		jobID:      f,
		QueryOne:   qs1,
		QueryTwo:   qs2,
		QueryThree: qs3,
		QueryFour:  qs4,
		QueryFive:  qs5,
	}, nil
}
