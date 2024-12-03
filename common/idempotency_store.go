package common

import (
	"errors"
)

type IdempotencyStore struct {
	last_id_per_og map[string]*IdempotencyID
}

func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{
		last_id_per_og: make(map[string]*IdempotencyID),
	}
}

func (s *IdempotencyStore) Save(id *IdempotencyID) {
	og := id.Origin
	s.last_id_per_og[og] = id
}

func (s *IdempotencyStore) AlreadyProcessed(id *IdempotencyID) bool {
	og := id.Origin
	last, ok := s.last_id_per_og[og]
	if !ok {
		return false
	}
	return last.Sequence >= id.Sequence
}

func (s *IdempotencyStore) LastForOrigin(og string) (*IdempotencyID, error) {
	last, ok := s.last_id_per_og[og]
	if !ok {
		return nil, errors.New("the given origin doesn't have a IdempotencyID")
	}
	return last, nil
}

func (s *IdempotencyStore) saveLast(id *IdempotencyID) {
	og := id.Origin
	this, ok := s.last_id_per_og[og]
	if !ok || this.Sequence < id.Sequence {
		s.Save(id)
	}
}

func (s *IdempotencyStore) Merge(other *IdempotencyStore) {
	for id := range other.last_id_per_og {
		s.saveLast(other.last_id_per_og[id])
	}
}
