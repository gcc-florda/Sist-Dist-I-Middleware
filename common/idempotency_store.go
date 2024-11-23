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
	og := id.origin
	s.last_id_per_og[og] = id
}

func (s *IdempotencyStore) AlreadyProcessed(id *IdempotencyID) bool {
	og := id.origin
	last, ok := s.last_id_per_og[og]
	if !ok {
		return false
	}
	return last.sequence == id.sequence
}

func (s *IdempotencyStore) LastForOrigin(og string) (*IdempotencyID, error) {
	last, ok := s.last_id_per_og[og]
	if !ok {
		return nil, errors.New("the given origin doesn't have a IdempotencyID")
	}
	return last, nil
}

func (s *IdempotencyStore) saveLast(id *IdempotencyID) {
	og := id.origin
	this, ok := s.last_id_per_og[og]
	if !ok || this.sequence < id.sequence {
		s.Save(id)
	}
}

func (s *IdempotencyStore) Merge(other *IdempotencyStore) {
	for id := range other.last_id_per_og {
		s.saveLast(other.last_id_per_og[id])
	}
}
